"""
Módulo BatchSender.
Empaqueta, transforma y envía los lotes al Gateway (ArticleHandler).
Utiliza una conexión inyectada (Dependency Injection) y aplica un modelo Crash-Only ante fallos.
"""
import json
import logging
import socket
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional

from common.protocol.constants import (
    LANG_EN,
    LANG_FR,
    LANG_OTHER,
    SECTION_BIOGRAPHIE,
    SECTION_BIOGRAPHY,
)
from common.protocol.external import framing, serializer

logger = logging.getLogger(__name__)

LANG_CODES = {"en": LANG_EN, "fr": LANG_FR}


def _walk_sections(node):
    if isinstance(node, list):
        for item in node:
            yield from _walk_sections(item)
    elif isinstance(node, dict):
        name = node.get("name")
        if isinstance(name, str) and name.strip():
            yield name
        for key in ("has_parts", "parts"):
            if key in node:
                yield from _walk_sections(node[key])


def _iter_section_names(value):
    if value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return
        try:
            value = json.loads(text)
        except (ValueError, TypeError):
            # String que no es JSON: se toma como el nombre de una única sección.
            yield text
            return
    yield from _walk_sections(value)


def _sections_flags(value):
    flags = 0
    for name in _iter_section_names(value):
        normalized = name.strip().lower()
        if normalized == "biography":
            flags |= SECTION_BIOGRAPHY
        elif normalized == "biographie":
            flags |= SECTION_BIOGRAPHIE
    return flags


class _LightweightArticle:
    """
    Entidad ultraligera que utiliza __slots__ para reducir drásticamente el consumo 
    de memoria RAM (O(1) overhead) durante el mapeo de lotes masivos.
    Cumple con el contrato de atributos requerido por serialize_article.
    """
    __slots__ = [
        "docId", "name", "url", "dateModified", 
        "inLanguage", "sectionsFlags", "abstract", "imageUrl"
    ]

    def __init__(
        self, docId: int, name: str, url: str, dateModified: int,
        inLanguage: int, sectionsFlags: int, abstract: str, imageUrl: str
    ) -> None:
        self.docId = docId
        self.name = name
        self.url = url
        self.dateModified = dateModified
        self.inLanguage = inLanguage
        self.sectionsFlags = sectionsFlags
        self.abstract = abstract
        self.imageUrl = imageUrl


class BatchSender:
    """
    Gestiona la transmisión de datos hacia el Gateway sobre una sesión TCP.
    Falla ante cortes para forzar un reinicio limpio del cliente.
    """
    
    _REQUIRED_COLUMNS = {
        "docId", "name", "url", "dateModified", 
        "inLanguage", "sectionsFlags", "abstract", "imageUrl"
    }

    def __init__(
        self,
        sock: socket.socket,
        on_frame: Optional[Callable[[bytes], bool]] = None,
    ) -> None:
        """
        Inicializa el sender recibiendo el socket ya conectado y configurado.

        `on_frame` procesa los frames que no son ACK y llegan mientras se espera el ACK de un
        envío (resultados que el Gateway reenvía en streaming durante la ingesta). Debe devolver
        True si el frame cierra la sesión (fin de sesión o error del clúster).
        """
        self._socket: socket.socket = sock
        self._on_frame = on_frame
        # Contadores de estado para el cierre lógico
        self._total_articles: int = 0
        self._total_batches: int = 0

    def _map_to_articles(self, batch: Any) -> List[_LightweightArticle]:
        """
        Extrae columnas del bloque de PyArrow adaptando el esquema real del dataset 
        al contrato del Gateway.
        """
        try:
            batch_dict = batch.to_pydict()
        except AttributeError:
            raise TypeError("El batch provisto no soporta la exportación mediante to_pydict().")

        if not batch_dict:
            return []

        # La cantidad de filas del lote
        batch_size = len(next(iter(batch_dict.values())))

        # Mapeo directo de nombres (Dataset -> Gateway)
        doc_ids = batch_dict.get("identifier", [0] * batch_size)
        names = batch_dict.get("name", [""] * batch_size)
        urls = batch_dict.get("url", [""] * batch_size)
        abstracts = batch_dict.get("abstract", [""] * batch_size)
        
        # Extracción de columnas que requieren transformación
        raw_dates = batch_dict.get("date_modified", [0] * batch_size)
        raw_images = batch_dict.get("image", [None] * batch_size)
        raw_langs = batch_dict.get("in_language", [None] * batch_size)
        raw_sections = batch_dict.get("sections", [None] * batch_size)

        articles: List[_LightweightArticle] = []
        
        for i in range(batch_size):
            # Transformación
            
            # Extraer URL de la imagen anidada
            img_struct = raw_images[i]
            img_url = img_struct.get("content_url", "") if isinstance(img_struct, dict) else ""

            # Extraer identificador de idioma anidado ("en" / "fr")
            lang_struct = raw_langs[i]
            if isinstance(lang_struct, dict):
                lang_key = str(lang_struct.get("identifier", "")).strip().lower()
            else:
                lang_key = str(lang_struct).strip().lower() if lang_struct is not None else ""
            lang_int = LANG_CODES.get(lang_key, LANG_OTHER)

            # Fechas: el parquet trae datetime naive (UTC), no un epoch
            date_val = raw_dates[i]
            if isinstance(date_val, datetime):
                if date_val.tzinfo is None:
                    date_val = date_val.replace(tzinfo=timezone.utc)
                date_int = int(date_val.timestamp())
            elif isinstance(date_val, (int, float)):
                date_int = int(date_val)
            else:
                date_int = 0

            sections_flags = _sections_flags(raw_sections[i])

            # Empaquetado final
            articles.append(
                _LightweightArticle(
                    docId=int(doc_ids[i] if doc_ids[i] is not None else 0),
                    name=str(names[i] if names[i] is not None else ""),
                    url=str(urls[i] if urls[i] is not None else ""),
                    dateModified=date_int,
                    inLanguage=lang_int,
                    sectionsFlags=sections_flags,
                    abstract=str(abstracts[i] if abstracts[i] is not None else ""),
                    imageUrl=str(img_url)
                )
            )
            
        return articles

    def _transmit(self, payload: bytes) -> None:
        """
        Envía un bloque binario y sincroniza el estado esperando un ACK del Gateway.
        Propaga la excepción de red si la comunicación se rompe.
        """
        try:
            framing.send_message(self._socket, payload)

            # El Gateway puede intercalar resultados (Q1/Q2) antes del ACK: se procesan y se
            # sigue esperando. Hay exactamente un ACK por mensaje enviado.
            while True:
                response = framing.recv_message(self._socket)

                if framing.is_ack(response):
                    return

                if self._on_frame is None:
                    raise ValueError("El Gateway respondió con un mensaje inesperado. Se requería un ACK.")

                if self._on_frame(response):
                    raise ConnectionError(
                        "El clúster cerró la sesión antes de completar la ingesta."
                    )

        except (ConnectionError, TimeoutError, OSError) as e:
            logger.critical(f"Ruptura del canal TCP durante la transmisión: {e}. Abortando sesión.")
            raise ConnectionError(f"Fallo de red en BatchSender: {e}") from e

    def send(self, batch: Any, is_last: bool = False) -> None:
        """
        Punto de entrada público. Serializa y despacha los datos y/o la señal de EOF.
        Mantiene control del progreso para el reporte final de cierre de sesión.
        """
        if batch is not None:
            articles = self._map_to_articles(batch)
            self._total_articles += len(articles)
            self._total_batches += 1
            batch_payload = serializer.serialize_batch(articles)
            self._transmit(batch_payload)
            
        if is_last:
            logger.info(
                f"Ingesta finalizada. Transmitiendo señal EOF. "
                f"Total de artículos procesados: {self._total_articles} en {self._total_batches} lotes."
            )
            eof_payload = serializer.serialize_eof(self._total_articles, self._total_batches)
            self._transmit(eof_payload)