"""
Módulo ResultReceiver.
Escucha sobre la conexión unificada para recolectar los resultados del clúster Map-Reduce.
Implementa procesamiento de latidos (heartbeats) para mantener viva la sesión durante esperas largas.
"""
import logging
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import unquote, urlparse

from common.protocol.constants import (
    ExternalType,
    FLAG_IS_LAST_PART,
    FLAG_IS_SESSION_END,
    ResultType,
)
from common.protocol.external import framing, serializer

logger = logging.getLogger(__name__)


class ResultReceiver:
    """
    Escucha y decodifica las respuestas del Gateway.
    Dise��ado para tolerar esperas prolongadas mediante la recepci��n de ACKs (Heartbeats)
    y fallar rǭpido ante desconexiones silenciosas o errores del clǧster.
    """

    STATUS_EVERY = 100

    def __init__(self, sock: socket.socket, outfiles: Dict[str,str], image_dir=None) -> None:
        """
        Inicializa el receiver recibiendo el socket ya conectado y configurado.
        """
        self._socket: socket.socket = sock
        self._image_dir = Path(image_dir) if image_dir else None
        if self._image_dir:
            self._image_dir.mkdir(parents=True, exist_ok=True)
        # Estado interno de la recepción
        self._results_received: int = 0
        self._results_by_query_type: Dict[int, int]={}
        self._filehandlers: Dict[str,Any]={}
        self._partial_results: Dict[int, list] = {}

        for filetype, filename in outfiles.items():
            self._filehandlers[filetype]=open(filename, "w", encoding="utf-8")
         
    def _process_result(self, data: Dict[str, Any]) -> None:
        """
        Procesa de manera individual cada resultado retornado por el clúster.
        """
        resultType=data.get("resultType")

        if resultType == ResultType.Q2:
            parts = self._partial_results.setdefault(data["docId"], [])
            parts.append(data.get("payload", b""))
            if not data["flags"] & FLAG_IS_LAST_PART:
                return
            data = {**data, "payload": b"".join(self._partial_results.pop(data["docId"]))}

        self._results_received += 1

        detail = self._describe(
            data.get("resultType"), data.get("payload", b""), data.get("docId")
        )

        # El summary cuenta filas lógicas, no mensajes:
        # Q1 suma 1 por artículo; Q3 suma 1 por palabra; Q4/Q5 una por fila.
        if resultType and resultType > 0:
            cantidad = detail.count("\n") + 1 if detail else 0
            self._results_by_query_type[resultType] = (
                self._results_by_query_type.get(resultType, 0) + cantidad
            )
            if detail:
                self._filehandlers.get(f"q{resultType}").write(detail + "\n")

        logger.debug(
            f"Resultado [{self._results_received}] Q{data.get('resultType')} "
            f"doc={data.get('docId')} part={data.get('partId')} -> {detail}"
        )

        if self._results_received % self.STATUS_EVERY == 0:
            logger.info(
                "Progreso: mensajes=%s, resultados=%s",
                self._results_received,
                dict(sorted(self._results_by_query_type.items())),
            )

    def _describe(self, result_type, payload, doc_id=None):
        if not payload:
            return "fin de sesi��n"
        if result_type == ResultType.Q1:
            item = serializer.deserialize_q1_payload(payload)
            date = datetime.fromtimestamp(item["dateModified"], tz=timezone.utc)
            return f'"{item['url']}",{date:%Y-%m-%d %H:%M:%S}'
        if result_type == ResultType.Q2:
            item = serializer.deserialize_q2_payload(payload)
            image_path = self._save_q2_image(doc_id, item["image"], item["url"])
            if image_path:
                return f'"{item["name"]}","{item["url"]}","{image_path}"'
            return f'"{item["name"]}","{item["url"]}",{len(item["image"])} bytes'
        if result_type == ResultType.Q5:
            item = serializer.deserialize_q5_payload(payload)
            return f'"{item["url"]}",{item["wordCount"]}'
        if result_type == ResultType.Q4:
            pairs = serializer.deserialize_q4_payload(payload)
            return "\n".join(f"{word},{count}" for word, count in pairs)
        if result_type == ResultType.Q3:
            words = serializer.deserialize_q3_payload(payload)
            return "\n".join(words)
        return f"{len(payload)} bytes (sin decodificar)"

    def _save_q2_image(self, doc_id, image, url):
        if not self._image_dir or not image:
            return ""

        original_name = Path(unquote(urlparse(url).path)).name
        if not original_name:
            original_name = "image.img"

        original_name = Path(original_name).name
        if not Path(original_name).suffix:
            original_name += ".img"

        prefix = doc_id if doc_id is not None else "unknown"
        filename = f"{prefix}_{original_name}"

        path = self._image_dir / filename
        path.write_bytes(image)
        return str(path)

    def receive_until_last(self) -> None:
        """
        Bucle de red bloqueante.
        Lee secuencialmente los frames TCP, gestiona los latidos para evitar timeouts en 
        largas esperas, y evalúa la condición de corte (isSessionEnd o Error del clúster).
        """
        logger.info("Esperando resultados del clúster distribuido...")

        try:
            while True:
                # TimeoutError si el Gateway no envía datos ni latidos a tiempo
                payload = framing.recv_message(self._socket)

                # Heartbeat
                if framing.is_ack(payload):
                    logger.debug("Latido (ACK) recibido del Gateway. El clúster sigue procesando.")
                    continue

                # Deserialización del mensaje
                try:
                    msg_type, data = serializer.deserialize_message(payload)
                except ValueError as e:
                    logger.error(f"Fallo al decodificar el mensaje del Gateway: {e}")
                    raise RuntimeError("Corrupción en el protocolo binario de recepción.") from e

                # Enrutamiento de la respuesta
                if msg_type == ExternalType.RESULT:
                    self._process_result(data)
                    if data["flags"] & FLAG_IS_SESSION_END:
                        logger.info(
                            f"Señal de fin de sesión recibida del clúster. "
                            f"Total de resultados recolectados con éxito: {self._results_received}."
                        )
                        break

                elif msg_type == ExternalType.ERROR:
                    error_code = data.get("code", "UNKNOWN")
                    error_msg = data.get("message", "Sin descripción")
                    logger.error(f"El clúster abortó el procesamiento con un ERROR [{error_code}]: {error_msg}")
                    break
                
                else:
                    logger.warning(f"Tipo de mensaje externo ignorado o desconocido: {msg_type}")

        except TimeoutError as e:
            logger.critical(
                "El Gateway dejó de responder (heartbeat)"
                "Abortando recepción."
            )
            raise ConnectionError("Timeout esperando resultados del clúster.") from e
        except ConnectionError as e:
            logger.critical(f"Ruptura inesperada del canal TCP durante la recepción: {e}.")
            raise
        except Exception as e:
            logger.critical(f"Error crítico irrecuperable en el bucle de recepción: {e}")
            raise

    def close_files(self) -> None:
        for handle in self._filehandlers.values():
            handle.close()