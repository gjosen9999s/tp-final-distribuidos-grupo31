"""Q2: filtra year <= 2020 y secciones biography/biographie."""

import logging

from common.config import env
from common.middleware.group import GroupCoordinator
from common.protocol.constants import (
    CTRL_Q2_FILTER,
    FLAG_IS_LAST_PART,
    SECTION_BIOGRAPHIE,
    SECTION_BIOGRAPHY,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller
from controllers.image_downloader.wikimedia_client import WikimediaClient

logger = logging.getLogger("Q2Filter")

BIOGRAPHY_MASK = SECTION_BIOGRAPHY | SECTION_BIOGRAPHIE


class Q2Filter(Controller):
    def __init__(self, *args, image_client=None, chunk_size=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.image_client = image_client or WikimediaClient()
        self.chunk_size = max(1, chunk_size or env.result_chunk_size())
        self.group = GroupCoordinator(
            controller_type=CTRL_Q2_FILTER,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
        )
        logger.info(
            f"[Q2Filter] Iniciado nodo {self.node_id}/{self.controller_count} "
            f"(chunk_size={self.chunk_size} bytes, progress_batch={self.progress_batch_size})"
        )

    def _send_done(self, client_id, quantity):
        logger.info(
            f"[Q2Filter] Cliente ID={client_id} cerrado: enviando DONE "
            f"(nodo {self.node_id}, {quantity} resultados emitidos)"
        )
        self.out_mw.send(
            control.serialize_done(client_id, CTRL_Q2_FILTER, self.node_id, quantity)
        )

    def handle_article(self, client_id, article):
        try:
            self._handle_article(client_id, article)
        except Exception:
            logger.exception(
                f"[Q2Filter] Error procesando artículo doc={getattr(article, 'docId', '?')} "
                f"del cliente ID={client_id}"
            )
            raise

    def _handle_article(self, client_id, article):
        with self._lock:
            if self.group.is_closed(client_id):
                return
            matches = self._matches(article)

        image = self.image_client.download(article.imageUrl) if matches else None
        logger.debug(
            f"[Q2Filter] Cliente ID={client_id} doc={article.docId} "
            f"year={article.year} sections={article.sectionsFlags} matches={matches}"
        )
        if matches and not image:
            logger.warning(
                f"[Q2Filter] Cliente ID={client_id} doc={article.docId}: "
                f"sin imagen (url={article.imageUrl!r}); se emite igual con 0 bytes"
            )

        with self._lock:
            if self.group.is_closed(client_id):
                return

            if matches:
                self.group.add_emitted(client_id)
                self._send_result(client_id, article, image)
                logger.info(
                    f"[Q2Filter] Cliente ID={client_id} MATCH doc={article.docId} "
                    f"name={article.name!r} imagen={len(image or b'')} bytes"
                )

            self.group.on_processed(client_id)

    def handle_control(self, msg_type, client_id, message, from_control):
        logger.debug(
            f"[Q2Filter] Control {msg_type} cliente ID={client_id} "
            f"(origen={'control' if from_control else 'datos'})"
        )
        with self._lock:
            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_result(self, client_id, article, image):
        payload = external.serialize_q2_payload(
            article.name or "",
            article.url,
            article.imageUrl or "",
            image or b"",
        )
        parts = [
            payload[i : i + self.chunk_size]
            for i in range(0, len(payload), self.chunk_size)
        ]
        for part_id, chunk in enumerate(parts):
            flags = FLAG_IS_LAST_PART if part_id == len(parts) - 1 else 0
            self.out_mw.send(
                internal.serialize_result_message(
                    client_id, ResultType.Q2, article.docId, part_id, flags, chunk
                )
            )

    @staticmethod
    def _matches(article):
        if not article.sectionsFlags or not (article.sectionsFlags & BIOGRAPHY_MASK):
            return False
        if article.dateModified is None or article.dateModified <= 0:
            return False
        if not article.url:
            return False
        year = article.year
        return year is not None and year <= 2020