"""Q1: artículos en inglés hasta 2020."""

import logging
from collections import Counter

from common.middleware.group import GroupCoordinator
from common.protocol.constants import (
    CTRL_Q1,
    FLAG_IS_LAST_PART,
    LANG_EN,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("q1")
PROGRESS_EVERY = 5000


class Q1Controller(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._seen = Counter()
        self._matched = Counter()
        self.group = GroupCoordinator(
            controller_type=CTRL_Q1,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
            origin_node=self.origin_node,
            next_msg_seq=self.next_msg_seq,
        )
        # Control de duplicados logicos
        self.emitted_article_ids = {}

        logger.info(
            "START node=%s/%s",
            self.node_id,
            self.controller_count,
        )

    def _send_done(self, client_id, quantity):
        logger.info(
            "DONE client=%s node=%s emitted=%s",
            client_id,
            self.node_id,
            quantity,
        )
        msg_seq = self.next_msg_seq(client_id)
        self.out_mw.send(
            control.serialize_done(
                client_id,
                self.origin_node,
                msg_seq,
                CTRL_Q1,
                self.node_id,
                quantity,
            )
        )
        self.emitted_article_ids.pop(client_id, None)
        self._seen.pop(client_id, None)
        self._matched.pop(client_id, None)

    def handle_article(self, client_id, article):
        # El lock es porque esto lo llaman dos hilos (de datos y de control).
        with self._lock:
            if self.group.is_closed(client_id):
                return

            self._seen[client_id] += 1

            if self._matches(article):

                emitted = self.emitted_article_ids.setdefault(client_id, set())
                if article.articleId in emitted:
                    self.group.on_processed(client_id)
                    return
                emitted.add(article.articleId)
                
                self._matched[client_id] += 1
                # Q1: cada articulo que matchea es un resultado completo.
                logger.debug(
                    "MATCH client=%s node=%s doc=%s",
                    client_id,
                    self.node_id,
                    article.docId,
                )
                self.group.add_emitted(client_id)
                payload = external.serialize_q1_payload(
                    article.url, article.dateModified
                )
                msg_seq = self.next_msg_seq(client_id)
                self.out_mw.send(
                    internal.serialize_result_message(
                        client_id,
                        self.origin_node,
                        msg_seq,
                        article.articleId,
                        ResultType.Q1,
                        article.docId,
                        0,
                        FLAG_IS_LAST_PART,
                        payload,
                    )
                )

            # El group cuenta TODO lo que entra (matchee o no) y reporta
            # cada B; en modo FLUSH reporta cada mensaje.
            self.group.on_processed(client_id)

            if self._seen[client_id] % PROGRESS_EVERY == 0:
                logger.info(
                    "PROGRESS client=%s node=%s seen=%s matched=%s",
                    client_id,
                    self.node_id,
                    self._seen[client_id],
                    self._matched[client_id],
                )

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            self.group.on_control(msg_type, client_id, message, from_control)

    @staticmethod
    def _matches(article):
        # dateModified es epoch; el año lo saca Article.year.
        # Q1 pide inglés y <= 2020.
        if article.inLanguage != LANG_EN:
            return False
        # 0 = fecha faltante (el cliente manda 0 cuando no hay date_modified).
        if article.dateModified is None or article.dateModified <= 0 or article.url is None:
            return False
        year = article.year
        return year is not None and year <= 2020
