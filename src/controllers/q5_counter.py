"""Q5 Counter: cuenta palabras de cada abstract y enruta según el año (<=2020 o >=2021)."""

import logging
import os
import re
from collections import Counter

from common.middleware.group import GroupCoordinator
from common.protocol.constants import CTRL_Q5_COUNTER
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logger = logging.getLogger("q5_counter")
PROGRESS_EVERY = 5000

# Tenemos un paquete de stopwords por defecto si falla NLTK
try:
    from nltk.corpus import stopwords
    STOP_WORDS = set(stopwords.words("english")) | set(stopwords.words("french"))
except (ImportError, LookupError):
    STOP_WORDS = {
        "a", "an", "the", "and", "or", "but", "if", "in", "on", "at",
        "to", "for", "of", "with", "by", "from", "as", "is", "was",
        "are", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may",
        "might", "shall", "can", "need", "dare", "ought", "used",
        "that", "this", "these", "those", "it", "its", "he", "she",
        "they", "we", "you", "i", "me", "him", "her", "us", "them",
        "my", "your", "his", "our", "their", "not", "no", "nor", "so",
        "le", "la", "les", "un", "une", "des", "du", "de", "et", "en",
    }

WORD_REGEX = r"\b\w+\b"
NUMBER_REGEX = r"[+-]?((\d+\.?\d*)|(\.\d+))"


class Q5Counter(Controller):
    def __init__(self, *args, out_cand_mw=None, sum_queues=None, **kwargs):
        super().__init__(*args, **kwargs)
        mom_host = os.getenv("MOM_HOST", "localhost")
        self.accumulator_count = max(1, int(os.getenv("ACCUMULATOR_COUNT", "1")))

        self._own_queues = []
        if sum_queues is not None:
            self.sum_queues = list(sum_queues)
            if not self.sum_queues:
                raise ValueError("Q5 Counter necesita al menos una cola en sum_queues")
            self.accumulator_count = len(self.sum_queues)
        elif self.accumulator_count > 1:
            base = os.getenv("OUT_QUEUE", "Q5_Sum")
            self._own_queues = [
                MessageMiddlewareQueueRabbitMQ(mom_host, f"{base}_{index}")
                for index in range(self.accumulator_count)
            ]
            self.sum_queues = self._own_queues
        else:
            if self.out_mw is None:
                raise ValueError("Q5 Counter necesita OUT_QUEUE")
            self.sum_queues = [self.out_mw]

        # Agrego una segunda cola para los articulos candidatos (mayores a 2021)
        if out_cand_mw is None:
            out_cand_queue = os.getenv("OUT_CAND_QUEUE", "Q5_Cand")
            out_cand_mw = MessageMiddlewareQueueRabbitMQ(mom_host, out_cand_queue)
        self.out_cand_mw = out_cand_mw

        # Mensajes enviados para cada cliente por cada cola
        self._counts = Counter()
        self._seen = Counter()

        self.group = GroupCoordinator(
            controller_type=CTRL_Q5_COUNTER,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
            origin_node=self.origin_node,
            next_msg_seq=self.next_msg_seq,
        )

        logger.info(
            "START node=%s/%s accumulators=%s",
            self.node_id,
            self.controller_count,
            len(self.sum_queues),
        )

    def handle_article(self, client_id, article):
        # Estado protegido porque se lee desde un hilo y actualiza desde otro
        with self._lock:
            if self.group.is_closed(client_id):
                return

            self._seen[client_id] += 1
            word_count = self._count_words(article.abstract)

            if word_count > 0 and article.dateModified and article.dateModified > 0:
                year = article.year
                if year is not None and year <= 2020:
                    bucket = article.docId % len(self.sum_queues)
                    msg_seq = self.next_msg_seq(client_id)
                    self.sum_queues[bucket].send(
                        internal.serialize_wordcount_sum(
                            client_id,
                            self.origin_node,
                            msg_seq,
                            article.articleId,
                            word_count,
                        )
                    )
                    self._counts[(client_id, "sum", bucket)] += 1
                elif year is not None and year >= 2021:
                    msg_seq = self.next_msg_seq(client_id)
                    self.out_cand_mw.send(
                        internal.serialize_wordcount_cand(
                            client_id,
                            self.origin_node,
                            msg_seq,
                            article.articleId,
                            word_count,
                            article.docId,
                            article.url,
                        )
                    )
                    self._counts[(client_id, "cand")] += 1

            # Actualizar cuenta de los articulos procesados
            self.group.on_processed(client_id)

            if self._seen[client_id] % PROGRESS_EVERY == 0:
                sum_total = sum(
                    self._counts[(client_id, "sum", i)]
                    for i in range(len(self.sum_queues))
                )
                logger.info(
                    "PROGRESS client=%s node=%s seen=%s sum=%s cand=%s",
                    client_id,
                    self.node_id,
                    self._seen[client_id],
                    sum_total,
                    self._counts[(client_id, "cand")],
                )

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_done(self, client_id, _quantity):
        sum_qtys = [
            self._counts[(client_id, "sum", index)]
            for index in range(len(self.sum_queues))
        ]
        cand_qty = self._counts[(client_id, "cand")]

        logger.info(
            "DONE client=%s node=%s sum=%s cand=%s",
            client_id,
            self.node_id,
            sum_qtys,
            cand_qty,
        )

        # Un DONE por particion Q5_Sum_i con la cantidad enviada a esa particion.
        for index, queue in enumerate(self.sum_queues):
            msg_seq = self.next_msg_seq(client_id)
            queue.send(
                control.serialize_done(
                    client_id,
                    self.origin_node,
                    msg_seq,
                    CTRL_Q5_COUNTER,
                    self.node_id,
                    sum_qtys[index],
                )
            )

        # Un DONE para Q5_Cand
        cand_seq = self.next_msg_seq(client_id)
        self.out_cand_mw.send(
            control.serialize_done(
                client_id,
                self.origin_node,
                cand_seq,
                CTRL_Q5_COUNTER,
                self.node_id,
                cand_qty,
            )
        )

        self._cleanup_client(client_id)

    def _cleanup_client(self, client_id):
        self._seen.pop(client_id, None)
        self._counts.pop((client_id, "cand"), None)
        for index in range(len(self.sum_queues)):
            self._counts.pop((client_id, "sum", index), None)

    @staticmethod
    def _count_words(abstract):
        if not isinstance(abstract, str):
            return 0

        words = re.findall(WORD_REGEX, abstract.lower())
        return len(
            {
                word
                for word in words
                if word not in STOP_WORDS and not re.fullmatch(NUMBER_REGEX, word)
            }
        )

    def stop(self):
        super().stop()
        for queue in [self.out_cand_mw, *self._own_queues]:
            try:
                queue.close()
            except Exception:
                pass
