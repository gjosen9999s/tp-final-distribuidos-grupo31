"""Q5 Counter: cuenta palabras de cada abstract y enruta según el año (<=2020 o >=2021)."""

import os
import re
from collections import Counter

from common.middleware.group import GroupCoordinator
from common.protocol.constants import CTRL_Q5_COUNTER
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

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
    def __init__(self, *args, out_cand_mw=None, **kwargs):
        super().__init__(*args, **kwargs)
        mom_host = os.getenv("MOM_HOST", "localhost")

        # Agrego una segunda cola para los articulos candidatos (mayores a 2021)
        if out_cand_mw is None:
            out_cand_queue = os.getenv("OUT_CAND_QUEUE", "Q5_Cand")
            out_cand_mw = MessageMiddlewareQueueRabbitMQ(mom_host, out_cand_queue)
        self.out_cand_mw = out_cand_mw

        # Mensajes enviados para cada cliente por cada cola
        self._counts = Counter()

        self.group = GroupCoordinator(
            controller_type=CTRL_Q5_COUNTER,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
        )

    def handle_article(self, client_id, article):
        # Estado protegido porque se lee desde un hilo y actualiza desde otro
        with self._lock:
            if self.group.is_closed(client_id):
                return

            word_count = self._count_words(article.abstract)

            if word_count > 0 and article.dateModified and article.dateModified > 0:
                year = article.year
                if year is not None and year <= 2020:
                    self.out_mw.send(
                        internal.serialize_wordcount_sum(client_id, word_count)
                    )
                    self._counts[(client_id, "sum")] += 1
                elif year is not None and year >= 2021:
                    self.out_cand_mw.send(
                        internal.serialize_wordcount_cand(
                            client_id, word_count, article.docId, article.url
                        )
                    )
                    self._counts[(client_id, "cand")] += 1

            # Actualizar cuenta de los articulos procesados
            self.group.on_processed(client_id)

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_done(self, client_id, _quantity):
        sum_qty = self._counts[(client_id, "sum")]
        cand_qty = self._counts[(client_id, "cand")]

        # Un DONE para Q5_Sum
        self.out_mw.send(control.serialize_done(client_id,CTRL_Q5_COUNTER, self.node_id,sum_qty,))
        
        # Un DONE para Q5_Cand
        self.out_cand_mw.send(control.serialize_done(client_id,CTRL_Q5_COUNTER,self.node_id,cand_qty,))

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
        try:
            self.out_cand_mw.close()
        except Exception:
            pass
