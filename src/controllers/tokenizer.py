"""Extrae palabras no-stopword del abstract y las reparte por hash."""

import os
import re
import zlib
from collections import Counter

from nltk.corpus import stopwords

from common.middleware.group import GroupCoordinator
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.constants import CTRL_TOKENIZER
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

# Las stopwords se cargan una sola vez para todo el proceso.
STOP_WORDS = set(stopwords.words("english")) | set(stopwords.words("french"))
WORD_REGEX = r"\b\w+\b"
NUMBER_REGEX = r"[+-]?((\d+\.?\d*)|(\.\d+))"


class Tokenizer(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.partitions = int(os.getenv("PARTITIONS", "1"))
        self.q4_enabled = os.getenv("Q4_ENABLED", "1") != "0"
        mom_host = os.getenv("MOM_HOST", "localhost")

        # Una cola por particion para q3
        self.q3_queues = [
            MessageMiddlewareQueueRabbitMQ(mom_host, f"Q3_Words_{i}")
            for i in range(self.partitions)
        ]

        # Una cola por particion para q4, solo si q4 esta habilitado.
        self.q4_queues = (
            [
                MessageMiddlewareQueueRabbitMQ(mom_host, f"Q4_Words_{i}")
                for i in range(self.partitions)
            ]
            if self.q4_enabled
            else []
        )

        # Palabras envidadas para cada cliente para cada cola
        self._counts = Counter()

        self.group = GroupCoordinator(
            controller_type=CTRL_TOKENIZER,
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

            # Dentro de un mismo abstract, cada palabra cuenta una sola vez (set)
            words = self._tokenize(article.abstract)

            for word in words:
                # La misma palabra tiene que ir siempre a la misma particion de salida
                bucket = zlib.crc32(word.encode("utf-8")) % self.partitions

                self.q3_queues[bucket].send(
                    internal.serialize_word_q3_message(
                        client_id, word, article.inLanguage
                    )
                )

                if self.q4_enabled:
                    self.q4_queues[bucket].send(
                        internal.serialize_word_q4_message(
                            client_id, article.docId, word
                        )
                    )
                    self._counts[(client_id, "q4", bucket)] += 1

                self._counts[(client_id, "q3", bucket)] += 1

            # El group cuenta los artículos procesados (EOF, PROGRESS, FLUSH).
            self.group.on_processed(client_id)

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_done(self, client_id, _quantity):

        # Un DONE por cola de q3/particion con la cantidad enviada (msg) a esa cola.
        
        for index, queue in enumerate(self.q3_queues):
            queue.send(
                control.serialize_done(
                    client_id,
                    CTRL_TOKENIZER,
                    self.node_id,
                    self._counts[(client_id, "q3", index)],
                )
            )

        # Un DONE por cola de q4/particion con la cantidad enviada (msg) a esa cola.
        for index, queue in enumerate(self.q4_queues):
            queue.send(
                control.serialize_done(
                    client_id,
                    CTRL_TOKENIZER,
                    self.node_id,
                    self._counts[(client_id, "q4", index)],
                )
            )

    # Misma logica que el notebook / analyzer. Retorna un set (palabras unicas) por cada abstract recibido
    @staticmethod
    def _tokenize(abstract):
        if not isinstance(abstract, str):
            return set()

        words = re.findall(WORD_REGEX, abstract.lower())
        return {
            word
            for word in words
            if word not in STOP_WORDS and not re.fullmatch(NUMBER_REGEX, word)
        } #set

    def stop(self):
        super().stop()
        for queue in self.q3_queues + self.q4_queues:
            try:
                queue.close()
            except Exception:
                pass
