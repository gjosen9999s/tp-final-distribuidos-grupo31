"""Q4: cuenta abstracts distintos por palabra y emite top-20 parcial."""

import heapq
import logging
import os
from collections import Counter
from dataclasses import dataclass, field

from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q4_COUNTER,
    CTRL_TOKENIZER,
    ControlType,
    InternalType,
)
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("Q4Counter")

TOP_SIZE = int(os.getenv("TOP_SIZE", "20"))
PROGRESS_EVERY = max(1, int(os.getenv("Q4_PROGRESS_EVERY", "100000")))

def top_words(counts, size=TOP_SIZE):
    return heapq.nsmallest(size, counts.items(), key=lambda item: (-item[1], item[0]))

@dataclass
class _ClientState:
    counts: Counter = field(default_factory=Counter)
    processed: int = 0
    total: int = 0
    dones: set = field(default_factory=set)
    closed: bool = False
    warned: bool = False


class Q4Counter(Controller):
    def __init__(self, *args, out_queues=None, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q4 Counter necesita OUT_QUEUE")

        self.tokenizers = max(1, int(os.getenv("TOKENIZER_COUNT", "1")))
        self.join_count = max(1, int(os.getenv("JOIN_COUNT", "1")))

        self._own_queues = []
        if out_queues is not None:
            self.out_queues = list(out_queues)
        elif self.join_count > 1:
            from common.middleware.middleware_rabbitmq import (
                MessageMiddlewareQueueRabbitMQ,
            )

            base = os.getenv("OUT_QUEUE", "Q4_Shared")
            mom_host = os.getenv("MOM_HOST", "localhost")
            self._own_queues = [
                MessageMiddlewareQueueRabbitMQ(mom_host, f"{base}_{index}")
                for index in range(self.join_count)
            ]
            self.out_queues = self._own_queues
        else:
            self.out_queues = [self.out_mw]

        self._states = {}

        logger.info(
            f"[Q4Counter] Iniciado nodo {self.node_id} "
            f"(tokenizers={self.tokenizers}, joins={len(self.out_queues)}, top={TOP_SIZE})"
        )

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def _on_data(self, body, ack, nack):
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                self._handle_done(msg_type, client_id, message)
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type != InternalType.WORD_Q4:
                    raise ValueError(f"tipo inesperado para Q4 Counter: {msg_type}")
                self._handle_wordlist(client_id, message["wordlist"])
            ack()
        except Exception:
            logger.exception(f"[Q4Counter] Error procesando mensaje (nodo {self.node_id})")
            nack()

    def _handle_wordlist(self, client_id, wordlist):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            # Sumo todas las palabaras de la lista
            for word in wordlist:
                state.counts[word] += 1

            state.processed += len(wordlist)
            
            if state.processed % PROGRESS_EVERY == 0:
                logger.info(
                    "[Q4Counter] PROGRESS client=%s node=%s processed=%s total=%s "
                    "dones=%s/%s distintas=%s",
                    client_id,
                    self.node_id,
                    state.processed,
                    state.total,
                    len(state.dones),
                    self.tokenizers,
                    len(state.counts),
                )
            self._emit_if_ready(client_id, state)

    def _handle_done(self, msg_type, client_id, message):
        if msg_type != ControlType.DONE:
            return

        if message["controllerType"] != CTRL_TOKENIZER:
            return

        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            key = (message["controllerType"], message["nodeId"])
            if key in state.dones:
                return

            state.dones.add(key)
            state.total += message["quantity"]
            logger.info(
                "[Q4Counter] DONE client=%s from_tokenizer=%s dones=%s/%s "
                "total=%s processed=%s",
                client_id,
                message["nodeId"],
                len(state.dones),
                self.tokenizers,
                state.total,
                state.processed,
            )
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        if state.closed:
            return

        if len(state.dones) < self.tokenizers:
            return

        if state.processed > state.total and not state.warned:
            state.warned = True
            logger.warning(
                f"[Q4Counter] Cliente ID={client_id}: se procesaron {state.processed} palabras "
                f"pero los DONE informan {state.total} (¿mensajes duplicados?). No se cierra."
            )

        if state.processed != state.total:
            return

        top = top_words(state.counts)
        queue = self.out_queues[client_id % len(self.out_queues)]

        queue.send(internal.serialize_q4_partial_message(client_id, self.node_id, top))
        queue.send(
            control.serialize_done(client_id, CTRL_Q4_COUNTER, self.node_id, 1)
        )

        logger.info(
            f"[Q4Counter] Cliente ID={client_id} cerrado (nodo {self.node_id}): "
            f"{state.processed} palabras, {len(state.counts)} distintas, "
            f"top parcial enviado a {getattr(queue, 'queue_name', 'Q4_Shared')} "
            f"(primero={top[0] if top else None})"
        )

        state.counts = Counter()
        state.closed = True

    def handle_article(self, client_id, article):
        pass

    def handle_control(self, msg_type, client_id, message, from_control):
        pass

    def stop(self):
        super().stop()
        for queue in self._own_queues:
            try:
                queue.close()
            except Exception:
                pass