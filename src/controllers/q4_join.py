"""Q4: fusiona los M top parciales y extrae el top-20 global."""

import logging
import os
from collections import Counter
from dataclasses import dataclass, field

from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q4_COUNTER,
    CTRL_Q4_JOIN,
    FLAG_IS_LAST_PART,
    ControlType,
    InternalType,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller
from controllers.q4_counter import TOP_SIZE, top_words

logger = logging.getLogger("Q4Join")


@dataclass
class _ClientState:
    partials: dict = field(default_factory=dict)
    dones: set = field(default_factory=set)
    closed: bool = False


class Q4Join(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q4 Join necesita OUT_QUEUE")

        self.partitions = max(1, int(os.getenv("PARTITIONS", "1")))
        self._states = {}

        logger.info(f"[Q4Join] Iniciado nodo {self.node_id} (particiones={self.partitions})")

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
                if msg_type != InternalType.Q4_PARTIAL:
                    raise ValueError(f"tipo inesperado para Q4 Join: {msg_type}")
                self._handle_partial(client_id, message["nodeId"], message["pairs"])
            ack()
        except Exception:
            logger.exception(f"[Q4Join] Error procesando mensaje (nodo {self.node_id})")
            nack()

    def _handle_partial(self, client_id, node_id, pairs):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            if node_id in state.partials:
                logger.warning(
                    f"[Q4Join] Cliente ID={client_id}: parcial repetido del Counter {node_id}; se ignora"
                )
                return

            state.partials[node_id] = pairs
            logger.debug(
                f"[Q4Join] Cliente ID={client_id}: parcial del Counter {node_id} "
                f"({len(state.partials)}/{self.partitions})"
            )
            self._emit_if_ready(client_id, state)

    def _handle_done(self, msg_type, client_id, message):
        if msg_type != ControlType.DONE:
            return

        if message["controllerType"] != CTRL_Q4_COUNTER:
            return

        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.dones.add(message["nodeId"])
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        if state.closed:
            return

        if len(state.partials) < self.partitions:
            return

        if len(state.dones) < self.partitions:
            return

        merged = Counter()
        for pairs in state.partials.values():
            for word, count in pairs:
                merged[word] += count

        top = top_words(merged, TOP_SIZE)

        self.out_mw.send(
            internal.serialize_result_message(
                client_id,
                ResultType.Q4,
                0,
                0,
                FLAG_IS_LAST_PART,
                external.serialize_q4_payload(top),
            )
        )
        self.out_mw.send(
            control.serialize_done(client_id, CTRL_Q4_JOIN, self.node_id, 1)
        )

        logger.info(
            f"[Q4Join] Cliente ID={client_id} cerrado: top-{len(top)} publicado "
            f"({', '.join(f'{word}={count}' for word, count in top[:3])}, ...)"
        )

        state.partials = {}
        state.closed = True

    def handle_article(self, client_id, article):
        pass

    def handle_control(self, msg_type, client_id, message, from_control):
        pass