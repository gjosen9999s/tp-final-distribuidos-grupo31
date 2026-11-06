"""Q3 Join: junta los parciales de todas las particiones."""

import logging
import os
from dataclasses import dataclass, field

from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q3_JOIN,
    CTRL_Q3_REDUCE,
    FLAG_IS_LAST_PART,
    ControlType,
    InternalType,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("q3_join")



# Estado de un cliente dentro de esta particion
@dataclass
class _ClientState:
    partials: list = field(default_factory=list)
    dones: set = field(default_factory=set)
    closed: bool = False


class Q3Join(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q3 Join necesita OUT_QUEUE")

        # Tiene que coincidir con la cantidad de nodos Q3 Reduce
        self.partitions = max(1, int(os.getenv("PARTITIONS", "1")))
        self._states = {}

        logger.info(
            "START node=%s partitions=%s",
            self.node_id,
            self.partitions,
        )

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def _on_data(self, body, ack, nack):
        
        # Por Q3_Shared llegan los parciales y los DONE de los Reduces
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                self._handle_done(msg_type, client_id, message)
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type != InternalType.Q3_PARTIAL:
                    raise ValueError(f"tipo inesperado para Q3 Join: {msg_type}")
                self._handle_partial(client_id, message["words"])
            ack()
        except Exception:
            nack()

    def _handle_partial(self, client_id, words):
        
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            # Un parcial por cada partición. No importa el orden en que lleguen.
            state.partials.append(words)
            logger.info(
                "PARTIAL client=%s partials=%s/%s",
                client_id,
                len(state.partials),
                self.partitions,
            )
            self._emit_if_ready(client_id, state)

    def _handle_done(self, msg_type, client_id, message):
        
        if msg_type != ControlType.DONE:
            return
        
        if message["controllerType"] != CTRL_Q3_REDUCE:
            return

        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            # Deduplica para no contar doble
            node_id = message["nodeId"]
            if node_id in state.dones:
                return

            state.dones.add(node_id)
            logger.info(
                "DONE client=%s node=%s dones=%s/%s",
                client_id,
                node_id,
                len(state.dones),
                self.partitions,
            )
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        
        # Se espera tener la misma cantidad de datos (parciales) que de Done
        if state.closed:
            return
        
        if len(state.partials) < self.partitions:
            return
        
        if len(state.dones) < self.partitions:
            return

        words = []
        for partial in state.partials:
            words.extend(partial)
        words = sorted(words)
        logger.info(
            "RESULT client=%s words=%s partials=%s dones=%s",
            client_id,
            len(words),
            len(state.partials),
            len(state.dones),
        )

        payload = external.serialize_q3_payload(words)
        self.out_mw.send(
            internal.serialize_result_message(
                client_id,
                ResultType.Q3,
                0,
                0,
                FLAG_IS_LAST_PART,
                payload,
            )
        )
        # Avisamos al ArticleHandler que Q3 terminó.
        self.out_mw.send(
            control.serialize_done(
                client_id, CTRL_Q3_JOIN, self.node_id, 1
            )
        )
        state.closed = True

    # No se usan. Pero se definen dado que se heredan.
    def handle_article(self, client_id, article):
        pass

    def handle_control(self, msg_type, client_id, message, from_control):
        pass
