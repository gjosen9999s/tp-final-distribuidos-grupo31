"""Q3 Reduce: palabras vistas al menos una vez en inglés y en francés."""

import logging
import os
from dataclasses import dataclass, field

from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q3_REDUCE,
    CTRL_TOKENIZER,
    LANG_EN,
    LANG_FR,
    ControlType,
    InternalType,
)
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("q3_reduce")
PROGRESS_EVERY = 100000


# Estado de un cliente dentro de esta particion
@dataclass
class _ClientState:
    en_words: set = field(default_factory=set)
    fr_words: set = field(default_factory=set)
    processed: int = 0
    total: int = 0
    dones: set = field(default_factory=set)
    closed: bool = False


class Q3Reduce(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q3 Reduce necesita OUT_QUEUE")

        # Cantidad de Tokenizers que producen para esta particion
        self.tokenizers = max(1, int(os.getenv("TOKENIZER_COUNT", "1")))

        # Estado x cliente, se crea cuando llega.
        self._states = {}

        logger.info(
            "START node=%s tokenizers=%s",
            self.node_id,
            self.tokenizers,
        )

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def _on_data(self, body, ack, nack):
        # Por esta cola llegan dos cosas WORD_Q3 y el DONE del tokenizer
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                self._handle_done(msg_type, client_id, message)
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type != InternalType.WORD_Q3:
                    raise ValueError(f"tipo inesperado para Q3 Reduce: {msg_type}")
                self._handle_word(
                    client_id, message["word"], message["inLanguage"]
                )
            ack()
        except Exception:
            nack()

    def _handle_word(self, client_id, word, in_language):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            # Se cuentan todas las palabras recibidas indpte del idioma porque se reciben sin filtrar
            state.processed += 1

            if in_language == LANG_EN:
                state.en_words.add(word)
            elif in_language == LANG_FR:
                state.fr_words.add(word)

            if state.processed % PROGRESS_EVERY == 0:
                logger.info(
                    "PROGRESS client=%s node=%s processed=%s total=%s dones=%s/%s",
                    client_id,
                    self.node_id,
                    state.processed,
                    state.total,
                    len(state.dones),
                    self.tokenizers,
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

            # DONE tiene la cant de palabras que mando Tokenizer a esta particion
            # Deduplico por nodo para no sumar doble
            key = (message["controllerType"], message["nodeId"])
            if key in state.dones:
                return

            state.dones.add(key)
            state.total += message["quantity"]
            logger.info(
                "DONE client=%s node=%s from=%s qty=%s dones=%s/%s total=%s processed=%s",
                client_id,
                self.node_id,
                message["nodeId"],
                message["quantity"],
                len(state.dones),
                self.tokenizers,
                state.total,
                state.processed,
            )
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        
        # Cerramos recién cuando llegaron todos los DONEs y procesamos todo.
        if state.closed:
            return
        
        if len(state.dones) < self.tokenizers:
            return
        
        if state.processed != state.total:
            return

        # Se envia resultado parcial de este nodo y el DONE

        words = sorted(state.en_words & state.fr_words)
        logger.info(
            "EMIT client=%s node=%s words=%s processed=%s total=%s dones=%s",
            client_id,
            self.node_id,
            len(words),
            state.processed,
            state.total,
            len(state.dones),
        )

        self.out_mw.send(
            internal.serialize_q3_partial_message(client_id, words)
        )

        self.out_mw.send(
            control.serialize_done(
                client_id, CTRL_Q3_REDUCE, self.node_id, 1
            )
        )
        state.closed = True

    # No se usan. Pero se definen dado que se heredan.
    def handle_article(self, client_id, article):
        pass

    def handle_control(self, msg_type, client_id, message, from_control):
        pass
