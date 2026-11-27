"""Q5 Accumulator: acumula sum/count de <=2020 de su partición y emite el parcial."""

import logging
import os
from dataclasses import dataclass, field

from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q5_ACCUMULATOR,
    CTRL_Q5_COUNTER,
    ControlType,
    InternalType,
)
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("q5_accumulator")
PROGRESS_EVERY = 5000


# Estado de un cliente dentro de esta particion
@dataclass
class _ClientState:
    partial_words_sum: int = 0
    partial_article_qty: int = 0
    processed: int = 0
    total: int = 0
    dones: set = field(default_factory=set)
    seen_msgs: set = field(default_factory=set)
    warned: bool = False


class Q5Accumulator(Controller):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q5 Accumulator necesita OUT_QUEUE")

        # Cantidad de Q5Counter que producen para esta particion
        self.q5_counters_qty = max(1, int(os.getenv("COUNTER_COUNT", "1")))
        self._states = {}
        self._closed_clients = set()

        logger.info(
            "START node=%s counters=%s",
            self.node_id,
            self.q5_counters_qty,
        )

    def is_closed(self, client_id):
        with self._lock:
            return client_id in self._closed_clients

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
                self.handle_control(msg_type, client_id, message, False)
            else:
                msg_type, client_id, origin_node, msg_seq, message = internal.deserialize(body)
                if msg_type != InternalType.WORDCOUNT_SUM:
                    raise ValueError(f"Mensaje desconocido: {msg_type}")
                self.handle_article(
                    client_id,
                    {
                        "originNode": origin_node,
                        "msgSeq": msg_seq,
                        "wordCount": message["wordCount"],
                    },
                )
            ack()
        except Exception:
            logger.exception("Error procesando mensaje en q5_accumulator")
            nack()

    def handle_article(self, client_id, article):
        self._handle_sum(
            client_id, article["originNode"], article["msgSeq"], article["wordCount"]
        )

    def handle_control(self, msg_type, client_id, message, from_control):
        if from_control:
            return
        if msg_type != ControlType.DONE:
            return
        if message["controllerType"] != CTRL_Q5_COUNTER:
            return
        self._handle_done(client_id, message["nodeId"], message["quantity"])

    def _handle_sum(self, client_id, origin_node, msg_seq, word_count):
        with self._lock:
            if client_id in self._closed_clients:
                return

            state = self._state(client_id)
            msg_key = (origin_node, msg_seq)
            if msg_key in state.seen_msgs:
                return

            state.seen_msgs.add(msg_key)
            state.partial_words_sum += word_count
            state.partial_article_qty += 1
            state.processed += 1

            if state.processed % PROGRESS_EVERY == 0:
                logger.info(
                    "PROGRESS client=%s node=%s processed=%s total=%s dones=%s/%s sum=%s count=%s",
                    client_id,
                    self.node_id,
                    state.processed,
                    state.total,
                    len(state.dones),
                    self.q5_counters_qty,
                    state.partial_words_sum,
                    state.partial_article_qty,
                )

            self._emit_if_ready(client_id, state)

    def _handle_done(self, client_id, node_id, quantity):
        with self._lock:
            if client_id in self._closed_clients:
                return

            state = self._state(client_id)
            key = (CTRL_Q5_COUNTER, node_id)
            if key in state.dones:
                return

            state.dones.add(key)
            state.total += quantity
            logger.info(
                "DONE_COUNTER client=%s node=%s counter=%s qty=%s dones=%s/%s total=%s processed=%s",
                client_id,
                self.node_id,
                node_id,
                quantity,
                len(state.dones),
                self.q5_counters_qty,
                state.total,
                state.processed,
            )
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        if client_id in self._closed_clients:
            return

        if len(state.dones) < self.q5_counters_qty:
            return

        if state.processed > state.total and not state.warned:
            state.warned = True
            logger.warning(
                "Cliente ID=%s nodo=%s: procesados=%s pero los DONE informan %s",
                client_id,
                self.node_id,
                state.processed,
                state.total,
            )

        if state.processed != state.total:
            return

        words_sum = state.partial_words_sum
        articles_qty = state.partial_article_qty
        processed = state.processed
        total = state.total

        logger.info(
            "DONE client=%s node=%s words_sum=%s articles=%s processed=%s total=%s",
            client_id,
            self.node_id,
            words_sum,
            articles_qty,
            processed,
            total,
        )

        partial_seq = self.next_msg_seq(client_id)
        self.out_mw.send(
            internal.serialize_q5_acc_partial(
                client_id,
                self.origin_node,
                partial_seq,
                words_sum,
                articles_qty,
            )
        )

        done_seq = self.next_msg_seq(client_id)
        self.out_mw.send(
            control.serialize_done(
                client_id,
                self.origin_node,
                done_seq,
                CTRL_Q5_ACCUMULATOR,
                self.node_id,
                1,
            )
        )

        self._cleanup_client(client_id)

    def _cleanup_client(self, client_id):
        self._closed_clients.add(client_id)
        self._states.pop(client_id, None)
