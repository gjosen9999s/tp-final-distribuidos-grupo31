"""Q5: calcula el promedio y filtra los candidatos >=2021."""

import logging
import os
import threading
from dataclasses import dataclass, field

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.constants import (
    CTRL_EOF,
    CTRL_Q5_ACCUMULATOR,
    CTRL_Q5_AVG_FILTER,
    CTRL_Q5_COUNTER,
    FLAG_IS_LAST_PART,
    ControlType,
    InternalType,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.base.controller import Controller

logger = logging.getLogger("q5_average_n_filter")
PROGRESS_EVERY = 50000


@dataclass
class _ClientState:
    candidates: list = field(default_factory=list)
    total_words_sum: int = 0
    total_article_qty: int = 0
    received_counter_dones: set = field(default_factory=set)
    received_accumulator_dones: set = field(default_factory=set)
    closed: bool = False


class Q5AverageNFilter(Controller):
    def __init__(self, *args, in_acc_mw=None, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q5 Average Filter necesita OUT_QUEUE")

        mom_host = os.getenv("MOM_HOST", "localhost")

        # Segunda cola de entrada para los promedios parciales
        # y los DONE de los Accumulators
        if in_acc_mw is None:
            in_acc_queue = os.getenv("IN_ACC_QUEUE", "Q5_Acc_Partial")
            in_acc_mw = MessageMiddlewareQueueRabbitMQ(mom_host, in_acc_queue)
        self.in_acc_mw = in_acc_mw

        # Cantidad esperada de Counters y Accumulators
        self.q5_counters_qty = max(1, int(os.getenv("COUNTER_COUNT", "1")))
        self.q5_accumulators_qty = max(1, int(os.getenv("ACCUMULATOR_COUNT", "1")))

        self._states = {}
        self._acc_thread = None

        logger.info(
            "START node=%s counters=%s accumulators=%s",
            self.node_id,
            self.q5_counters_qty,
            self.q5_accumulators_qty,
        )

    def run(self):
        # Hilo para consumir la segunda cola de Accumulators
        self._acc_thread = threading.Thread(target=self._consume_acc)
        self._acc_thread.start()
        try:
            super().run()
        finally:
            if self._acc_thread and self._acc_thread.is_alive():
                self._acc_thread.join(timeout=5)

    def _stop_consumers(self):
        super()._stop_consumers()
        if self.in_acc_mw:
            try:
                self.in_acc_mw.stop_consuming_threadsafe()
            except Exception:
                pass

    def stop(self):
        super().stop()
        if self.in_acc_mw:
            try:
                self.in_acc_mw.close()
            except Exception:
                pass

    def _consume_acc(self):
        try:
            self.in_acc_mw.start_consuming(self._on_acc_data)
        except Exception:
            if not self._stopping.is_set():
                raise

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def _on_data(self, body, ack, nack):
        """Consume de la cola de Q5Counter."""
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE and message.get("controllerType") == CTRL_Q5_COUNTER:
                    self._handle_counter_done(client_id, message["nodeId"])
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type == InternalType.WORDCOUNT_CAND:
                    self._handle_candidate(
                        client_id,
                        message["url"],
                        message["wordCount"],
                        message["docId"],
                    )
                else:
                    raise ValueError(
                        f"tipo interno inesperado en cola de candidatos: {msg_type}"
                    )
            ack()
        except Exception:
            logger.exception("Error procesando mensaje en q5_average_n_filter (cola candidatos)")
            nack()

    def _on_acc_data(self, body, ack, nack):
        """Consume de la cola de Q5Accumulator."""
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE and message.get("controllerType") == CTRL_Q5_ACCUMULATOR:
                    self._handle_accumulator_done(client_id, message["nodeId"])
            else:
                msg_type, client_id, message = internal.deserialize(body)
                if msg_type == InternalType.Q5_ACC_PARTIAL:
                    self._handle_acc_partial(
                        client_id,
                        message["totalSum"],
                        message["count"],
                    )
                else:
                    raise ValueError(
                        f"tipo interno inesperado en cola de acumuladores: {msg_type}"
                    )
            ack()
        except Exception:
            logger.exception("Error procesando mensaje en q5_average_n_filter (cola acumuladores)")
            nack()

    def _handle_candidate(self, client_id, url, word_count, doc_id):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.candidates.append((url, word_count, doc_id))
            if len(state.candidates) % PROGRESS_EVERY == 0:
                logger.info(
                    "PROGRESS client=%s node=%s candidates=%s",
                    client_id,
                    self.node_id,
                    len(state.candidates),
                )
            self._emit_if_ready(client_id, state)

    def _handle_acc_partial(self, client_id, total_sum, count):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.total_words_sum += total_sum
            state.total_article_qty += count
            logger.info(
                "PARTIAL_ACC client=%s node=%s added_sum=%s added_count=%s total_sum=%s total_count=%s",
                client_id,
                self.node_id,
                total_sum,
                count,
                state.total_words_sum,
                state.total_article_qty,
            )
            self._emit_if_ready(client_id, state)

    def _handle_counter_done(self, client_id, node_id):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.received_counter_dones.add(node_id)
            logger.info(
                "DONE_COUNTER client=%s node=%s counter=%s (%s/%s)",
                client_id,
                self.node_id,
                node_id,
                len(state.received_counter_dones),
                self.q5_counters_qty,
            )
            self._emit_if_ready(client_id, state)

    def _handle_accumulator_done(self, client_id, node_id):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.received_accumulator_dones.add(node_id)
            logger.info(
                "DONE_ACCUMULATOR client=%s node=%s accumulator=%s (%s/%s)",
                client_id,
                self.node_id,
                node_id,
                len(state.received_accumulator_dones),
                self.q5_accumulators_qty,
            )
            self._emit_if_ready(client_id, state)

    def _emit_if_ready(self, client_id, state):
        if state.closed:
            return

        if len(state.received_counter_dones) < self.q5_counters_qty:
            return

        if len(state.received_accumulator_dones) < self.q5_accumulators_qty:
            return

        state.closed = True

        average = (
            state.total_words_sum / state.total_article_qty
            if state.total_article_qty > 0
            else 0.0
        )

        matching = [
            (url, wc, doc_id)
            for (url, wc, doc_id) in state.candidates
            if wc > average
        ]

        logger.info(
            "RESULT client=%s node=%s avg=%.2f candidates=%s matched=%s",
            client_id,
            self.node_id,
            average,
            len(state.candidates),
            len(matching),
        )

        for url, wc, doc_id in matching:
            payload = external.serialize_q5_payload(url, wc)
            self.out_mw.send(
                internal.serialize_result_message(
                    client_id,
                    ResultType.Q5,
                    doc_id,
                    0,
                    FLAG_IS_LAST_PART,
                    payload,
                )
            )

        self.out_mw.send(
            control.serialize_done(
                client_id,
                CTRL_Q5_AVG_FILTER,
                self.node_id,
                len(matching),
            )
        )
        logger.info(
            "DONE client=%s node=%s emitted=%s",
            client_id,
            self.node_id,
            len(matching),
        )

    def handle_article(self, client_id, article):
        raise RuntimeError("Q5AverageNFilter no recibe mensajes ARTICLE")

    def handle_control(self, msg_type, client_id, message, from_control):
        pass
