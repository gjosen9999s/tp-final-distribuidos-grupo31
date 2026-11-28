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
from controllers.q5_persistence import (
    GLOBAL_SCOPE,
    NodeMetaTracker,
    build_state_store,
    client_scope,
    export_client_dedup,
    parse_client_scope,
    restore_client_dedup,
)

logger = logging.getLogger("q5_average_n_filter")
PROGRESS_EVERY = 50000


@dataclass
class _ClientState:
    candidates: list = field(default_factory=list)
    candidates_processed: int = 0
    expected_candidates: int = 0
    partials_by_acc: dict = field(default_factory=dict)
    received_counter_dones: set = field(default_factory=set)
    received_accumulator_dones: set = field(default_factory=set)
    seen_articles: set = field(default_factory=set)
    closed: bool = False


class Q5AverageNFilter(Controller):
    def __init__(self, *args, in_acc_mw=None, state_store=None, **kwargs):
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
        self.persist_batch_size = max(1, int(os.getenv("PERSIST_BATCH_SIZE", "50")))

        self._states = {}
        self._acc_thread = None

        self.store = build_state_store(self.controller_type, self.node_id, state_store)
        self._meta = NodeMetaTracker(self.store)
        self._closed_clients = self._meta.closed_clients

        self._cand_pending_acks = []
        self._dirty_deltas = {}

        if self.store is not None:
            self._restore_from_store()

        logger.info(
            "START node=%s counters=%s accumulators=%s persistence=%s",
            self.node_id,
            self.q5_counters_qty,
            self.q5_accumulators_qty,
            self.store is not None,
        )

    def _restore_from_store(self):
        with self._msg_seq_lock:
            for cid, reserved in self._meta.reserved_seqs.items():
                self._msg_seq[cid] = reserved

        for scope in self.store.list_scopes():
            if scope == GLOBAL_SCOPE:
                continue
            client_id = parse_client_scope(scope)
            if client_id is None:
                continue
            if self._meta.is_closed(client_id):
                self.store.clear(scope)
                continue

            snap, deltas, _ = self.store.load(scope)
            state = _ClientState()
            dedup_data = None
            if snap is not None:
                state.candidates = list(snap["candidates"])
                state.candidates_processed = snap["candidates_processed"]
                state.expected_candidates = snap["expected_candidates"]
                state.partials_by_acc = dict(snap["partials_by_acc"])
                state.received_counter_dones = set(snap["received_counter_dones"])
                state.received_accumulator_dones = set(snap["received_accumulator_dones"])
                state.seen_articles = set(snap["seen_articles"])
                dedup_data = snap.get("dedup")

            for delta in deltas:
                state.candidates.extend(delta.get("candidates_add", ()))
                state.candidates_processed += delta.get("candidates_processed_delta", 0)
                state.expected_candidates += delta.get("expected_candidates_delta", 0)
                state.partials_by_acc.update(delta.get("partials_update", {}))
                state.received_counter_dones.update(delta.get("counter_dones_add", ()))
                state.received_accumulator_dones.update(delta.get("acc_dones_add", ()))
                state.seen_articles.update(delta.get("seen_articles_add", ()))
                if "dedup" in delta:
                    dedup_data = delta["dedup"]

            restore_client_dedup(self.deduplicate, client_id, dedup_data)
            self._states[client_id] = state

    def next_msg_seq(self, client_id: int) -> int:
        seq = super().next_msg_seq(client_id)
        self._meta.ensure_seq_reserved(client_id, seq)
        return seq

    def run(self):
        # Hilo para consumir la segunda cola de Accumulators
        self._acc_thread = threading.Thread(target=self._consume_acc)
        self._acc_thread.start()
        try:
            super().run()
        finally:
            if self._acc_thread and self._acc_thread.is_alive():
                self._acc_thread.join(timeout=5)
            if self.store is not None:
                self.store.close()

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
        if self.store is not None and self._control_thread is None:
            self.store.close()

    def _consume_acc(self):
        try:
            self.in_acc_mw.start_consuming(self._on_acc_data)
        except Exception:
            if not self._stopping.is_set():
                raise

    def is_closed(self, client_id):
        return self._meta.is_closed(client_id)

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _ClientState()
            self._states[client_id] = state
        return state

    def _dirty_delta(self, client_id):
        delta = self._dirty_deltas.get(client_id)
        if delta is None:
            delta = {
                "candidates_add": [],
                "candidates_processed_delta": 0,
                "expected_candidates_delta": 0,
                "partials_update": {},
                "counter_dones_add": [],
                "acc_dones_add": [],
                "seen_articles_add": [],
            }
            self._dirty_deltas[client_id] = delta
        return delta

    def _export_snapshot_state(self, client_id, state):
        return {
            "candidates": list(state.candidates),
            "candidates_processed": state.candidates_processed,
            "expected_candidates": state.expected_candidates,
            "partials_by_acc": dict(state.partials_by_acc),
            "received_counter_dones": set(state.received_counter_dones),
            "received_accumulator_dones": set(state.received_accumulator_dones),
            "seen_articles": set(state.seen_articles),
            "dedup": export_client_dedup(self.deduplicate, client_id),
        }

    def _persist_client_locked(self, client_id):
        if self.store is None:
            self._dirty_deltas.pop(client_id, None)
            return
        delta = self._dirty_deltas.pop(client_id, None)
        if delta is None or self._meta.is_closed(client_id) or client_id not in self._states:
            return
        scope = client_scope(client_id)
        delta["dedup"] = export_client_dedup(self.deduplicate, client_id)
        cid = self.store.append_wal(scope, delta)
        if self.store.should_compact(scope):
            self.store.save_snapshot(
                scope,
                self._export_snapshot_state(client_id, self._states[client_id]),
                checkpoint_id=cid,
            )

    def _persist_all_dirty_locked(self):
        for client_id in list(self._dirty_deltas.keys()):
            self._persist_client_locked(client_id)

    def _flush_cand_batch(self):
        with self._lock:
            self._persist_all_dirty_locked()
            acks = list(self._cand_pending_acks)
            self._cand_pending_acks.clear()

        for ack_fn in acks:
            ack_fn()

    def _on_data(self, body, ack, nack):
        """Consume de la cola de Q5Counter."""
        try:
            is_control = body[0] >= CTRL_EOF
            if is_control:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE and message.get("controllerType") == CTRL_Q5_COUNTER:
                    self._handle_counter_done(
                        client_id, message["nodeId"], message["quantity"]
                    )
            else:
                msg_type, client_id, origin_node, msg_seq, message = internal.deserialize(body)
                if msg_type == InternalType.WORDCOUNT_CAND:
                    if self.deduplicate.is_duplicate(client_id, origin_node, msg_seq):
                        if self.store is None:
                            ack()
                        else:
                            with self._lock:
                                self._cand_pending_acks.append(ack)
                                should_flush = len(self._cand_pending_acks) >= self.persist_batch_size
                            if should_flush:
                                self._flush_cand_batch()
                        return

                    self._handle_candidate(
                        client_id,
                        origin_node,
                        msg_seq,
                        message["articleId"],
                        message["url"],
                        message["wordCount"],
                        message["docId"],
                    )
                else:
                    raise ValueError(
                        f"tipo interno inesperado en cola de candidatos: {msg_type}"
                    )

            if self.store is None:
                ack()
                return

            with self._lock:
                self._cand_pending_acks.append(ack)
                should_flush = (
                    is_control
                    or self._meta.is_closed(client_id)
                    or len(self._cand_pending_acks) >= self.persist_batch_size
                )
            if should_flush:
                self._flush_cand_batch()
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
                msg_type, client_id, origin_node, msg_seq, message = internal.deserialize(body)
                if msg_type == InternalType.Q5_ACC_PARTIAL:
                    if self.deduplicate.is_duplicate(client_id, origin_node, msg_seq):
                        ack()
                        return

                    self._handle_acc_partial(
                        client_id,
                        origin_node,
                        msg_seq,
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

    def _handle_candidate(
        self, client_id, origin_node, msg_seq, article_id, url, word_count, doc_id
    ):
        with self._lock:
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            if state.closed:
                return

            delta = self._dirty_delta(client_id) if self.store is not None else None
            if article_id not in state.seen_articles:
                state.seen_articles.add(article_id)
                cand_tuple = (article_id, url, word_count, doc_id)
                state.candidates.append(cand_tuple)
                if delta is not None:
                    delta["seen_articles_add"].append(article_id)
                    delta["candidates_add"].append(cand_tuple)

            state.candidates_processed += 1
            if delta is not None:
                delta["candidates_processed_delta"] += 1

            if state.candidates_processed % PROGRESS_EVERY == 0:
                logger.info(
                    "PROGRESS client=%s node=%s candidates=%s processed=%s expected=%s",
                    client_id,
                    self.node_id,
                    len(state.candidates),
                    state.candidates_processed,
                    state.expected_candidates,
                )
            self._emit_if_ready(client_id, state)

    def _handle_acc_partial(self, client_id, origin_node, msg_seq, total_sum, count):
        with self._lock:
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            if state.closed:
                return

            state.partials_by_acc[origin_node] = (total_sum, count)
            if self.store is not None:
                self._dirty_delta(client_id)["partials_update"][origin_node] = (
                    total_sum,
                    count,
                )

            logger.info(
                "PARTIAL_ACC client=%s node=%s origin=%s sum=%s count=%s partials=%s/%s",
                client_id,
                self.node_id,
                origin_node,
                total_sum,
                count,
                len(state.partials_by_acc),
                self.q5_accumulators_qty,
            )
            self._emit_if_ready(client_id, state)
            self._persist_client_locked(client_id)

    def _handle_counter_done(self, client_id, node_id, quantity=0):
        with self._lock:
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            if state.closed or node_id in state.received_counter_dones:
                return

            state.received_counter_dones.add(node_id)
            state.expected_candidates += quantity
            if self.store is not None:
                delta = self._dirty_delta(client_id)
                delta["counter_dones_add"].append(node_id)
                delta["expected_candidates_delta"] += quantity

            logger.info(
                "DONE_COUNTER client=%s node=%s counter=%s qty=%s (%s/%s) expected=%s processed=%s",
                client_id,
                self.node_id,
                node_id,
                quantity,
                len(state.received_counter_dones),
                self.q5_counters_qty,
                state.expected_candidates,
                state.candidates_processed,
            )
            self._emit_if_ready(client_id, state)

    def _handle_accumulator_done(self, client_id, node_id):
        with self._lock:
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            if state.closed or node_id in state.received_accumulator_dones:
                return

            state.received_accumulator_dones.add(node_id)
            if self.store is not None:
                self._dirty_delta(client_id)["acc_dones_add"].append(node_id)

            logger.info(
                "DONE_ACCUMULATOR client=%s node=%s accumulator=%s (%s/%s)",
                client_id,
                self.node_id,
                node_id,
                len(state.received_accumulator_dones),
                self.q5_accumulators_qty,
            )
            self._emit_if_ready(client_id, state)
            self._persist_client_locked(client_id)

    def _emit_if_ready(self, client_id, state):
        if state.closed:
            return

        if len(state.received_counter_dones) < self.q5_counters_qty:
            return

        if state.candidates_processed < state.expected_candidates:
            return

        if len(state.received_accumulator_dones) < self.q5_accumulators_qty:
            return

        if len(state.partials_by_acc) < self.q5_accumulators_qty:
            return

        state.closed = True

        total_words_sum = sum(s for s, _ in state.partials_by_acc.values())
        total_article_qty = sum(c for _, c in state.partials_by_acc.values())

        average = (
            total_words_sum / total_article_qty
            if total_article_qty > 0
            else 0.0
        )

        matching = [
            (article_id, url, wc, doc_id)
            for (article_id, url, wc, doc_id) in state.candidates
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

        for article_id, url, wc, doc_id in matching:
            payload = external.serialize_q5_payload(url, wc)
            msg_seq = self.next_msg_seq(client_id)

            self.out_mw.send(
                internal.serialize_result_message(
                    client_id,
                    self.origin_node,
                    msg_seq,
                    article_id,
                    ResultType.Q5,
                    doc_id,
                    0,
                    FLAG_IS_LAST_PART,
                    payload,
                )
            )

        msg_seq = self.next_msg_seq(client_id)
        self.out_mw.send(
            control.serialize_done(
                client_id,
                self.origin_node,
                msg_seq,
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
        self._cleanup_client(client_id)

    def _cleanup_client(self, client_id):
        self._dirty_deltas.pop(client_id, None)
        self._states.pop(client_id, None)
        self.deduplicate.clear_client(client_id)
        self._meta.mark_client_closed(client_id)

    def handle_article(self, client_id, article):
        raise RuntimeError("Q5AverageNFilter no recibe mensajes ARTICLE")

    def handle_control(self, msg_type, client_id, message, from_control):
        pass
