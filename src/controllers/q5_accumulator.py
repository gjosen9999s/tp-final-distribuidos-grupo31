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
from controllers.q5_persistence import (
    GLOBAL_SCOPE,
    NodeMetaTracker,
    build_state_store,
    client_scope,
    export_client_dedup,
    parse_client_scope,
    restore_client_dedup,
)

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
    seen_articles: set = field(default_factory=set)
    warned: bool = False


class Q5Accumulator(Controller):
    def __init__(self, *args, state_store=None, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None:
            raise ValueError("Q5 Accumulator necesita OUT_QUEUE")

        # Cantidad de Q5Counter que producen para esta particion
        self.q5_counters_qty = max(1, int(os.getenv("COUNTER_COUNT", "1")))
        self.persist_batch_size = max(1, int(os.getenv("PERSIST_BATCH_SIZE", "50")))
        self._states = {}

        self.store = build_state_store(self.controller_type, self.node_id, state_store)
        self._meta = NodeMetaTracker(self.store)
        self._closed_clients = self._meta.closed_clients

        self._pending_acks = []
        self._dirty_deltas = {}

        if self.store is not None:
            self._restore_from_store()

        logger.info(
            "START node=%s counters=%s persistence=%s",
            self.node_id,
            self.q5_counters_qty,
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
                state.partial_words_sum = snap["partial_words_sum"]
                state.partial_article_qty = snap["partial_article_qty"]
                state.processed = snap["processed"]
                state.total = snap["total"]
                state.dones = set(snap["dones"])
                state.seen_articles = set(snap["seen_articles"])
                dedup_data = snap.get("dedup")

            for delta in deltas:
                state.partial_words_sum += delta.get("words_sum_delta", 0)
                state.partial_article_qty += delta.get("article_qty_delta", 0)
                state.processed += delta.get("processed_delta", 0)
                state.total += delta.get("total_delta", 0)
                state.dones.update(delta.get("dones_add", ()))
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
        try:
            super().run()
        finally:
            if self.store is not None:
                self.store.close()

    def stop(self):
        super().stop()
        if self.store is not None and self._control_thread is None:
            self.store.close()

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
                "words_sum_delta": 0,
                "article_qty_delta": 0,
                "processed_delta": 0,
                "total_delta": 0,
                "dones_add": [],
                "seen_articles_add": [],
            }
            self._dirty_deltas[client_id] = delta
        return delta

    def _export_snapshot_state(self, client_id, state):
        return {
            "partial_words_sum": state.partial_words_sum,
            "partial_article_qty": state.partial_article_qty,
            "processed": state.processed,
            "total": state.total,
            "dones": set(state.dones),
            "seen_articles": set(state.seen_articles),
            "dedup": export_client_dedup(self.deduplicate, client_id),
        }

    def _persist_dirty_locked(self):
        if self.store is None or not self._dirty_deltas:
            self._dirty_deltas.clear()
            return

        for client_id, delta in list(self._dirty_deltas.items()):
            if self._meta.is_closed(client_id) or client_id not in self._states:
                continue
            scope = client_scope(client_id)
            delta["dedup"] = export_client_dedup(self.deduplicate, client_id)
            cid = self.store.append_wal(scope, delta)
            if self.store.should_compact(scope):
                self.store.save_snapshot(
                    scope,
                    self._export_snapshot_state(client_id, self._states[client_id]),
                    checkpoint_id=cid,
                )
        self._dirty_deltas.clear()

    def _flush_batch(self):
        with self._lock:
            self._persist_dirty_locked()
            acks = list(self._pending_acks)
            self._pending_acks.clear()

        for ack_fn in acks:
            ack_fn()

    def _on_data(self, body, ack, nack):
        try:
            is_control = body[0] >= CTRL_EOF
            if is_control:
                msg_type, client_id, message = control.deserialize(body)
                self.handle_control(msg_type, client_id, message, False)
            else:
                msg_type, client_id, origin_node, msg_seq, message = internal.deserialize(body)
                if msg_type != InternalType.WORDCOUNT_SUM:
                    raise ValueError(f"Mensaje desconocido: {msg_type}")
                if self.deduplicate.is_duplicate(client_id, origin_node, msg_seq):
                    if self.store is None:
                        ack()
                    else:
                        with self._lock:
                            self._pending_acks.append(ack)
                            should_flush = len(self._pending_acks) >= self.persist_batch_size
                        if should_flush:
                            self._flush_batch()
                    return

                self.handle_article(
                    client_id,
                    {
                        "originNode": origin_node,
                        "msgSeq": msg_seq,
                        "articleId": message["articleId"],
                        "wordCount": message["wordCount"],
                    },
                )

            if self.store is None:
                ack()
                return

            with self._lock:
                self._pending_acks.append(ack)
                should_flush = (
                    is_control
                    or self._meta.is_closed(client_id)
                    or len(self._pending_acks) >= self.persist_batch_size
                )
            if should_flush:
                self._flush_batch()
        except Exception:
            logger.exception("Error procesando mensaje en q5_accumulator")
            nack()

    def handle_article(self, client_id, article):
        self._handle_sum(
            client_id,
            article["originNode"],
            article["msgSeq"],
            article["articleId"],
            article["wordCount"],
        )

    def handle_control(self, msg_type, client_id, message, from_control):
        if from_control:
            return
        if msg_type != ControlType.DONE:
            return
        if message["controllerType"] != CTRL_Q5_COUNTER:
            return
        self._handle_done(client_id, message["nodeId"], message["quantity"])

    def _handle_sum(self, client_id, origin_node, msg_seq, article_id, word_count):
        with self._lock:
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            delta = self._dirty_delta(client_id) if self.store is not None else None

            if article_id not in state.seen_articles:
                state.seen_articles.add(article_id)
                state.partial_words_sum += word_count
                state.partial_article_qty += 1
                if delta is not None:
                    delta["words_sum_delta"] += word_count
                    delta["article_qty_delta"] += 1
                    delta["seen_articles_add"].append(article_id)

            state.processed += 1
            if delta is not None:
                delta["processed_delta"] += 1

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
            if self._meta.is_closed(client_id):
                return

            state = self._state(client_id)
            key = (CTRL_Q5_COUNTER, node_id)
            if key in state.dones:
                return

            state.dones.add(key)
            state.total += quantity
            if self.store is not None:
                delta = self._dirty_delta(client_id)
                delta["dones_add"].append(key)
                delta["total_delta"] += quantity

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
        if self._meta.is_closed(client_id):
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

        if state.processed < state.total:
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
        self._dirty_deltas.pop(client_id, None)
        self._states.pop(client_id, None)
        self.deduplicate.clear_client(client_id)
        self._meta.mark_client_closed(client_id)
