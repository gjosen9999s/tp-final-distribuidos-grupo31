"""Q5 Counter: cuenta palabras únicas por abstract y rutea por fecha."""

import logging
import os
import re
from collections import Counter

from nltk.corpus import stopwords

from common.middleware.group import GroupCoordinator, _GroupState
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.constants import CTRL_EOF, CTRL_Q5_COUNTER, InternalType
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

STOP_WORDS = set(stopwords.words("english")) | set(stopwords.words("french"))
WORD_REGEX = r"\b\w+\b"
NUMBER_REGEX = r"[+-]?((\d+\.?\d*)|(\.\d+))"

logger = logging.getLogger("q5_counter")
PROGRESS_EVERY = 5000


class Q5Counter(Controller):
    def __init__(self, *args, out_cand_mw=None, sum_queues=None, state_store=None, **kwargs):
        super().__init__(*args, **kwargs)

        if self.out_mw is None and sum_queues is None:
            raise ValueError("Q5 Counter necesita OUT_QUEUE o sum_queues")

        mom_host = os.getenv("MOM_HOST", "localhost")

        # Cola de salida para los pre-2021 particionados hacia Q5Accumulator_i
        self._own_queues = []
        if sum_queues is not None:
            self.sum_queues = list(sum_queues)
        else:
            acc_count = max(1, int(os.getenv("ACCUMULATOR_COUNT", "1")))
            if acc_count > 1:
                base = os.getenv("OUT_QUEUE", "Q5_Sum")
                self._own_queues = [
                    MessageMiddlewareQueueRabbitMQ(mom_host, f"{base}_{index}")
                    for index in range(acc_count)
                ]
                self.sum_queues = self._own_queues
            else:
                self.sum_queues = [self.out_mw]

        if not self.sum_queues:
            raise ValueError("Q5 Counter necesita al menos una cola en sum_queues")

        # Segunda cola de salida para los candidatos >=2021
        if out_cand_mw is None:
            cand_queue = os.getenv("OUT_CAND_QUEUE", "Q5_Candidates")
            out_cand_mw = MessageMiddlewareQueueRabbitMQ(mom_host, cand_queue)
        self.out_cand_mw = out_cand_mw

        # Contadores por cliente: ("sum", partition_idx) y ("cand")
        self._counts = Counter()
        self._seen = Counter()
        self.persist_batch_size = max(1, int(os.getenv("PERSIST_BATCH_SIZE", "50")))

        self.group = GroupCoordinator(
            controller_type=CTRL_Q5_COUNTER,
            node_id=self.node_id,
            progress_batch_size=self.progress_batch_size,
            controller_count=self.controller_count,
            control_out=self.control_out,
            done_sender=self._send_done,
            origin_node=self.origin_node,
            next_msg_seq=self.next_msg_seq,
        )

        self.store = build_state_store(self.controller_type, self.node_id, state_store)
        self._meta = NodeMetaTracker(self.store)
        self._pending_acks = []
        self._dirty_clients = set()

        if self.store is not None:
            self._restore_from_store()

        logger.info(
            "START node=%s/%s accumulators=%s persistence=%s",
            self.node_id,
            self.controller_count,
            len(self.sum_queues),
            self.store is not None,
        )

    def _export_group_state(self, client_id):
        with self.group._lock:
            gs = self.group._states.get(client_id)
            if gs is None:
                return None
            return {
                "total": gs.total,
                "processed": gs.processed,
                "last_reported": gs.last_reported,
                "sum": gs.sum,
                "emitted": gs.emitted,
                "flush_mode": gs.flush_mode,
                "flushed": gs.flushed,
                "closed": gs.closed,
                "flush_id": gs.flush_id,
            }

    def _restore_group_state(self, client_id, data):
        if not data:
            return
        with self.group._lock:
            self.group._states[client_id] = _GroupState(**data)

    def _export_client_state(self, client_id):
        return {
            "seen": self._seen[client_id],
            "cand": self._counts[(client_id, "cand")],
            "sums": [
                self._counts[(client_id, "sum", i)]
                for i in range(len(self.sum_queues))
            ],
            "group": self._export_group_state(client_id),
            "dedup": export_client_dedup(self.deduplicate, client_id),
            "group_dedup": export_client_dedup(self.group.deduplicate, client_id),
        }

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
            latest = deltas[-1] if deltas else snap
            if latest is None:
                continue

            self._seen[client_id] = latest["seen"]
            self._counts[(client_id, "cand")] = latest["cand"]
            for idx, val in enumerate(latest["sums"]):
                if idx < len(self.sum_queues):
                    self._counts[(client_id, "sum", idx)] = val
            self._restore_group_state(client_id, latest.get("group"))
            restore_client_dedup(self.deduplicate, client_id, latest.get("dedup"))
            restore_client_dedup(
                self.group.deduplicate, client_id, latest.get("group_dedup")
            )

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

    def _persist_client_locked(self, client_id):
        if self.store is None or self._meta.is_closed(client_id):
            self._dirty_clients.discard(client_id)
            return
        scope = client_scope(client_id)
        snapshot_like = self._export_client_state(client_id)
        cid = self.store.append_wal(scope, snapshot_like)
        if self.store.should_compact(scope):
            self.store.save_snapshot(scope, snapshot_like, checkpoint_id=cid)
        self._dirty_clients.discard(client_id)

    def _persist_dirty_locked(self):
        for client_id in list(self._dirty_clients):
            self._persist_client_locked(client_id)

    def _drain_pending_acks(self):
        with self._lock:
            acks = list(self._pending_acks)
            self._pending_acks.clear()
        for ack_fn in acks:
            ack_fn()

    def _schedule_drain_acks_from_other_thread_locked(self):
        if not self._pending_acks:
            return
        conn = getattr(self.in_mw, "connection", None)
        if conn is not None and getattr(conn, "is_open", False):
            try:
                conn.add_callback_threadsafe(self._drain_pending_acks)
                return
            except Exception:
                pass

    def _flush_data_batch(self):
        with self._lock:
            self._persist_dirty_locked()
            acks = list(self._pending_acks)
            self._pending_acks.clear()
        for ack_fn in acks:
            ack_fn()

    def _on_data(self, body, ack, nack):
        if self.store is None:
            super()._on_data(body, ack, nack)
            return

        try:
            is_control = body[0] >= CTRL_EOF
            if is_control:
                msg_type, client_id, message = control.deserialize(body)
                self.handle_control(msg_type, client_id, message, False)
            else:
                msg_type, client_id, origin_node, msg_seq, article = internal.deserialize(body)
                if msg_type != InternalType.ARTICLE:
                    raise ValueError(f"tipo interno inesperado: {msg_type}")

                if self.deduplicate.is_duplicate(client_id, origin_node, msg_seq):
                    with self._lock:
                        self._pending_acks.append(ack)
                        should_flush = len(self._pending_acks) >= self.persist_batch_size
                    if should_flush:
                        self._flush_data_batch()
                    return

                self.handle_article(client_id, article)

            with self._lock:
                self._pending_acks.append(ack)
                should_flush = (
                    is_control
                    or self._meta.is_closed(client_id)
                    or len(self._pending_acks) >= self.persist_batch_size
                )
            if should_flush:
                self._flush_data_batch()
        except Exception:
            nack()

    def _on_control(self, body, ack, nack):
        if self.store is None:
            super()._on_control(body, ack, nack)
            return

        try:
            msg_type, client_id, message = control.deserialize(body)
            self.handle_control(msg_type, client_id, message, True)
            with self._lock:
                self._persist_dirty_locked()
                if not self._meta.is_closed(client_id):
                    self._persist_client_locked(client_id)
                self._schedule_drain_acks_from_other_thread_locked()
            ack()
        except Exception:
            nack()

    def handle_article(self, client_id, article):
        with self._lock:
            if self._meta.is_closed(client_id) or self.group.is_closed(client_id):
                return

            self._seen[client_id] += 1

            # Sin fecha válida no se puede clasificar en <=2020 o >=2021.
            year = article.year
            if year is None:
                if self.store is not None:
                    self._dirty_clients.add(client_id)
                self.group.on_processed(client_id)
                return

            word_count = self._count_words(article.abstract)

            # Abstract vacío o solo stopwords/números no aporta al promedio ni es candidato.
            if word_count > 0:
                if year <= 2020:
                    bucket = article.docId % len(self.sum_queues)
                    msg_seq = self.next_msg_seq(client_id)
                    self.sum_queues[bucket].send(
                        internal.serialize_wordcount_sum(
                            client_id,
                            self.origin_node,
                            msg_seq,
                            article.articleId,
                            word_count,
                        )
                    )
                    self._counts[(client_id, "sum", bucket)] += 1
                else:
                    msg_seq = self.next_msg_seq(client_id)
                    self.out_cand_mw.send(
                        internal.serialize_wordcount_cand(
                            client_id,
                            self.origin_node,
                            msg_seq,
                            article.articleId,
                            word_count,
                            article.docId,
                            article.url,
                        )
                    )
                    self._counts[(client_id, "cand")] += 1

            if self.store is not None:
                self._dirty_clients.add(client_id)
            self.group.on_processed(client_id)

            if self._seen[client_id] % PROGRESS_EVERY == 0:
                sum_total = sum(
                    self._counts[(client_id, "sum", i)]
                    for i in range(len(self.sum_queues))
                )
                logger.info(
                    "PROGRESS client=%s node=%s seen=%s sum=%s cand=%s",
                    client_id,
                    self.node_id,
                    self._seen[client_id],
                    sum_total,
                    self._counts[(client_id, "cand")],
                )

    def handle_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            if self._meta.is_closed(client_id):
                return
            if self.store is not None:
                self._dirty_clients.add(client_id)
            self.group.on_control(msg_type, client_id, message, from_control)

    def _send_done(self, client_id, _quantity):
        sum_qtys = [
            self._counts[(client_id, "sum", index)]
            for index in range(len(self.sum_queues))
        ]
        cand_qty = self._counts[(client_id, "cand")]

        logger.info(
            "DONE client=%s node=%s sum=%s cand=%s",
            client_id,
            self.node_id,
            sum_qtys,
            cand_qty,
        )

        # Un DONE por particion Q5_Sum_i con la cantidad enviada a esa particion.
        for index, queue in enumerate(self.sum_queues):
            msg_seq = self.next_msg_seq(client_id)
            queue.send(
                control.serialize_done(
                    client_id,
                    self.origin_node,
                    msg_seq,
                    CTRL_Q5_COUNTER,
                    self.node_id,
                    sum_qtys[index],
                )
            )

        # Un DONE para Q5_Cand
        cand_seq = self.next_msg_seq(client_id)
        self.out_cand_mw.send(
            control.serialize_done(
                client_id,
                self.origin_node,
                cand_seq,
                CTRL_Q5_COUNTER,
                self.node_id,
                cand_qty,
            )
        )

        self._cleanup_client(client_id)

    def _cleanup_client(self, client_id):
        self._dirty_clients.discard(client_id)
        self._seen.pop(client_id, None)
        self._counts.pop((client_id, "cand"), None)
        for index in range(len(self.sum_queues)):
            self._counts.pop((client_id, "sum", index), None)
        self.deduplicate.clear_client(client_id)
        self._meta.mark_client_closed(client_id)

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
        for queue in [self.out_cand_mw, *self._own_queues]:
            try:
                queue.close()
            except Exception:
                pass
        if self.store is not None and self._control_thread is None:
            self.store.close()
