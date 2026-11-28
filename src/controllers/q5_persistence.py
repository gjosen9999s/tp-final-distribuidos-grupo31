"""Helpers de integración con StateStore para los controllers de Q5."""

from __future__ import annotations

import os
import threading
from typing import Any

from common.config import env
from common.storage import StateStore

GLOBAL_SCOPE = "global"
SEQ_RESERVE_STEP = 200


def client_scope(client_id: int) -> str:
    return f"client_{client_id}"


def parse_client_scope(scope: str) -> int | None:
    if not scope.startswith("client_"):
        return None
    raw_id = scope.split("_", 1)[1]
    return int(raw_id) if raw_id.isdigit() else None


def build_state_store(
    controller_type: str,
    node_id: int,
    state_store: StateStore | None = None,
) -> StateStore | None:
    """Construye ``StateStore`` si se inyectó uno o si ``STATE_DIR`` está definido."""
    if state_store is not None:
        return state_store
    raw_dir = os.getenv("STATE_DIR")
    if not raw_dir:
        return None
    return StateStore(
        base_dir=raw_dir,
        node_name=f"{controller_type}_{node_id}",
        compact_every_batches=env.state_compact_every_batches(),
    )


def export_client_dedup(deduplicate, client_id: int) -> dict[int, tuple[int, set[int]]]:
    """Extrae una copia serializable del estado de DeduplicateWindow de un cliente."""
    with deduplicate.lock:
        return {
            origin: (st["highest"], set(st["seen"]))
            for (cid, origin), st in deduplicate.states.items()
            if cid == client_id
        }


def restore_client_dedup(
    deduplicate,
    client_id: int,
    data: dict[int, tuple[int, set[int]]] | None,
) -> None:
    """Restaura en DeduplicateWindow el estado persistido de un cliente."""
    if not data:
        return
    with deduplicate.lock:
        for origin, (highest, seen) in data.items():
            deduplicate.states[(client_id, origin)] = {
                "highest": highest,
                "seen": set(seen),
            }


class NodeMetaTracker:
    """Gestiona en ``scope='global'`` los clientes cerrados y reservas de ``msg_seq``."""

    def __init__(self, store: StateStore | None) -> None:
        self.store = store
        self._lock = threading.Lock()
        self.closed_clients: set[int] = set()
        self.reserved_seqs: dict[int, int] = {}

        if self.store is not None:
            self._load()

    def _load(self) -> None:
        assert self.store is not None
        snap, deltas, _ = self.store.load(GLOBAL_SCOPE)
        if snap is not None:
            self.closed_clients = set(snap.get("closed_clients", ()))
            self.reserved_seqs = dict(snap.get("reserved_seqs", {}))

        for delta in deltas:
            if "closed_client" in delta:
                cid = delta["closed_client"]
                self.closed_clients.add(cid)
                self.reserved_seqs.pop(cid, None)
            if "reserved_seq" in delta:
                cid, reserved = delta["reserved_seq"]
                if cid not in self.closed_clients and reserved > self.reserved_seqs.get(cid, 0):
                    self.reserved_seqs[cid] = reserved

    def _compact_if_needed_locked(self, cid: int) -> None:
        assert self.store is not None
        if self.store.should_compact(GLOBAL_SCOPE):
            self.store.save_snapshot(
                GLOBAL_SCOPE,
                {
                    "closed_clients": set(self.closed_clients),
                    "reserved_seqs": dict(self.reserved_seqs),
                },
                checkpoint_id=cid,
            )

    def is_closed(self, client_id: int) -> bool:
        with self._lock:
            return client_id in self.closed_clients

    def ensure_seq_reserved(self, client_id: int, seq: int) -> None:
        if self.store is None:
            return
        with self._lock:
            if seq <= self.reserved_seqs.get(client_id, 0):
                return
            new_reserved = ((seq // SEQ_RESERVE_STEP) + 1) * SEQ_RESERVE_STEP
            cid = self.store.append_wal(
                GLOBAL_SCOPE,
                {"reserved_seq": (client_id, new_reserved)},
            )
            self.reserved_seqs[client_id] = new_reserved
            self._compact_if_needed_locked(cid)

    def mark_client_closed(self, client_id: int) -> None:
        with self._lock:
            self.closed_clients.add(client_id)
            self.reserved_seqs.pop(client_id, None)
            if self.store is not None:
                cid = self.store.append_wal(
                    GLOBAL_SCOPE,
                    {"closed_client": client_id},
                )
                self._compact_if_needed_locked(cid)

        if self.store is not None:
            self.store.clear(client_scope(client_id))
