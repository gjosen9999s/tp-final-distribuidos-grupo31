"""Subsistema de persistencia local agnóstica del dominio."""

from .state_store import (
    CorruptedSnapshotError,
    CorruptedWALError,
    StateStore,
    StateStoreError,
)

__all__ = [
    "CorruptedSnapshotError",
    "CorruptedWALError",
    "StateStore",
    "StateStoreError",
]
