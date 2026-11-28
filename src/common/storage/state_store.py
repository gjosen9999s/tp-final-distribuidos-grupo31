"""Persistencia local híbrida (WAL + Snapshot atómico) agnóstica del dominio.

Formato binario de cada frame (tanto en snapshot.bin como en wal.bin):
    [4B uint32 length][4B uint32 crc32][length bytes pickle payload]
"""

from __future__ import annotations

import os
import pickle
import shutil
import struct
import threading
import zlib
from pathlib import Path
from typing import Any, BinaryIO

_FRAME_HEADER = struct.Struct("!II")
_FRAME_HEADER_SIZE = _FRAME_HEADER.size  # 8 bytes: 4B length + 4B crc32
_FORMAT_VERSION = 1

_SNAPSHOT_FILE = "snapshot.bin"
_SNAPSHOT_TMP = "snapshot.tmp"
_WAL_FILE = "wal.bin"
_WAL_TMP = "wal.tmp"


class StateStoreError(Exception):
    """Error base del subsistema de persistencia."""


class CorruptedWALError(StateStoreError):
    """Corrupción detectada en un registro intermedio ya confirmado del WAL."""


class CorruptedSnapshotError(StateStoreError):
    """Corrupción detectada en el archivo consolidado snapshot.bin."""


def _pack_frame(envelope: dict[str, Any]) -> bytes:
    """Serializa un diccionario envelope con encabezado [length][crc32]."""
    payload = pickle.dumps(envelope, protocol=pickle.HIGHEST_PROTOCOL)
    crc = zlib.crc32(payload) & 0xFFFFFFFF
    return _FRAME_HEADER.pack(len(payload), crc) + payload


class StateStore:
    """Almacén de estado local durable mediante WAL secuencial + Snapshot atómico."""

    def __init__(
        self,
        base_dir: str | Path,
        node_name: str,
        compact_every_batches: int = 50,
    ) -> None:
        self._validate_name(node_name, "node_name")
        if compact_every_batches < 1:
            raise ValueError("compact_every_batches debe ser >= 1")

        self.base_dir = Path(base_dir)
        self.node_name = node_name
        self.compact_every_batches = compact_every_batches
        self._node_dir = self.base_dir / self.node_name

        self._meta_lock = threading.Lock()
        self._scope_locks: dict[str, threading.RLock] = {}
        self._wal_files: dict[str, BinaryIO] = {}
        self._last_cid: dict[str, int] = {}
        self._batches_since_compact: dict[str, int] = {}
        self._initialized_scopes: set[str] = set()
        self._closed = False

    def __enter__(self) -> StateStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _validate_name(value: str, label: str) -> None:
        if (
            not isinstance(value, str)
            or not value
            or value in (".", "..")
            or "/" in value
            or "\\" in value
            or "\x00" in value
        ):
            raise ValueError(f"{label} inválido: {value!r}")

    def _scope_dir(self, scope: str) -> Path:
        return self._node_dir / scope

    def _get_scope_lock(self, scope: str) -> threading.RLock:
        with self._meta_lock:
            lock = self._scope_locks.get(scope)
            if lock is None:
                lock = threading.RLock()
                self._scope_locks[scope] = lock
            return lock

    def _check_open(self) -> None:
        if self._closed:
            raise StateStoreError("StateStore ya se encuentra cerrado")

    def _ensure_initialized(self, scope: str) -> None:
        """Si ``scope`` ya existía en disco y aún no se cargó, ejecuta ``load`` antes de escribir."""
        if scope in self._initialized_scopes:
            return
        if self._scope_dir(scope).exists():
            self.load(scope)
        else:
            self._last_cid[scope] = 0
            self._batches_since_compact[scope] = 0
            self._initialized_scopes.add(scope)

    @staticmethod
    def _fsync_dir(dir_path: Path) -> None:
        dir_fd = os.open(dir_path, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def _ensure_scope_dir(self, scope: str) -> Path:
        scope_dir = self._scope_dir(scope)
        if not scope_dir.exists():
            parent_existed = self._node_dir.exists()
            scope_dir.mkdir(parents=True, exist_ok=True)
            if not parent_existed and self.base_dir.exists():
                self._fsync_dir(self.base_dir)
            self._fsync_dir(self._node_dir)
        return scope_dir

    def _get_or_open_wal(self, scope: str) -> BinaryIO:
        with self._meta_lock:
            fh = self._wal_files.get(scope)
            if fh is not None and not fh.closed:
                return fh

        scope_dir = self._ensure_scope_dir(scope)
        wal_path = scope_dir / _WAL_FILE
        existed = wal_path.exists()
        fh = open(wal_path, "ab")
        if not existed:
            self._fsync_dir(scope_dir)

        with self._meta_lock:
            self._wal_files[scope] = fh
        return fh

    def append_wal(self, scope: str, delta: Any) -> int:
        """Agrega un delta al WAL de ``scope`` con ``fsync`` y retorna su ``cid``."""
        self._validate_name(scope, "scope")
        with self._get_scope_lock(scope):
            self._check_open()
            self._ensure_initialized(scope)

            cid = self._last_cid[scope] + 1
            frame = _pack_frame({"v": _FORMAT_VERSION, "cid": cid, "delta": delta})

            fh = self._get_or_open_wal(scope)
            fh.write(frame)
            fh.flush()
            os.fsync(fh.fileno())

            self._last_cid[scope] = cid
            self._batches_since_compact[scope] = (
                self._batches_since_compact.get(scope, 0) + 1
            )
            return cid

    def _close_wal_handle(self, scope: str) -> None:
        with self._meta_lock:
            fh = self._wal_files.pop(scope, None)
        if fh is not None:
            try:
                fh.flush()
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass

    @staticmethod
    def _write_and_fsync_file(path: Path, data: bytes) -> None:
        with open(path, "wb") as fh:
            if data:
                fh.write(data)
                fh.flush()
            os.fsync(fh.fileno())

    def should_compact(self, scope: str) -> bool:
        """Indica si ``scope`` alcanzó el umbral de lotes para compactar."""
        self._validate_name(scope, "scope")
        with self._get_scope_lock(scope):
            self._check_open()
            return self._batches_since_compact.get(scope, 0) >= self.compact_every_batches

    def save_snapshot(
        self,
        scope: str,
        full_state: Any,
        checkpoint_id: int | None = None,
    ) -> int:
        """Guarda atómicamente ``snapshot.bin`` y vacía ``wal.bin``."""
        self._validate_name(scope, "scope")
        with self._get_scope_lock(scope):
            self._check_open()
            self._ensure_initialized(scope)

            current_cid = self._last_cid[scope]
            if checkpoint_id is None:
                cid = current_cid + 1
            else:
                if checkpoint_id < current_cid:
                    raise ValueError(
                        f"checkpoint_id regresivo ({checkpoint_id} < {current_cid})"
                    )
                cid = checkpoint_id

            scope_dir = self._ensure_scope_dir(scope)
            tmp_snapshot = scope_dir / _SNAPSHOT_TMP
            final_snapshot = scope_dir / _SNAPSHOT_FILE

            # escribir snapshot.tmp + fsync -> os.replace -> fsync(dir)
            frame = _pack_frame({"v": _FORMAT_VERSION, "cid": cid, "state": full_state})
            self._write_and_fsync_file(tmp_snapshot, frame)
            # esta parte es crucial, replace es atomico, el cambio de nombre se hace
            # pero el inodo se reapunta en RAM, necesitamos fsync del directorio
            # para poder garantizar que se apunta al nuevo archivo checkpoint.bin
            os.replace(tmp_snapshot, final_snapshot)
            self._fsync_dir(scope_dir)

            # cerrar descriptor viejo y reemplazar wal.bin por uno vacío
            self._close_wal_handle(scope)
            wal_path = scope_dir / _WAL_FILE
            if wal_path.exists():
                tmp_wal = scope_dir / _WAL_TMP
                self._write_and_fsync_file(tmp_wal, b"")
                os.replace(tmp_wal, wal_path)
                self._fsync_dir(scope_dir)

            self._last_cid[scope] = cid
            self._batches_since_compact[scope] = 0
            return cid

    @classmethod
    def _read_snapshot(cls, scope_dir: Path) -> tuple[Any | None, int]:
        final_path = scope_dir / _SNAPSHOT_FILE
        if not final_path.exists():
            return None, 0

        raw = final_path.read_bytes()
        if len(raw) < _FRAME_HEADER_SIZE:
            raise CorruptedSnapshotError(
                f"snapshot.bin truncado ({len(raw)} bytes < {_FRAME_HEADER_SIZE})"
            )

        length, expected_crc = _FRAME_HEADER.unpack(raw[:_FRAME_HEADER_SIZE])
        if length == 0 or _FRAME_HEADER_SIZE + length != len(raw):
            raise CorruptedSnapshotError(
                f"Longitud inválida en snapshot.bin: esperada={_FRAME_HEADER_SIZE + length}, real={len(raw)}"
            )

        payload = raw[_FRAME_HEADER_SIZE:]
        actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if actual_crc != expected_crc:
            raise CorruptedSnapshotError("CRC32 inválido en snapshot.bin")

        try:
            envelope = pickle.loads(payload)
        except Exception as exc:
            raise CorruptedSnapshotError("Payload corrupto en snapshot.bin") from exc

        if (
            not isinstance(envelope, dict)
            or envelope.get("v") != _FORMAT_VERSION
            or not isinstance(envelope.get("cid"), int)
            or "state" not in envelope
        ):
            raise CorruptedSnapshotError("Estructura inválida en snapshot.bin")

        return envelope["state"], envelope["cid"]

    @staticmethod
    def _truncate_wal(fh: BinaryIO, valid_offset: int) -> None:
        fh.truncate(valid_offset)
        fh.flush()
        os.fsync(fh.fileno())

    @classmethod
    def _read_wal_frames(cls, wal_path: Path) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        valid_offset = 0

        with open(wal_path, "r+b") as fh:
            file_size = os.fstat(fh.fileno()).st_size

            while valid_offset < file_size:
                fh.seek(valid_offset)
                header = fh.read(_FRAME_HEADER_SIZE)

                if len(header) < _FRAME_HEADER_SIZE:
                    cls._truncate_wal(fh, valid_offset)
                    break

                length, expected_crc = _FRAME_HEADER.unpack(header)
                if length == 0:
                    raise CorruptedWALError(
                        f"Frame de longitud 0 en WAL (offset={valid_offset})"
                    )

                payload = fh.read(length)
                frame_end = valid_offset + _FRAME_HEADER_SIZE + length

                if len(payload) < length:
                    cls._truncate_wal(fh, valid_offset)
                    break

                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    if frame_end < file_size:
                        raise CorruptedWALError(
                            f"CRC32 inválido en registro intermedio del WAL (offset={valid_offset})"
                        )
                    cls._truncate_wal(fh, valid_offset)
                    break

                try:
                    envelope = pickle.loads(payload)
                except Exception as exc:
                    if frame_end < file_size:
                        raise CorruptedWALError(
                            f"Payload corrupto en registro intermedio del WAL (offset={valid_offset})"
                        ) from exc
                    cls._truncate_wal(fh, valid_offset)
                    break

                if (
                    not isinstance(envelope, dict)
                    or envelope.get("v") != _FORMAT_VERSION
                    or not isinstance(envelope.get("cid"), int)
                    or "delta" not in envelope
                ):
                    if frame_end < file_size:
                        raise CorruptedWALError(
                            f"Estructura inválida en registro intermedio del WAL (offset={valid_offset})"
                        )
                    cls._truncate_wal(fh, valid_offset)
                    break

                entries.append(envelope)
                valid_offset = frame_end

        return entries

    def load(self, scope: str) -> tuple[Any | None, list[Any], int]:
        """Recupera ``(snapshot_state, pending_deltas, max_cid)`` de ``scope``."""
        self._validate_name(scope, "scope")
        with self._get_scope_lock(scope):
            self._check_open()
            self._close_wal_handle(scope)

            scope_dir = self._scope_dir(scope)
            if not scope_dir.exists():
                self._last_cid[scope] = 0
                self._batches_since_compact[scope] = 0
                self._initialized_scopes.add(scope)
                return None, [], 0

            # borrar archivos .tmp huérfanos de caídas previas
            removed_tmp = False
            for tmp_name in (_SNAPSHOT_TMP, _WAL_TMP):
                tmp_path = scope_dir / tmp_name
                if tmp_path.exists():
                    tmp_path.unlink()
                    removed_tmp = True
            if removed_tmp:
                self._fsync_dir(scope_dir)

            # lsnapshot consolidado (si existe)
            snapshot_state, snapshot_cid = self._read_snapshot(scope_dir)

            # leer WAL truncando final roto e ignorando cid <= snapshot_cid
            pending_deltas: list[Any] = []
            max_cid = snapshot_cid
            wal_path = scope_dir / _WAL_FILE
            if wal_path.exists():
                for entry in self._read_wal_frames(wal_path):
                    cid = entry["cid"]
                    if cid > snapshot_cid:
                        pending_deltas.append(entry["delta"])
                        if cid > max_cid:
                            max_cid = cid

            self._last_cid[scope] = max_cid
            self._batches_since_compact[scope] = len(pending_deltas)
            self._initialized_scopes.add(scope)
            return snapshot_state, pending_deltas, max_cid

    def list_scopes(self) -> list[str]:
        """Lista todos los scopes existentes en disco para este nodo."""
        self._check_open()
        if not self._node_dir.exists():
            return []
        return sorted(
            entry.name
            for entry in self._node_dir.iterdir()
            if entry.is_dir()
        )

    def clear(self, scope: str) -> None:
        """Elimina por completo la carpeta en disco y el estado interno de ``scope``."""
        self._validate_name(scope, "scope")
        with self._get_scope_lock(scope):
            self._check_open()
            self._close_wal_handle(scope)
            self._last_cid.pop(scope, None)
            self._batches_since_compact.pop(scope, None)
            self._initialized_scopes.discard(scope)

            scope_dir = self._scope_dir(scope)
            if scope_dir.exists():
                shutil.rmtree(scope_dir)
                if self._node_dir.exists():
                    self._fsync_dir(self._node_dir)

    def close(self) -> None:
        """Espera a que drenen las operaciones en vuelo y cierra todos los archivos."""
        with self._meta_lock:
            if self._closed:
                return
            self._closed = True
            locks = list(self._scope_locks.values())

        for lock in locks:
            with lock:
                pass

        with self._meta_lock:
            handles = list(self._wal_files.values())
            self._wal_files.clear()

        for fh in handles:
            try:
                fh.flush()
            except Exception:
                pass
            try:
                fh.close()
            except Exception:
                pass

