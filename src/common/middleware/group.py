"""Primitivas de coordinación de grupos (reduce, gather/broadcast, barrier, flush)."""

import threading
from dataclasses import dataclass

from common.protocol.constants import ControlType
from common.protocol.control import serializer as control


@dataclass
class _GroupState:
    total: int | None = None
    processed: int = 0
    last_reported: int = 0
    sum: int = 0
    emitted: int = 0
    flush_mode: bool = False
    flushed: bool = False
    closed: bool = False
    flush_id: int = 0


class GroupCoordinator:
    """Estado por clientId: PROGRESS/FLUSH, cierre por conteo y DONE."""

    def __init__(
        self,
        controller_type,
        node_id,
        progress_batch_size,
        controller_count,
        control_out,
        done_sender,
    ):
        self.controller_type = controller_type
        self.node_id = node_id
        self.progress_batch_size = max(1, progress_batch_size)
        self.controller_count = max(1, controller_count)
        self.control_out = control_out
        self.done_sender = done_sender
        self._states = {}
        self._lock = threading.RLock()

    def _state(self, client_id):
        state = self._states.get(client_id)
        if state is None:
            state = _GroupState()
            self._states[client_id] = state
        return state

    def is_closed(self, client_id):
        with self._lock:
            return self._state(client_id).closed

    def add_emitted(self, client_id, quantity=1):
        with self._lock:
            self._state(client_id).emitted += quantity

    def on_processed(self, client_id, quantity=1):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            state.processed += quantity
            delta = state.processed - state.last_reported
            if state.flush_mode:
                if delta > 0:
                    self._send_progress(client_id, delta)
                    state.last_reported = state.processed
            elif delta >= self.progress_batch_size:
                self._send_progress(client_id, delta)
                state.last_reported = state.processed

    def on_control(self, msg_type, client_id, message, from_control):
        with self._lock:
            state = self._state(client_id)
            if state.closed:
                return

            if msg_type == ControlType.EOF:
                self._on_eof(client_id, state, message["total"], from_control)
            elif msg_type == ControlType.PROGRESS:
                state.sum += message["quantity"]
                self._evaluate(client_id, state)
            elif msg_type == ControlType.FLUSH:
                self._on_flush(client_id, state)

    def _on_eof(self, client_id, state, total, from_control):
        state.total = total
        # El EOF llega in-band por una sola réplica de datos; desde acá
        # se republica al control para que lo vea toda la etapa.
        if not from_control:
            self.control_out.send(control.serialize_eof(client_id, total))
        self._evaluate(client_id, state)

    def _on_flush(self, client_id, state):
        delta = state.processed - state.last_reported
        if delta > 0:
            self._send_progress(client_id, delta)
            state.last_reported = state.processed
        state.flush_mode = True

    def _evaluate(self, client_id, state):
        if state.total is None or state.closed:
            return

        if not state.flushed and state.sum < state.total:
            threshold = state.total - self.progress_batch_size * self.controller_count
            if state.sum >= threshold:
                state.flush_id += 1
                self.control_out.send(control.serialize_flush(client_id, state.flush_id))
                state.flushed = True

        if state.sum == state.total:
            self.done_sender(client_id, state.emitted)
            state.closed = True

    def _send_progress(self, client_id, quantity):
        self.control_out.send(
            control.serialize_progress(
                client_id, self.controller_type, self.node_id, quantity
            )
        )
