"""Controller base: consumidores, dispatch y ciclo de vida."""

import threading
from abc import ABC, abstractmethod

from common.protocol.constants import CTRL_EOF, InternalType
from common.protocol.control import serializer as control
from common.protocol.internal import serializer as internal


class Controller(ABC):
    def __init__(
        self,
        controller_type,
        in_mw,
        out_mw,
        control_in,
        control_out,
        node_id,
        progress_batch_size,
        controller_count,
    ):
        if control_in is None or control_out is None:
            raise ValueError("El controller necesita control_in y control_out")

        self.controller_type = controller_type
        self.in_mw = in_mw
        self.out_mw = out_mw
        self.control_in = control_in
        self.control_out = control_out
        self.node_id = node_id
        self.progress_batch_size = max(1, progress_batch_size)
        self.controller_count = max(1, controller_count)
        self._lock = threading.Lock()
        self._states = {}
        self._stopping = threading.Event()

    def run(self):
        control_thread = threading.Thread(target=self._consume_control, daemon=True)
        control_thread.start()
        try:
            self.in_mw.start_consuming(self._on_data)
        except Exception:
            if not self._stopping.is_set():
                raise

    def stop(self):
        self._stopping.set()
        try:
            self.in_mw.stop_consuming_threadsafe()
        except Exception:
            pass
        try:
            self.control_in.stop_consuming_threadsafe()
        except Exception:
            pass

    def _consume_control(self):
        try:
            self.control_in.start_consuming(self._on_control)
        except Exception:
            if not self._stopping.is_set():
                raise

    def _on_data(self, body, ack, nack):
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                self.handle_control(msg_type, client_id, message, False)
            else:
                msg_type, client_id, article = internal.deserialize(body)
                if msg_type != InternalType.ARTICLE:
                    raise ValueError(f"tipo interno inesperado: {msg_type}")
                self.handle_article(client_id, article)
            ack()
        except Exception:
            nack()

    def _on_control(self, body, ack, nack):
        try:
            msg_type, client_id, message = control.deserialize(body)
            self.handle_control(msg_type, client_id, message, True)
            ack()
        except Exception:
            nack()

    @abstractmethod
    def handle_article(self, client_id, article):
        ...

    @abstractmethod
    def handle_control(self, msg_type, client_id, message, from_control):
        ...
