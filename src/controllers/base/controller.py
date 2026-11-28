"""Controller base: consumidores, dispatch y ciclo de vida."""
import os
import threading
from abc import ABC, abstractmethod
from collections import Counter

from common.protocol.constants import CTRL_EOF, InternalType
from common.protocol.control import serializer as control
from common.deduplicate import DeduplicateWindow
from common.protocol.internal import serializer as internal

ORIGIN_BASE = {
    "q1_controller": 10,
    "q2_filter": 20,
    "tokenizer": 30,
    "q3_reduce": 40,
    "q3_join": 50,
    "q4_counter": 60,
    "q4_join": 70,
    "q5_counter": 80,
    "q5_accumulator": 90,
    "q5_average_n_filter": 100,
}

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
        self._control_thread = None

        self.origin_node = ORIGIN_BASE.get(controller_type, 0) + node_id
        self._msg_seq = Counter()
        self._msg_seq_lock = threading.Lock()

        self.deduplicate = DeduplicateWindow(window_size=int(os.getenv("DEDUPLICATE_WINDOW_SIZE", "1000")))

    def run(self):
        self._control_thread = threading.Thread(target=self._consume_control)
        self._control_thread.start()
        try:
            self.in_mw.start_consuming(self._on_data)
        except Exception:
            if not self._stopping.is_set():
                raise
        finally:
            self._stopping.set()
            self._stop_consumers()
            self._control_thread.join(timeout=5)

    def stop(self):
        self._stopping.set()
        self._stop_consumers()

    def next_msg_seq(self, client_id: int) -> int:
        with self._msg_seq_lock:
            self._msg_seq[client_id] += 1
            return self._msg_seq[client_id]
    
    def _stop_consumers(self):
        for middleware in (self.in_mw, self.control_in):
            try:
                middleware.stop_consuming_threadsafe()
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
                msg_type, client_id, origin_node, msg_seq, article = internal.deserialize(body)
                if msg_type != InternalType.ARTICLE:
                    raise ValueError(f"tipo interno inesperado: {msg_type}")

                # Control de duplicados (origen/sequencia), los mensajes duplicados se ignoran
                if self.deduplicate.is_duplicate(client_id, origin_node, msg_seq):
                    ack()
                    return
                
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
