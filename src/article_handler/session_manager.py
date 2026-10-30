"""Valida y etiqueta la sesión (clientId)."""
import socket
import threading
import logging
from typing import Dict, Optional, List
from common.protocol.external import framing

logger = logging.getLogger("SessionManager")


class ClientSession:
    """Representa la sesión activa de un cliente conectado vía TCP."""

    def __init__(self, client_id: int, sock: socket.socket, addr, expected_dones: int = 3):
        self.client_id = client_id
        self.sock = sock
        self.addr = addr
        self.expected_dones = expected_dones
        self.dones_count = 0
        self._lock = threading.Lock()
        self._eof_reached = False
        self._buffer: List[bytes] = []

    def send_ack(self) -> None:
        """Envía un ACK al cliente de forma thread-safe."""
        with self._lock:
            framing.send_ack(self.sock)

    def set_eof_reached(self) -> None:
        """
        Marca que el cliente finalizó la transmisión de lotes y 
        despacha los resultados acumulados previamente.
        """
        with self._lock:
            self._eof_reached = True
            for payload in self._buffer:
                framing.send_message(self.sock, payload)
            self._buffer.clear()

    def send_result_or_buffer(self, payload: bytes) -> None:
        """
        Si el cliente ya envió EOF, transmite el resultado por el socket.
        Si el cliente aún envía batches, lo encola para evitar colisionar con los ACKs.
        """
        with self._lock:
            if self._eof_reached:
                framing.send_message(self.sock, payload)
            else:
                self._buffer.append(payload)

    def is_eof_reached(self) -> bool:
        with self._lock:
            return self._eof_reached

    def register_done(self) -> bool:
        """
        Incrementa los mensajes DONE recibidos de las réplicas.
        Devuelve True únicamente cuando todas las réplicas esperadas han reportado DONE.
        """
        with self._lock:
            self.dones_count += 1
            return self.dones_count >= self.expected_dones


class SessionManager:
    """Administra las sesiones TCP concurrentes asignando identificadores únicos (clientId)."""

    def __init__(self, expected_dones: int = 3):
        self._next_id = 1
        self.expected_dones = expected_dones
        self._lock = threading.Lock()
        self._sessions: Dict[int, ClientSession] = {}

    def register_session(self, sock: socket.socket, addr) -> ClientSession:
        with self._lock:
            client_id = self._next_id
            self._next_id += 1
            session = ClientSession(client_id, sock, addr, expected_dones=self.expected_dones)
            self._sessions[client_id] = session
            return session

    def unregister_session(self, client_id: int) -> None:
        with self._lock:
            session = self._sessions.pop(client_id, None)
            if session:
                try:
                    session.sock.close()
                except Exception:
                    pass

    def get_session(self, client_id: int) -> Optional[ClientSession]:
        with self._lock:
            return self._sessions.get(client_id)

    def close_all(self) -> None:
        with self._lock:
            for session in self._sessions.values():
                try:
                    session.sock.close()
                except Exception:
                    pass
            self._sessions.clear()