"""Safe socket + framing length-prefixed (sin librerías externas).

  mensaje : [ uint32 largo ][ payload ]

send_all / recv_all garantizan procesar TODOS los bytes (evitan short read/write).
"""

import socket

from . import serializer
from .serializer import UINT32_SIZE


def send_all(sock: socket.socket, data: bytes) -> None:
    total = 0
    while total < len(data):
        sent = sock.send(data[total:])
        if sent == 0:
            raise ConnectionError("send_all: socket cerrado")
        total += sent


def recv_all(sock: socket.socket, size: int) -> bytes:
    data = bytearray(size)
    pos = 0
    while pos < size:
        n = sock.recv_into(memoryview(data)[pos:])
        if n == 0:
            raise ConnectionError("recv_all: conexión cerrada antes del mensaje completo")
        pos += n
    return bytes(data)


def send_message(sock: socket.socket, payload: bytes) -> None:
    send_all(sock, serializer.serialize_uint32(len(payload)) + payload)


def recv_message(sock: socket.socket, max_frame=None) -> bytes:
    size = serializer.deserialize_uint32(recv_all(sock, UINT32_SIZE))
    if max_frame is not None and size > max_frame:
        raise ValueError(f"frame demasiado grande: {size}")
    return recv_all(sock, size)


def send_ack(sock: socket.socket) -> None:
    send_message(sock, b"")


def is_ack(payload: bytes) -> bool:
    return len(payload) == 0
