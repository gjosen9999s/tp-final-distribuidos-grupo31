"""Serialización del plano de control (EOF/PROGRESS/DONE/FLUSH)."""

from ..constants import CTRL_DONE, CTRL_EOF, CTRL_FLUSH, CTRL_PROGRESS, ControlType
from ..external.serializer import (
    Reader,
    serialize_uint8,
    serialize_uint16,
    serialize_uint32,
)


def serialize_eof(client_id, total):
    return (
        serialize_uint8(CTRL_EOF)
        + serialize_uint32(client_id)
        + serialize_uint32(total)
    )


def serialize_progress(client_id, controller_type, node_id, quantity):
    return (
        serialize_uint8(CTRL_PROGRESS)
        + serialize_uint32(client_id)
        + serialize_uint8(controller_type)
        + serialize_uint16(node_id)
        + serialize_uint32(quantity)
    )


def serialize_done(client_id, controller_type, node_id, quantity):
    return (
        serialize_uint8(CTRL_DONE)
        + serialize_uint32(client_id)
        + serialize_uint8(controller_type)
        + serialize_uint16(node_id)
        + serialize_uint32(quantity)
    )


def serialize_flush(client_id, flush_id):
    return (
        serialize_uint8(CTRL_FLUSH)
        + serialize_uint32(client_id)
        + serialize_uint32(flush_id)
    )


def deserialize(payload):
    reader = Reader(payload)
    msg_type = reader.uint8()
    client_id = reader.uint32()
    if msg_type == ControlType.EOF:
        return ControlType.EOF, client_id, {"total": reader.uint32()}
    if msg_type in (ControlType.PROGRESS, ControlType.DONE):
        return (
            ControlType.PROGRESS
            if msg_type == ControlType.PROGRESS
            else ControlType.DONE,
            client_id,
            {
                "controllerType": reader.uint8(),
                "nodeId": reader.uint16(),
                "quantity": reader.uint32(),
            },
        )
    if msg_type == ControlType.FLUSH:
        return ControlType.FLUSH, client_id, {"flushId": reader.uint32()}
    raise ValueError(f"tipo de control desconocido: {msg_type}")
