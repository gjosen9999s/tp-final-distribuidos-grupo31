"""Serializador binario propio (big-endian). Sin librerías externas.

Formato de cada tipo en el wire:
  uint8 / uint16 / uint32 : N bytes big-endian
  bool                    : 1 byte (0 / 1)
  string                  : [ uint32 largo ][ bytes UTF-8 ]
  bytes                   : [ uint32 largo ][ bytes crudos ]   -> imagen
  list                    : [ uint32 cantidad ][ item 1 ][ item 2 ] ...
"""

UINT8_SIZE = 1
UINT16_SIZE = 2
UINT32_SIZE = 4
BOOL_SIZE = 1


def serialize_uint8(value):
    return int(value).to_bytes(UINT8_SIZE, "big")


def deserialize_uint8(data):
    return int.from_bytes(data, "big")


def serialize_uint16(value):
    return int(value).to_bytes(UINT16_SIZE, "big")


def deserialize_uint16(data):
    return int.from_bytes(data, "big")


def serialize_uint32(value):
    return int(value).to_bytes(UINT32_SIZE, "big")


def deserialize_uint32(data):
    return int.from_bytes(data, "big")


def serialize_bool(value):
    return (1 if value else 0).to_bytes(BOOL_SIZE, "big")


def deserialize_bool(data):
    return int.from_bytes(data, "big") != 0


def serialize_string(value):
    data = value.encode("utf-8")
    return serialize_uint32(len(data)) + data


def deserialize_string(data):
    return data.decode("utf-8")


def serialize_bytes(value):
    return serialize_uint32(len(value)) + value


def deserialize_bytes(data):
    return bytes(data)


class Reader:
    """Lee secuencialmente un buffer ya recibido (offset interno)."""

    def __init__(self, data):
        self._data = data
        self.pos = 0

    def read(self, size):
        chunk = self._data[self.pos:self.pos + size]
        if len(chunk) != size:
            raise ValueError("Reader: buffer insuficiente")
        self.pos += size
        return chunk

    def uint8(self):
        return deserialize_uint8(self.read(UINT8_SIZE))

    def uint16(self):
        return deserialize_uint16(self.read(UINT16_SIZE))

    def uint32(self):
        return deserialize_uint32(self.read(UINT32_SIZE))

    def bool(self):
        return deserialize_bool(self.read(BOOL_SIZE))

    def string(self):
        return deserialize_string(self.read(self.uint32()))

    def bytes(self):
        return deserialize_bytes(self.read(self.uint32()))

    def list(self, read_item):
        return [read_item(self) for _ in range(self.uint32())]


def serialize_list(items, serialize_item):
    return serialize_uint32(len(items)) + b"".join(serialize_item(i) for i in items)
