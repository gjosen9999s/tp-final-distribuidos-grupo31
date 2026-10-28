"""Serializador binario propio (big-endian). 

Formato de cada tipo en el wire:
  uint8 / uint16 / uint32 : N bytes big-endian
  bool                    : 1 byte (0 / 1)
  string                  : [ uint32 largo ][ bytes UTF-8 ]
  bytes                   : [ uint32 largo ][ bytes crudos ]   -> imagen
  list                    : [ uint32 cantidad ][ item 1 ][ item 2 ] ...
"""

from ..constants import (
    BOOL_SIZE,
    EXT_BATCH,
    EXT_EOF,
    EXT_ERROR,
    EXT_RESULT,
    ExternalType,
    UINT8_SIZE,
    UINT16_SIZE,
    UINT32_SIZE,
)
from ...model.article import Article


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


def serialize_article(article):
    return (
        serialize_uint32(article.docId)
        + serialize_string(article.name)
        + serialize_string(article.url)
        + serialize_uint32(article.dateModified)
        + serialize_uint8(article.inLanguage)
        + serialize_uint8(article.sectionsFlags)
        + serialize_string(article.abstract)
        + serialize_string(article.imageUrl)
    )


def deserialize_article(reader):
    return Article(
        docId=reader.uint32(),
        name=reader.string(),
        url=reader.string(),
        dateModified=reader.uint32(),
        inLanguage=reader.uint8(),
        sectionsFlags=reader.uint8(),
        abstract=reader.string(),
        imageUrl=reader.string(),
    )


def serialize_batch(articles):
    return (
        serialize_uint8(EXT_BATCH)
        + serialize_uint32(len(articles))
        + b"".join(serialize_article(article) for article in articles)
    )


def deserialize_batch(reader):
    count = reader.uint32()
    return [deserialize_article(reader) for _ in range(count)]


def serialize_eof(total_articles, total_batches):
    return (
        serialize_uint8(EXT_EOF)
        + serialize_uint32(total_articles)
        + serialize_uint32(total_batches)
    )


def deserialize_eof(reader):
    return {
        "totalArticles": reader.uint32(),
        "totalBatches": reader.uint32(),
    }


def serialize_result(result_type, doc_id, part_id, flags, payload):
    return (
        serialize_uint8(EXT_RESULT)
        + serialize_uint8(result_type)
        + serialize_uint32(doc_id)
        + serialize_uint32(part_id)
        + serialize_uint8(flags)
        + serialize_bytes(payload)
    )


def deserialize_result(reader):
    return {
        "resultType": reader.uint8(),
        "docId": reader.uint32(),
        "partId": reader.uint32(),
        "flags": reader.uint8(),
        "payload": reader.bytes(),
    }


def serialize_error(code, message):
    return (
        serialize_uint8(EXT_ERROR)
        + serialize_uint32(code)
        + serialize_string(message)
    )


def deserialize_error(reader):
    return {"code": reader.uint32(), "message": reader.string()}


def deserialize_message(payload):
    reader = Reader(payload)
    msg_type = reader.uint8()
    if msg_type == ExternalType.BATCH:
        return ExternalType.BATCH, deserialize_batch(reader)
    if msg_type == ExternalType.EOF:
        return ExternalType.EOF, deserialize_eof(reader)
    if msg_type == ExternalType.RESULT:
        return ExternalType.RESULT, deserialize_result(reader)
    if msg_type == ExternalType.ERROR:
        return ExternalType.ERROR, deserialize_error(reader)
    raise ValueError(f"tipo externo desconocido: {msg_type}")


def serialize_q1_payload(url, date_modified):
    return serialize_string(url) + serialize_uint32(date_modified)


def deserialize_q1_payload(payload):
    reader = Reader(payload)
    return {"url": reader.string(), "dateModified": reader.uint32()}
