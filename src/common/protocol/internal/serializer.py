"""Serialización del plano de datos interno (AMQP)."""

from ...model.article import Article
from ..constants import (
    ART_ABSTRACT,
    ART_DATE_MODIFIED,
    ART_DOCID,
    ART_IMAGE_URL,
    ART_IN_LANGUAGE,
    ART_NAME,
    ART_SECTIONS_FLAGS,
    ART_URL,
    INT_ARTICLE,
    INT_RESULT,
)
from ..external.serializer import (
    Reader,
    serialize_bytes,
    serialize_uint8,
    serialize_uint32,
    serialize_string,
)


def serialize_article(article, mask):
    out = bytearray(serialize_uint8(mask))
    if mask & ART_DOCID:
        out += serialize_uint32(article.docId)
    if mask & ART_NAME:
        out += serialize_string(article.name)
    if mask & ART_URL:
        out += serialize_string(article.url)
    if mask & ART_DATE_MODIFIED:
        out += serialize_uint32(article.dateModified)
    if mask & ART_IN_LANGUAGE:
        out += serialize_uint8(article.inLanguage)
    if mask & ART_SECTIONS_FLAGS:
        out += serialize_uint8(article.sectionsFlags)
    if mask & ART_ABSTRACT:
        out += serialize_string(article.abstract)
    if mask & ART_IMAGE_URL:
        out += serialize_string(article.imageUrl)
    return bytes(out)


def deserialize_article(reader):
    mask = reader.uint8()
    values = {}
    if mask & ART_DOCID:
        values["docId"] = reader.uint32()
    if mask & ART_NAME:
        values["name"] = reader.string()
    if mask & ART_URL:
        values["url"] = reader.string()
    if mask & ART_DATE_MODIFIED:
        values["dateModified"] = reader.uint32()
    if mask & ART_IN_LANGUAGE:
        values["inLanguage"] = reader.uint8()
    if mask & ART_SECTIONS_FLAGS:
        values["sectionsFlags"] = reader.uint8()
    if mask & ART_ABSTRACT:
        values["abstract"] = reader.string()
    if mask & ART_IMAGE_URL:
        values["imageUrl"] = reader.string()
    return Article(**values)


def serialize_article_message(client_id, article, mask):
    return (
        serialize_uint8(INT_ARTICLE)
        + serialize_uint32(client_id)
        + serialize_article(article, mask)
    )


def serialize_result_message(client_id, result_type, doc_id, part_id, flags, payload):
    return (
        serialize_uint8(INT_RESULT)
        + serialize_uint32(client_id)
        + serialize_uint8(result_type)
        + serialize_uint32(doc_id)
        + serialize_uint32(part_id)
        + serialize_uint8(flags)
        + serialize_bytes(payload)
    )


def deserialize(payload):
    reader = Reader(payload)
    msg_type = reader.uint8()
    client_id = reader.uint32()
    if msg_type == INT_ARTICLE:
        return "article", client_id, deserialize_article(reader)
    if msg_type == INT_RESULT:
        return (
            "result",
            client_id,
            {
                "resultType": reader.uint8(),
                "docId": reader.uint32(),
                "partId": reader.uint32(),
                "flags": reader.uint8(),
                "payload": reader.bytes(),
            },
        )
    raise ValueError(f"tipo interno desconocido: {msg_type}")
