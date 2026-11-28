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
    INT_Q3_PARTIAL,
    INT_Q4_PARTIAL,
    INT_Q5_ACC_PARTIAL,
    INT_RESULT,
    INT_WORD_Q3,
    INT_WORD_Q4,
    INT_WORDCOUNT_CAND,
    INT_WORDCOUNT_SUM,
    InternalType,
)
from ..external.serializer import (
    Reader,
    read_word_count,
    serialize_bytes,
    serialize_list,
    serialize_q4_payload,
    serialize_uint8,
    serialize_uint16,
    serialize_uint32,
    serialize_string,
    serialize_stringlist,
    deserialize_stringlist
)

def serialize_internal_header(origin_node: int, msg_seq: int) -> bytes:
    
    if origin_node < 0 or origin_node > 0xFFFF:
        raise ValueError("origin_node inválido")

    if msg_seq < 0 or msg_seq > 0xFFFFFFFF:
        raise ValueError("msg_seq inválido")

    return serialize_uint16(origin_node) + serialize_uint32(msg_seq)

def read_internal_header(reader: Reader) -> tuple[int, int]:

    origin_node = reader.uint16()
    msg_seq = reader.uint32()
    
    return origin_node, msg_seq


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


def serialize_article_message(client_id, origin_node, msg_seq, article_id, article, mask):
    return (
        serialize_uint8(INT_ARTICLE)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(article_id)
        + serialize_article(article, mask)
    )


def serialize_result_message(
    client_id,
    origin_node,
    msg_seq,
    article_id,
    result_type,
    doc_id,
    part_id,
    flags,
    payload,
):
    return (
        serialize_uint8(INT_RESULT)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(article_id)
        + serialize_uint8(result_type)
        + serialize_uint32(doc_id)
        + serialize_uint32(part_id)
        + serialize_uint8(flags)
        + serialize_bytes(payload)
    )


def serialize_wordcount_sum(client_id: int, origin_node, msg_seq, word_count: int) -> bytes:
    """WORDCOUNT_SUM: conteo de palabras únicas de un artículo ≤2020."""
    return (
        serialize_uint8(INT_WORDCOUNT_SUM)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(word_count)
    )


def serialize_wordcount_cand(
    client_id: int, origin_node, msg_seq, word_count: int, doc_id: int, url: str
) -> bytes:
    """WORDCOUNT_CAND: conteo + identificador de un artículo candidato ≥2021."""
    return (
        serialize_uint8(INT_WORDCOUNT_CAND)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(word_count)
        + serialize_uint32(doc_id)
        + serialize_string(url)
    )


def serialize_q5_acc_partial(
    client_id: int, origin_node, msg_seq, total_sum: int, count: int
) -> bytes:
    """Q5_ACC_PARTIAL: (sum, count) de articulos de antes del 2020 para el Q5_AVG_Filter."""
    return (
        serialize_uint8(INT_Q5_ACC_PARTIAL)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(total_sum)
        + serialize_uint32(count)
    )


def deserialize(payload):
    # Todo mensaje interno arranca con [1B tipo][4B clientId].
    reader = Reader(payload)
    msg_type = reader.uint8()
    client_id = reader.uint32()

    # [1B tipo=1][4B clientId][ [2B origin node][4B msg seq] ][1B mask][campos según mask]
    if msg_type == InternalType.ARTICLE:
        origin_node, msg_seq = read_internal_header(reader)
        article_id = reader.uint32()
        article = deserialize_article(reader)
        article.articleId = article_id

        return (
            InternalType.ARTICLE,
            client_id,
            origin_node,
            msg_seq,
            article,
        )

    # [1B tipo=10][4B clientId][2B originNode][4B msgSeq][4B articleId]
    # [1B resultType][4B docId][4B partId][1B flags][4B len payload][payload]
    if msg_type == InternalType.RESULT:
        origin_node, msg_seq = read_internal_header(reader)
        article_id = reader.uint32()

        return (
            InternalType.RESULT,
            client_id,
            origin_node,
            msg_seq,
            article_id,
            {
                "resultType": reader.uint8(),
                "docId": reader.uint32(),
                "partId": reader.uint32(),
                "flags": reader.uint8(),
                "payload": reader.bytes(),
            },
        )
    # [1B tipo=2][4B clientId][2B originNode][4B MsgSeq][4B len wordlist][word|word|...|word][1B inLanguage]
    if msg_type == InternalType.WORD_Q3:
        
        origin_node, msg_seq = read_internal_header(reader)

        return InternalType.WORD_Q3, client_id, origin_node, msg_seq, {
            "wordlist": deserialize_stringlist(reader.bytes()),
            "inLanguage": reader.uint8(),
        }

    # [1B type=3][4B clientId][2B originNode][4B MsgSeq][4B docId][4B len wordlist][word|word|...|word]
    if msg_type == InternalType.WORD_Q4:

        origin_node, msg_seq = read_internal_header(reader)
        
        return InternalType.WORD_Q4, client_id, origin_node, msg_seq, {
        "docId": reader.uint32(),
        "wordlist": deserialize_stringlist(reader.bytes()),
    }

    # [1B tipo=7][4B clientId][2B originNode][4B MsgSeq][4B len words][ [4B len word][word] ... ]
    if msg_type == InternalType.Q3_PARTIAL:
        origin_node, msg_seq = read_internal_header(reader)
        return (
            InternalType.Q3_PARTIAL,
            client_id,
            origin_node,
            msg_seq,
            {"words": reader.list(lambda current: current.string())},
        )
    
    # [1B tipo=8][4B clientId][2B originNode][4B MsgSeq][2B nodeId][4B len pairs][ [4B len word][word][4B count] ... ]
    if msg_type == InternalType.Q4_PARTIAL:
        origin_node, msg_seq = read_internal_header(reader)
        return (
            InternalType.Q4_PARTIAL,
            client_id,
            origin_node,
            msg_seq,
            {
                "nodeId": reader.uint16(),
                "pairs": reader.list(read_word_count),
            },
        )

    if msg_type == InternalType.WORDCOUNT_SUM:
        origin_node, msg_seq = read_internal_header(reader)
        return ( InternalType.WORDCOUNT_SUM, client_id, origin_node, msg_seq, {"wordCount": reader.uint32()} )

    if msg_type == InternalType.WORDCOUNT_CAND:
        origin_node, msg_seq = read_internal_header(reader)
        return (
            InternalType.WORDCOUNT_CAND,
            client_id, 
            origin_node,
            msg_seq,
            {
                "wordCount": reader.uint32(),
                "docId": reader.uint32(),
                "url": reader.string(),
            },
        )

    if msg_type == InternalType.Q5_ACC_PARTIAL:
        origin_node, msg_seq = read_internal_header(reader)
        return (
            InternalType.Q5_ACC_PARTIAL,
            client_id, 
            origin_node,
            msg_seq,
            {
                "totalSum": reader.uint32(),
                "count": reader.uint32(),
            },
        )
    raise ValueError(f"tipo interno desconocido: {msg_type}")

def serialize_word_q3_message(client_id, word, in_language):
    return (
        serialize_uint8(INT_WORD_Q3)
        + serialize_uint32(client_id)
        + serialize_string(word)
        + serialize_uint8(in_language)
    )

def serialize_wordlist_q3_message(client_id, origin_node, msg_seq, wordlist, in_language):
    return (
        serialize_uint8(INT_WORD_Q3)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_stringlist(wordlist)
        + serialize_uint8(in_language)
    )

def serialize_word_q4_message(client_id, doc_id, word):
    return (
        serialize_uint8(INT_WORD_Q4)
        + serialize_uint32(client_id)
        + serialize_uint32(doc_id)
        + serialize_string(word)
    )

def serialize_wordlist_q4_message(client_id, origin_node, msg_seq, doc_id, wordlist):
    return (
        serialize_uint8(INT_WORD_Q4)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint32(doc_id)
        + serialize_stringlist(wordlist)
    )


def serialize_q3_partial_message(client_id, origin_node, msg_seq, words):
    return (
        serialize_uint8(INT_Q3_PARTIAL)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_list(words, serialize_string)
    )


def serialize_q4_partial_message(client_id, origin_node, msg_seq, node_id, pairs):
    return (
        serialize_uint8(INT_Q4_PARTIAL)
        + serialize_uint32(client_id)
        + serialize_internal_header(origin_node, msg_seq)
        + serialize_uint16(node_id)
        + serialize_q4_payload(pairs)
    )