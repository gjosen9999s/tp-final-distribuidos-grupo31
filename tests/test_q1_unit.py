"""Nivel 1: Q1 con MOMs falsos, sin RabbitMQ ni pika."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.model.article import Article
from common.protocol.constants import (
    ART_DATE_MODIFIED,
    ART_DOCID,
    ART_IN_LANGUAGE,
    ART_URL,
    FLAG_IS_LAST_PART,
    LANG_EN,
    ControlType,
    InternalType,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.q1_controller import Q1Controller

MASK = ART_DOCID | ART_URL | ART_DATE_MODIFIED | ART_IN_LANGUAGE
CLIENT = 7


class FakeMW:
    def __init__(self):
        self.sent = []

    def send(self, payload):
        self.sent.append(payload)

    def start_consuming(self, callback):
        self.callback = callback

    def stop_consuming(self):
        pass


def make_article(doc_id=1, year=2020, language=LANG_EN, url="https://en.wikipedia.org/wiki/X"):
    date = None
    if year is not None:
        date = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
    return Article(docId=doc_id, url=url, dateModified=date, inLanguage=language)


def decode_out(payload):
    if payload[0] >= 101:
        return control.deserialize(payload)
    return internal.deserialize(payload)


class Q1UnitTest(unittest.TestCase):
    def setUp(self):
        self.in_mw = FakeMW()
        self.out_mw = FakeMW()
        self.control_in = FakeMW()
        self.control_out = FakeMW()
        self.q1 = Q1Controller(
            "q1_controller",
            self.in_mw,
            self.out_mw,
            self.control_in,
            self.control_out,
            node_id=0,
            progress_batch_size=2,
            controller_count=1,
        )
        self.ack = lambda: None
        self.nack = lambda: None
        self.control_seen = 0

    def send_article(self, article, client_id=CLIENT):
        message = internal.serialize_article_message(client_id, article, MASK)
        self.q1._on_data(message, self.ack, self.nack)

    def send_eof(self, total, client_id=CLIENT):
        message = control.serialize_eof(client_id, total)
        self.q1._on_data(message, self.ack, self.nack)

    def replay_control(self):
        while self.control_seen < len(self.control_out.sent):
            payload = self.control_out.sent[self.control_seen]
            self.control_seen += 1
            self.q1._on_control(payload, self.ack, self.nack)

    def out_messages(self):
        return [decode_out(payload) for payload in self.out_mw.sent]

    def control_messages(self):
        return [control.deserialize(payload) for payload in self.control_out.sent]

    def matches(self, article):
        return Q1Controller._matches(article)

    def test_matches(self):
        self.assertTrue(self.matches(make_article(year=2020)))
        self.assertFalse(self.matches(make_article(year=2021)))
        self.assertFalse(self.matches(make_article(language=1)))
        self.assertFalse(self.matches(make_article(url=None)))
        self.assertFalse(self.matches(make_article(year=None)))
        self.assertFalse(self.matches(make_article(year=1970)))

    def test_matching_article_emits_result(self):
        self.send_article(make_article(doc_id=10))

        messages = self.out_messages()
        self.assertEqual(len(messages), 1)
        msg_type, client_id, result = messages[0]
        self.assertEqual(msg_type, InternalType.RESULT)
        self.assertEqual(client_id, CLIENT)
        self.assertEqual(result["resultType"], ResultType.Q1)
        self.assertEqual(result["docId"], 10)
        self.assertEqual(result["flags"] & FLAG_IS_LAST_PART, FLAG_IS_LAST_PART)

        payload = external.deserialize_q1_payload(result["payload"])
        self.assertEqual(payload["url"], "https://en.wikipedia.org/wiki/X")
        self.assertEqual(payload["dateModified"], 1577836800)

    def test_non_matching_does_not_emit_result(self):
        self.send_article(make_article(language=1))
        self.send_article(make_article(year=2021))
        self.assertEqual(self.out_messages(), [])

    def test_progress_every_b(self):
        self.send_article(make_article(doc_id=1))
        self.send_article(make_article(doc_id=2))

        messages = self.control_messages()
        self.assertEqual(len(messages), 1)
        msg_type, client_id, progress = messages[0]
        self.assertEqual(msg_type, ControlType.PROGRESS)
        self.assertEqual(client_id, CLIENT)
        self.assertEqual(progress["quantity"], 2)
        self.assertEqual(len(self.out_messages()), 2)

    def test_eof_flush_and_done(self):
        for doc_id in range(4):
            self.send_article(make_article(doc_id=doc_id))
        self.send_eof(4)
        self.replay_control()

        control_types = [message[0] for message in self.control_messages()]
        self.assertIn(ControlType.EOF, control_types)
        self.assertIn(ControlType.FLUSH, control_types)

        out = self.out_messages()
        results = [message for message in out if message[0] == InternalType.RESULT]
        dones = [message for message in out if message[0] == ControlType.DONE]
        self.assertEqual(len(results), 4)
        self.assertEqual(len(dones), 1)

        msg_type, client_id, done = dones[0]
        self.assertEqual(client_id, CLIENT)
        self.assertEqual(done["quantity"], 4)

    def test_empty_client(self):
        self.send_eof(0)
        self.replay_control()

        out = self.out_messages()
        self.assertEqual(len(out), 1)
        msg_type, client_id, done = out[0]
        self.assertEqual(msg_type, ControlType.DONE)
        self.assertEqual(client_id, CLIENT)
        self.assertEqual(done["quantity"], 0)

    def test_multiple_clients(self):
        self.send_article(make_article(doc_id=1), client_id=7)
        self.send_article(make_article(doc_id=2), client_id=8)
        self.assertEqual(len(self.out_messages()), 2)

        self.send_eof(1, client_id=7)
        self.send_eof(1, client_id=8)
        self.replay_control()

        dones = [
            message for message in self.out_messages()
            if message[0] == ControlType.DONE
        ]
        self.assertEqual(len(dones), 2)
        self.assertEqual({message[1] for message in dones}, {7, 8})

    def test_flush_mode_reports_each_article(self):
        self.send_article(make_article(doc_id=1))
        self.send_article(make_article(doc_id=2))
        self.send_eof(3)
        self.replay_control()

        control_types = [message[0] for message in self.control_messages()]
        self.assertIn(ControlType.FLUSH, control_types)

        self.send_article(make_article(doc_id=3))
        self.replay_control()

        progress_quantities = [
            message[2]["quantity"]
            for message in self.control_messages()
            if message[0] == ControlType.PROGRESS
        ]
        self.assertEqual(progress_quantities, [2, 1])
        self.assertEqual(
            len([m for m in self.out_messages() if m[0] == ControlType.DONE]),
            1,
        )


if __name__ == "__main__":
    unittest.main()
