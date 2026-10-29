"""Nivel 2: simulador de ArticleHandler para probar Q1 contra RabbitMQ."""

import argparse
import os
import sys
import threading
from datetime import datetime, timezone

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.model.article import Article
from common.protocol.constants import (
    ART_DATE_MODIFIED,
    ART_DOCID,
    ART_IN_LANGUAGE,
    ART_URL,
    LANG_EN,
    LANG_FR,
    ControlType,
    InternalType,
)
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal
from controllers.q1_controller import Q1Controller

MASK = ART_DOCID | ART_URL | ART_DATE_MODIFIED | ART_IN_LANGUAGE
CLIENT_ID = 1


def epoch(year):
    return int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())


def make_articles():
    return [
        Article(1, url="https://en.wikipedia.org/wiki/1", dateModified=epoch(2020), inLanguage=LANG_EN),
        Article(2, url="https://en.wikipedia.org/wiki/2", dateModified=epoch(2019), inLanguage=LANG_EN),
        Article(3, url="https://en.wikipedia.org/wiki/3", dateModified=epoch(2021), inLanguage=LANG_EN),
        Article(4, url="https://fr.wikipedia.org/wiki/4", dateModified=epoch(2020), inLanguage=LANG_FR),
        Article(5, url="https://en.wikipedia.org/wiki/5", dateModified=epoch(2020), inLanguage=LANG_EN),
        Article(6, url="https://fr.wikipedia.org/wiki/6", dateModified=epoch(2018), inLanguage=LANG_FR),
    ]


class Simulator:
    def __init__(self, host, in_queue, out_queue, controllers):
        self.input = MessageMiddlewareQueueRabbitMQ(host, in_queue)
        self.results = MessageMiddlewareQueueRabbitMQ(host, out_queue)
        self.controllers = controllers
        self.articles = make_articles()
        self.expected = [article for article in self.articles if Q1Controller._matches(article)]
        self.seen = []
        self.dones = []
        self.done = threading.Event()

    def run(self):
        timer = threading.Timer(30, self._timeout)
        timer.start()
        try:
            self._publish()
            self.results.start_consuming(self._on_result)
        finally:
            timer.cancel()
            self.input.close()
            self.results.close()
        self._check()

    def _publish(self):
        for article in self.articles:
            self.input.send(
                internal.serialize_article_message(CLIENT_ID, article, MASK)
            )
        self.input.send(control.serialize_eof(CLIENT_ID, len(self.articles)))

    def _on_result(self, body, ack, nack):
        try:
            if body[0] >= 101:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE and client_id == CLIENT_ID:
                    self.dones.append(message)
                    if len(self.dones) >= self.controllers:
                        self.done.set()
                        try:
                            self.results.stop_consuming()
                        except Exception:
                            pass
            else:
                msg_type, client_id, result = internal.deserialize(body)
                if msg_type == InternalType.RESULT and client_id == CLIENT_ID:
                    self.seen.append(
                        external.deserialize_q1_payload(result["payload"])
                    )
            ack()
        except Exception:
            nack()

    def _check(self):
        expected = {(article.url, article.dateModified) for article in self.expected}
        seen = {(item["url"], item["dateModified"]) for item in self.seen}

        if seen != expected:
            raise AssertionError(f"resultados: esperado {expected}, visto {seen}")
        if len(self.dones) != self.controllers:
            raise AssertionError(
                f"DONEs: esperado {self.controllers}, visto {len(self.dones)}"
            )
        emitted = sum(done["quantity"] for done in self.dones)
        if emitted != len(self.expected):
            raise AssertionError(
                f"quantity de DONEs: esperado {len(self.expected)}, visto {emitted}"
            )

    def _timeout(self):
        print("timeout esperando los DONEs", file=sys.stderr)
        os._exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--controllers", type=int, default=1)
    parser.add_argument("--in-queue", default="Q1_Test_Metadata")
    parser.add_argument("--out-queue", default="Q1_Test_Results")
    args = parser.parse_args()

    Simulator(args.host, args.in_queue, args.out_queue, args.controllers).run()
    print("Q1 integration OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
