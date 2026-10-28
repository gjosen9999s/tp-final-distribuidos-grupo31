"""Mock ArticleHandler para e2e: socket con el cliente + RabbitMQ con Q1."""

import argparse
import logging
import socket
import sys

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.protocol.constants import (
    ART_DATE_MODIFIED,
    ART_DOCID,
    ART_IN_LANGUAGE,
    ART_URL,
    FLAG_IS_LAST_PART,
    FLAG_IS_SESSION_END,
    ControlType,
    ExternalType,
    InternalType,
    ResultType,
)
from common.protocol.control import serializer as control
from common.protocol.external import framing, serializer
from common.protocol.internal import serializer as internal

logging.basicConfig(level=logging.INFO, format="[MOCK-AH] %(message)s")
logger = logging.getLogger("mock_ah")

CLIENT_ID = 1
Q1_MASK = ART_DOCID | ART_URL | ART_DATE_MODIFIED | ART_IN_LANGUAGE


class MockArticleHandler:
    def __init__(self, host, port, mom_host, in_queue, out_queue, controllers):
        self.host = host
        self.port = port
        self.input = MessageMiddlewareQueueRabbitMQ(mom_host, in_queue)
        self.results = MessageMiddlewareQueueRabbitMQ(mom_host, out_queue)
        self.controllers = controllers
        self.total_articles = 0
        self.results_seen = 0
        self.dones = 0

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(1)
        logger.info(f"Escuchando en {self.host}:{self.port}")

        try:
            while True:
                client_sock, addr = server.accept()
                logger.debug(f"Cliente conectado desde {addr}")
                try:
                    self._handle_client(client_sock)
                except ConnectionError:
                    logger.debug("Cliente cerró sin enviar datos (healthcheck)")
                except Exception as e:
                    logger.error(f"Error con el cliente: {e}")
                finally:
                    client_sock.close()
                    logger.debug("Cliente cerrado")
        except KeyboardInterrupt:
            pass
        finally:
            server.close()
            self.input.close()
            self.results.close()

    def _handle_client(self, client_sock):
        self.total_articles = 0
        self.results_seen = 0

        while True:
            payload = framing.recv_message(client_sock)
            msg_type, data = serializer.deserialize_message(payload)
            framing.send_ack(client_sock)

            if msg_type == ExternalType.BATCH:
                for article in data:
                    self.input.send(
                        internal.serialize_article_message(CLIENT_ID, article, Q1_MASK)
                    )
                    self.total_articles += 1
                logger.info(f"BATCH recibido: {len(data)} artículos")

            elif msg_type == ExternalType.EOF:
                logger.info(f"EOF del cliente. Total artículos: {self.total_articles}")
                break

            else:
                logger.warning(f"Mensaje externo ignorado: {msg_type}")

        self.input.send(control.serialize_eof(CLIENT_ID, self.total_articles))
        self.dones = 0
        self.results.start_consuming(
            lambda body, ack, nack: self._on_result(client_sock, body, ack, nack)
        )

        final = serializer.serialize_result(
            ResultType.Q1, 0, 0, FLAG_IS_LAST_PART | FLAG_IS_SESSION_END, b""
        )
        framing.send_message(client_sock, final)
        logger.info(
            f"Fin de sesión. Resultados reenviados: {self.results_seen}, "
            f"DONEs: {self.dones}"
        )

    def _on_result(self, client_sock, body, ack, nack):
        try:
            if body[0] >= 101:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE and client_id == CLIENT_ID:
                    self.dones += 1
                    if self.dones >= self.controllers:
                        self.results.stop_consuming()
            else:
                msg_type, client_id, result = internal.deserialize(body)
                if msg_type == InternalType.RESULT and client_id == CLIENT_ID:
                    self.results_seen += 1
                    framing.send_message(
                        client_sock,
                        serializer.serialize_result(
                            result["resultType"],
                            result["docId"],
                            result["partId"],
                            result["flags"],
                            result["payload"],
                        ),
                    )
            ack()
        except Exception:
            nack()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mom-host", default="localhost")
    parser.add_argument("--in-queue", default="Q1_Test_Metadata")
    parser.add_argument("--out-queue", default="Q1_Test_Results")
    parser.add_argument("--controllers", type=int, default=1)
    args = parser.parse_args()

    MockArticleHandler(
        args.host,
        args.port,
        args.mom_host,
        args.in_queue,
        args.out_queue,
        args.controllers,
    ).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
