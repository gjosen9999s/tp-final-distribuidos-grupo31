"""Mock ArticleHandler para e2e: socket con el cliente + RabbitMQ con Q1."""

import argparse
import logging
import socket
import sys
import threading

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

Q1_MASK = ART_DOCID | ART_URL | ART_DATE_MODIFIED | ART_IN_LANGUAGE
READY_FILE = "/tmp/mock_ah_ready"


class ClientSession:
    def __init__(self, handler, sock, client_id):
        self.handler = handler
        self.sock = sock
        self.client_id = client_id
        self.send_lock = threading.Lock()
        self.eof_reached = threading.Event()
        self.done = threading.Event()
        self.buffer = []
        self.flushed = False
        self.results_seen = 0
        self.dones = 0
        self.total_articles = 0
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self.thread.start()

    def send_ack(self):
        with self.send_lock:
            framing.send_ack(self.sock)

    def send_result(self, payload):
        with self.send_lock:
            framing.send_message(self.sock, payload)

    def flush_buffer(self):
        if self.flushed:
            return
        for payload in self.buffer:
            self.send_result(payload)
        self.buffer.clear()
        self.flushed = True

    def _run(self):
        try:
            while True:
                payload = framing.recv_message(self.sock)
                msg_type, data = serializer.deserialize_message(payload)

                if msg_type == ExternalType.BATCH:
                    for article in data:
                        self.handler.publish_article(self, article)
                        self.total_articles += 1
                    self.send_ack()
                    logger.info(
                        f"Cliente {self.client_id}: BATCH de {len(data)} artículos"
                    )

                elif msg_type == ExternalType.EOF:
                    self.handler.publish_eof(self)
                    self.send_ack()
                    self.eof_reached.set()
                    logger.info(
                        f"Cliente {self.client_id}: EOF con {self.total_articles} artículos"
                    )
                    break

                else:
                    self.send_ack()
                    logger.warning(
                        f"Cliente {self.client_id}: mensaje externo {msg_type}"
                    )

        except ConnectionError:
            logger.debug(f"Cliente {self.client_id} cerró sin enviar datos")
        except Exception as e:
            logger.error(f"Cliente {self.client_id}: {e}")
        finally:
            if self.eof_reached.is_set():
                self.done.wait(timeout=300)
            self.sock.close()
            logger.info(
                f"Cliente {self.client_id}: resultados={self.results_seen}, "
                f"DONEs={self.dones}"
            )


class MockArticleHandler:
    def __init__(self, host, port, mom_host, in_queue, out_queue, controllers, clients):
        self.host = host
        self.port = port
        self.input = MessageMiddlewareQueueRabbitMQ(mom_host, in_queue)
        self.results = MessageMiddlewareQueueRabbitMQ(mom_host, out_queue)
        self.controllers = controllers
        self.clients = clients
        self.next_client_id = 1
        self.publish_lock = threading.Lock()
        self.sessions = {}

    def publish_article(self, session, article):
        with self.publish_lock:
            self.input.send(
                internal.serialize_article_message(
                    session.client_id, article, Q1_MASK
                )
            )

    def publish_eof(self, session):
        with self.publish_lock:
            self.input.send(
                control.serialize_eof(session.client_id, session.total_articles)
            )

    def _dispatch(self, body, ack, nack):
        try:
            if body[0] >= 101:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE:
                    session = self.sessions.get(client_id)
                    if session is not None:
                        session.dones += 1
                        if session.dones >= self.controllers:
                            session.flush_buffer()
                            final = serializer.serialize_result(
                                ResultType.Q1,
                                0,
                                0,
                                FLAG_IS_LAST_PART | FLAG_IS_SESSION_END,
                                b"",
                            )
                            session.send_result(final)
                            session.done.set()
            else:
                msg_type, client_id, result = internal.deserialize(body)
                if msg_type == InternalType.RESULT:
                    session = self.sessions.get(client_id)
                    if session is not None:
                        session.results_seen += 1
                        payload = serializer.serialize_result(
                            result["resultType"],
                            result["docId"],
                            result["partId"],
                            result["flags"],
                            result["payload"],
                        )
                        if session.eof_reached.is_set():
                            session.flush_buffer()
                            session.send_result(payload)
                        else:
                            session.buffer.append(payload)
            ack()
        except Exception:
            nack()

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)
        with open(READY_FILE, "w") as ready:
            ready.write("ready")
        logger.info(f"Escuchando en {self.host}:{self.port}")

        dispatch_thread = threading.Thread(
            target=self.results.start_consuming,
            args=(self._dispatch,),
            daemon=True,
        )
        dispatch_thread.start()

        sessions = []
        try:
            while self.clients == 0 or len(sessions) < self.clients:
                client_sock, addr = server.accept()
                client_id = self.next_client_id
                self.next_client_id += 1
                session = ClientSession(self, client_sock, client_id)
                self.sessions[client_id] = session
                sessions.append(session)
                logger.info(f"Cliente {client_id} conectado desde {addr}")
                session.start()

            for session in sessions:
                session.done.wait(timeout=300)
            for session in sessions:
                session.thread.join(timeout=10)

            logger.info(
                f"Total artículos: {sum(s.total_articles for s in sessions)} | "
                f"Total resultados Q1: {sum(s.results_seen for s in sessions)}"
            )
        except KeyboardInterrupt:
            pass
        finally:
            self.results.stop_consuming_threadsafe()
            dispatch_thread.join(timeout=5)
            server.close()
            self.input.close()
            self.results.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--mom-host", default="localhost")
    parser.add_argument("--in-queue", default="Q1_Test_Metadata")
    parser.add_argument("--out-queue", default="Q1_Test_Results")
    parser.add_argument("--controllers", type=int, default=1)
    parser.add_argument("--clients", type=int, default=1)
    args = parser.parse_args()

    MockArticleHandler(
        args.host,
        args.port,
        args.mom_host,
        args.in_queue,
        args.out_queue,
        args.controllers,
        args.clients,
    ).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
