"""Entry point del ArticleHandler."""
import os
import signal
import sys
import threading

from article_handler.egress_thread import EgressThread
from article_handler.ingest_thread import IngestThread
from article_handler.session_manager import SessionManager
from common.logging.logger import Logger
from common.middleware.middleware_rabbitmq import RabbitMQMiddleware


def main():
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "5678"))
    stop = threading.Event()

    middleware = RabbitMQMiddleware(
        host=os.getenv("MOM_HOST", "localhost"),
        port=int(os.getenv("MOM_PORT", "5672")),
    )
    logger = Logger(middleware)
    sessions = SessionManager()
    ingest = IngestThread(host, port, sessions, middleware, logger, stop)
    egress = EgressThread(middleware, logger, stop)

    def on_signal(signum, frame):
        stop.set()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    ingest.start()
    egress.start()
    try:
        ingest.join()
    finally:
        stop.set()
        ingest.join()
        egress.join()
        middleware.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
