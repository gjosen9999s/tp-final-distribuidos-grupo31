"""Entry point del ArticleHandler."""
import os
import signal
import sys
import threading

from article_handler.egress_thread import EgressThread
from article_handler.ingest_thread import IngestThread
from article_handler.session_manager import SessionManager
from common.logging.logger import Logger
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

INITIAL_QUEUES = ("Q1_Metadata", "Q2_Sections", "Q34_Abstract", "Q5_Abstract")


def main():
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "5678"))
    mom_host = os.getenv("MOM_HOST", "localhost")
    stop = threading.Event()

    data_queues = {
        name: MessageMiddlewareQueueRabbitMQ(mom_host, name)
        for name in INITIAL_QUEUES
    }
    results = MessageMiddlewareQueueRabbitMQ(
        mom_host, os.getenv("RESULTS_QUEUE", "QueryResults")
    )
    logs = MessageMiddlewareQueueRabbitMQ(
        mom_host, os.getenv("LOGS_QUEUE", "logs")
    )

    logger = Logger(logs)
    sessions = SessionManager()
    ingest = IngestThread(host, port, sessions, data_queues, logger, stop)
    egress = EgressThread(results, logger, stop)

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
        for queue in data_queues.values():
            queue.close()
        results.close()
        logs.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
