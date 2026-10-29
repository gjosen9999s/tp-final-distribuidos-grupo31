"""Entry point del ArticleHandler."""
import logging
import os
import signal
import sys
import threading
from article_handler.egress_thread import EgressThread
from article_handler.ingest_thread import IngestThread
from article_handler.session_manager import SessionManager
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")

DEFAULT_QUEUES = ("Q1_Metadata", "Q2_Sections", "Q34_Abstract", "Q5_Abstract")


def main():
    logger.info("Iniciando servicio ArticleHandler (Gateway)...")
    host = os.getenv("SERVER_HOST", "0.0.0.0")
    port = int(os.getenv("SERVER_PORT", "5678"))
    mom_host = os.getenv("MOM_HOST", "localhost")
    expected_dones = int(os.getenv("CONTROLLERS_COUNT", "3"))
    stop = threading.Event()

    queues_env = os.getenv("DATA_QUEUES")
    if queues_env:
        target_queues = [q.strip() for q in queues_env.split(",") if q.strip()]
    else:
        target_queues = DEFAULT_QUEUES

    logger.info(f"Conectando a RabbitMQ en host='{mom_host}' para colas de ingesta: {target_queues}")

    data_queues = {
        name: MessageMiddlewareQueueRabbitMQ(mom_host, name) for name in target_queues
    }
    results = MessageMiddlewareQueueRabbitMQ(
        mom_host, os.getenv("RESULTS_QUEUE", "QueryResults")
    )

    sessions = SessionManager(expected_dones=expected_dones)

    ingest = IngestThread(host, port, sessions, data_queues, stop_event=stop)
    egress = EgressThread(results, sessions, stop_event=stop)

    def on_signal(signum, frame):
        logger.warning(f"Señal de apagado recibida ({signum}). Iniciando Graceful Shutdown...")
        stop.set()
        signal.signal(signal.SIGINT, on_signal)
        signal.signal(signal.SIGTERM, on_signal)

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    ingest.start()
    egress.start()

    try:
        ingest.join()
    finally:
        logger.info("Cerrando recursos y conexiones TCP/MOM...")
        stop.set()
        sessions.close_all()
        ingest.join()
        egress.join()
        for queue in data_queues.values():
            queue.close()
        results.close()
        logger.info("ArticleHandler finalizado con éxito.")

    return 0


if __name__ == "__main__":
    sys.exit(main())