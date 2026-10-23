"""Entry point del Log Controller."""
import os
import signal
import sys

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from log_controller.log_consumer import LogConsumer
from log_controller.log_persister import LogPersister


def main():
    mom_host = os.getenv("MOM_HOST", "localhost")
    logs = MessageMiddlewareQueueRabbitMQ(
        mom_host, os.getenv("LOGS_QUEUE", "logs")
    )
    persister = LogPersister(os.getenv("LOG_DIR", "/logs"))
    consumer = LogConsumer(logs, persister)

    def on_signal(signum, frame):
        consumer.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        consumer.run()
    finally:
        logs.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
