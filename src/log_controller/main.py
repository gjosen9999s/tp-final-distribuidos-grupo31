"""Entry point del Log Controller."""
import os
import signal
import sys

from common.middleware.middleware_rabbitmq import RabbitMQMiddleware
from log_controller.log_consumer import LogConsumer
from log_controller.log_persister import LogPersister


def main():
    middleware = RabbitMQMiddleware(
        host=os.getenv("MOM_HOST", "localhost"),
        port=int(os.getenv("MOM_PORT", "5672")),
    )
    persister = LogPersister(os.getenv("LOG_DIR", "/logs"))
    consumer = LogConsumer(middleware, persister, queue=os.getenv("LOGS_QUEUE", "logs"))

    def on_signal(signum, frame):
        consumer.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    try:
        consumer.run()
    finally:
        middleware.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
