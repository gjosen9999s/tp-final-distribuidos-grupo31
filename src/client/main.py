"""Entry point del cliente."""
import argparse
import os
import signal
import sys

from client.batch_sender import BatchSender
from client.parquet_reader import ParquetReader
from client.result_receiver import ResultReceiver


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=os.getenv("INPUT_FILE", "input.csv"))
    parser.add_argument("--host", default=os.getenv("SERVER_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("SERVER_PORT", "5678")))
    return parser.parse_args()


def main():
    args = parse_args()
    stop = False

    def on_signal(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    reader = ParquetReader(args.input)
    sender = BatchSender(args.host, args.port)
    receiver = ResultReceiver(args.host, args.port)
    try:
        for batch, is_last in reader.batches():
            if stop:
                break
            sender.send(batch, is_last)
        receiver.receive_until_last()
    finally:
        sender.close()
        receiver.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
