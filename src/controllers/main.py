"""Entry point genérico de controllers (CONTROLLER_TYPE)."""
import importlib
import os
import signal
import sys

REGISTRY = {
    "q1_controller": ("controllers.q1_controller", "Q1Controller"),
    "q2_filter": ("controllers.q2_filter", "Q2Filter"),
    "image_downloader": ("controllers.image_downloader", "ImageDownloader"),
    "tokenizer": ("controllers.tokenizer", "Tokenizer"),
    "q3_reduce": ("controllers.q3_reduce", "Q3Reduce"),
    "q3_join": ("controllers.q3_join", "Q3Join"),
    "q4_counter": ("controllers.q4_counter", "Q4Counter"),
    "q4_join": ("controllers.q4_join", "Q4Join"),
    "q5_counter": ("controllers.q5_counter", "Q5Counter"),
    "q5_accumulator": ("controllers.q5_accumulator", "Q5Accumulator"),
    "q5_average_n_filter": ("controllers.q5_average_n_filter", "Q5AverageNFilter"),
}


def build(controller_type):
    try:
        module_name, class_name = REGISTRY[controller_type]
    except KeyError:
        raise SystemExit(f"CONTROLLER_TYPE inválido: {controller_type!r}")
    module = importlib.import_module(module_name)
    controller_class = getattr(module, class_name)
    return controller_class(
        controller_type=controller_type,
        in_queue=os.getenv("IN_QUEUE"),
        out_queue=os.getenv("OUT_QUEUE"),
        progress_k=int(os.getenv("PROGRESS_K", "100")),
    )


def main():
    controller_type = os.getenv("CONTROLLER_TYPE")
    if not controller_type:
        raise SystemExit("Falta CONTROLLER_TYPE")
    controller = build(controller_type)

    def on_signal(signum, frame):
        controller.stop()

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    controller.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
