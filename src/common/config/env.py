"""Lectura de variables de entorno."""

import os

from . import constants


def _get(name, default=None, cast=str):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return cast(value)


def mom_host():
    return _get("MOM_HOST", constants.DEFAULT_MOM_HOST)


def mom_port():
    return _get("MOM_PORT", constants.DEFAULT_MOM_PORT, int)


def server_host():
    return _get("SERVER_HOST", constants.DEFAULT_SERVER_HOST)


def server_port():
    return _get("SERVER_PORT", constants.DEFAULT_SERVER_PORT, int)


def controller_type():
    return _get("CONTROLLER_TYPE")


def node_id():
    return _get("NODE_ID")


def in_queue():
    return _get("IN_QUEUE")


def out_queue():
    return _get("OUT_QUEUE")


def control_exchange():
    return _get("CONTROL_EXCHANGE", constants.CONTROL_EXCHANGE)


def control_key():
    return _get("CONTROL_KEY")


def batch_size():
    return _get("BATCH_SIZE", constants.BATCH_SIZE, int)


def max_frame():
    return _get("MAX_FRAME", constants.MAX_FRAME, int)


def result_chunk_size():
    return _get("RESULT_CHUNK_SIZE", constants.RESULT_CHUNK_SIZE, int)


def progress_batch_size():
    return _get("PROGRESS_BATCH_SIZE", constants.PROGRESS_BATCH_SIZE, int)


def logs_queue():
    return _get("LOGS_QUEUE", constants.LOGS_QUEUE)


def log_dir():
    return _get("LOG_DIR", constants.LOG_DIR)


def results_queue():
    return _get("RESULTS_QUEUE", constants.QUERY_RESULTS)


def input_file():
    return _get("INPUT_FILE", constants.INPUT_FILE)
