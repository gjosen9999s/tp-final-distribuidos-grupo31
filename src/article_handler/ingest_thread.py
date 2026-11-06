"""Recibe los batches y los reparte a las colas internas."""
import socket
import threading
import logging
from pathlib import Path
from common.protocol.constants import (
    ART_DOCID, ART_NAME, ART_URL, ART_DATE_MODIFIED,
    ART_IN_LANGUAGE, ART_SECTIONS_FLAGS, ART_ABSTRACT, ART_IMAGE_URL, ExternalType
)
from common.protocol.external import framing, serializer as external
from common.protocol.internal import serializer as internal
from common.protocol.control import serializer as control

logger = logging.getLogger("IngestThread")
READY_FILE = "/tmp/article_handler_ready"

FULL_ARTICLE_MASK = (
    ART_DOCID | ART_NAME | ART_URL | ART_DATE_MODIFIED |
    ART_IN_LANGUAGE | ART_SECTIONS_FLAGS | ART_ABSTRACT | ART_IMAGE_URL
)


class IngestThread(threading.Thread):
    def __init__(self, host: str, port: int, sessions, data_queues, logger_client=None, stop_event=None):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.sessions = sessions
        if isinstance(data_queues, dict):
            self.data_queues = list(data_queues.values())
        else:
            self.data_queues = list(data_queues)
        self.logger_client = logger_client
        self.stop_event = stop_event or threading.Event()
        self.server_socket = None
        self.send_lock = threading.Lock()

    def run(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(128)
        self.server_socket.settimeout(1.0)

        Path(READY_FILE).write_text("ready")
        logger.info(f"[IngestThread] Servidor TCP escuchando en {self.host}:{self.port}")

        while not self.stop_event.is_set():
            try:
                client_sock, addr = self.server_socket.accept()
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"[IngestThread] Error al aceptar conexión: {e}")
                break

            session = self.sessions.register_session(client_sock, addr)
            logger.info(f"[IngestThread] Cliente conectado: ID={session.client_id} ({addr})")

            client_thread = threading.Thread(
                target=self._handle_client,
                args=(session,),
                daemon=True
            )
            client_thread.start()

        self._close_server_socket()
        self._remove_ready_file()

    def stop(self):
        self.stop_event.set()
        self._close_server_socket()
        self._remove_ready_file()

    def _remove_ready_file(self):
        try:
            Path(READY_FILE).unlink()
        except FileNotFoundError:
            pass

    def _close_server_socket(self):
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

    def _handle_client(self, session):
        client_id = session.client_id
        sock = session.sock
        total_articles = 0
        eof_received = False

        try:
            while not self.stop_event.is_set():
                try:
                    payload = framing.recv_message(sock)
                except (ConnectionError, TimeoutError, OSError, ValueError):
                    logger.info(f"[IngestThread] Cliente ID={client_id} desconectado.")
                    break

                msg_type, data = external.deserialize_message(payload)

                if msg_type == ExternalType.BATCH:
                    for article in data:
                        internal_msg = internal.serialize_article_message(
                            client_id, article, FULL_ARTICLE_MASK
                        )
                        with self.send_lock:
                            for queue in self.data_queues:
                                queue.send(internal_msg)
                        total_articles += 1

                    session.send_ack()

                elif msg_type == ExternalType.EOF:
                    expected_total = data.get("totalArticles", total_articles) if isinstance(data, dict) else total_articles
                    eof_msg = control.serialize_eof(client_id, expected_total)

                    with self.send_lock:
                        for queue in self.data_queues:
                            queue.send(eof_msg)

                    # Confirmar recepción del EOF al cliente
                    session.send_ack()
                    # Habilitar la transmisión y vaciar el buffer hacia el ResultReceiver
                    session.set_eof_reached()
                    eof_received = True
                    logger.info(f"[IngestThread] EOF recibido para cliente ID={client_id} (Total enviado: {expected_total})")
                    break
                else:
                    logger.warning(f"[IngestThread] Mensaje desconocido de cliente ID={client_id}: {msg_type}")
                    session.send_ack()

        except Exception as e:
            logger.error(f"[IngestThread] Error gestionando cliente ID={client_id}: {e}")
        finally:
            if not eof_received:
                self.sessions.unregister_session(client_id)