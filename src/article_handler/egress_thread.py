"""Consume los resultados y los devuelve al cliente."""
import threading
import logging
from common.protocol.constants import CTRL_EOF, FLAG_IS_SESSION_END, InternalType, ControlType
from common.protocol.control import serializer as control
from common.protocol.external import serializer as external
from common.protocol.internal import serializer as internal

logger = logging.getLogger("EgressThread")


class EgressThread(threading.Thread):
    def __init__(self, results_queue, sessions, logger_client=None, stop_event=None):
        super().__init__(daemon=True)
        self.results_queue = results_queue
        self.sessions = sessions
        self.logger_client = logger_client
        self.stop_event = stop_event or threading.Event()

    def run(self):
        logger.info("[EgressThread] Escuchando resultados en results_queue...")
        try:
            self.results_queue.start_consuming(self._on_result)
        except Exception as e:
            if not self.stop_event.is_set():
                logger.error(f"[EgressThread] Excepción en consumidor de resultados: {e}")

    def stop(self):
        self.stop_event.set()
        try:
            self.results_queue.stop_consuming_threadsafe()
        except Exception:
            pass

    def _on_result(self, body, ack, nack):
        try:
            if body[0] >= CTRL_EOF:
                msg_type, client_id, message = control.deserialize(body)
                if msg_type == ControlType.DONE:
                    session = self.sessions.get_session(client_id)
                    if session:
                        # Esperar a que las N réplicas reporten DONE
                        if session.register_done():
                            try:
                                end_msg = external.serialize_result(
                                    0, 0, 0, FLAG_IS_SESSION_END, b""
                                )
                                session.send_result_or_buffer(end_msg)
                                logger.info(
                                    f"[EgressThread] Recibidos {session.dones_count}/{session.expected_dones} DONEs. "
                                    f"Fin de sesión enviado a cliente ID={client_id}"
                                )
                            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                                logger.warning(f"[EgressThread] Cliente ID={client_id} desconectado al enviar DONE: {e}")
                                self.sessions.unregister_session(client_id)
                        else:
                            logger.info(
                                f"[EgressThread] DONE parcial ({session.dones_count}/{session.expected_dones}) "
                                f"recibido para cliente ID={client_id}"
                            )
                ack()
            else:
                msg_type, client_id, result = internal.deserialize(body)
                if msg_type == InternalType.RESULT:
                    session = self.sessions.get_session(client_id)
                    if session:
                        try:
                            ext_msg = external.serialize_result(
                                result["resultType"],
                                result["docId"],
                                result["partId"],
                                result["flags"],
                                result["payload"]
                            )
                            session.send_result_or_buffer(ext_msg)
                        except (BrokenPipeError, ConnectionResetError, OSError) as e:
                            logger.warning(f"[EgressThread] Cliente ID={client_id} desconectado al enviar resultado: {e}")
                            self.sessions.unregister_session(client_id)
                ack()
        except Exception as e:
            logger.error(f"[EgressThread] Error inesperado procesando resultado: {e}")
            ack()