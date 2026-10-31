"""
Módulo ResultReceiver.
Escucha sobre la conexión unificada para recolectar los resultados del clúster Map-Reduce.
Implementa procesamiento de latidos (heartbeats) para mantener viva la sesión durante esperas largas.
"""
import logging
import socket
from datetime import datetime, timezone
from typing import Any, Dict

from common.protocol.constants import ExternalType, FLAG_IS_SESSION_END, ResultType
from common.protocol.external import framing, serializer

logger = logging.getLogger(__name__)


class ResultReceiver:
    """
    Escucha y decodifica las respuestas del Gateway.
    Diseñado para tolerar esperas prolongadas mediante la recepción de ACKs (Heartbeats)
    y fallar rápido ante desconexiones silenciosas o errores del clúster.
    """

    def __init__(self, sock: socket.socket) -> None:
        """
        Inicializa el receiver recibiendo el socket ya conectado y configurado.
        """
        self._socket: socket.socket = sock
        # Estado interno de la recepción
        self._results_received: int = 0
        self._results_by_query_type: Dict[int, int]={}

    def _process_result(self, data: Dict[str, Any]) -> None:
        """
        Procesa de manera individual cada resultado retornado por el clúster.
        """
        
        self._results_received += 1
        resultType=data.get("resultType")
        if resultType and resultType > 0:
            self._results_by_query_type[resultType] = self._results_by_query_type.get(resultType, 0) + 1 

        detail = self._describe(data.get("resultType"), data.get("payload", b""))
        logger.info(
            f"Resultado [{self._results_received}] Q{data.get('resultType')} "
            f"doc={data.get('docId')} part={data.get('partId')} -> {detail}"
        )

    @staticmethod
    def _describe(result_type, payload):
        if not payload:
            return "fin de sesión"
        if result_type == ResultType.Q1:
            item = serializer.deserialize_q1_payload(payload)
            date = datetime.fromtimestamp(item["dateModified"], tz=timezone.utc)
            return f"{item['url']} ({date:%Y-%m-%d %H:%M:%S UTC})"
        return f"{len(payload)} bytes (sin decodificar)"

    def receive_until_last(self) -> None:
        """
        Bucle de red bloqueante.
        Lee secuencialmente los frames TCP, gestiona los latidos para evitar timeouts en 
        largas esperas, y evalúa la condición de corte (isSessionEnd o Error del clúster).
        """
        logger.info("Esperando resultados del clúster distribuido...")

        try:
            while True:
                # TimeoutError si el Gateway no envía datos ni latidos a tiempo
                payload = framing.recv_message(self._socket)

                # Heartbeat
                if framing.is_ack(payload):
                    logger.debug("Latido (ACK) recibido del Gateway. El clúster sigue procesando.")
                    continue

                # Deserialización del mensaje
                try:
                    msg_type, data = serializer.deserialize_message(payload)
                except ValueError as e:
                    logger.error(f"Fallo al decodificar el mensaje del Gateway: {e}")
                    raise RuntimeError("Corrupción en el protocolo binario de recepción.") from e

                # Enrutamiento de la respuesta
                if msg_type == ExternalType.RESULT:
                    self._process_result(data)
                    if data["flags"] & FLAG_IS_SESSION_END:
                        logger.info(
                            f"Señal de fin de sesión recibida del clúster. "
                            f"Total de resultados recolectados con éxito: {self._results_received}."
                        )
                        break

                elif msg_type == ExternalType.ERROR:
                    error_code = data.get("code", "UNKNOWN")
                    error_msg = data.get("message", "Sin descripción")
                    logger.error(f"El clúster abortó el procesamiento con un ERROR [{error_code}]: {error_msg}")
                    break
                
                else:
                    logger.warning(f"Tipo de mensaje externo ignorado o desconocido: {msg_type}")

        except TimeoutError as e:
            logger.critical(
                "El Gateway dejó de responder (heartbeat)"
                "Abortando recepción."
            )
            raise ConnectionError("Timeout esperando resultados del clúster.") from e
        except ConnectionError as e:
            logger.critical(f"Ruptura inesperada del canal TCP durante la recepción: {e}.")
            raise
        except Exception as e:
            logger.critical(f"Error crítico irrecuperable en el bucle de recepción: {e}")
            raise