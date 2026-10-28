"""
Módulo ResultReceiver.
Escucha sobre la conexión unificada para recolectar los resultados del clúster Map-Reduce.
Implementa procesamiento de latidos (heartbeats) para mantener viva la sesión durante esperas largas.
"""
import logging
import socket
from typing import Any, Dict
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

    def _process_result(self, data: Dict[str, Any]) -> None:
        """
        Procesa de manera individual cada resultado retornado por el clúster.
        """
        self._results_received += 1
        # ¿lógica de negocio final (ej. guardar a disco local)?
        logger.info(
            f"Resultado [{self._results_received}] recibido: "
            f"Tipo: {data.get('resultType')} | DocID: {data.get('docId')} | "
            f"PartID: {data.get('partId')} | Payload: {len(data.get('payload', b''))} bytes"
        )

    def receive_until_last(self) -> None:
        """
        Bucle de red bloqueante.
        Lee secuencialmente los frames TCP, gestiona los latidos para evitar timeouts en 
        largas esperas, y evalúa la condición de corte (EOF o Error del clúster).
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
                if msg_type == "result":
                    self._process_result(data)
                
                elif msg_type == "eof":
                    logger.info(
                        f"Señal de fin de procesamiento recibida del clúster. "
                        f"Total de resultados recolectados con éxito: {self._results_received}."
                    )
                    break
                
                elif msg_type == "error":
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