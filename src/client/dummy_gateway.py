"""
Dummy Gateway para pruebas de integración del Cliente Map-Reduce.
Simula el comportamiento del servidor, control de congestión (ACKs) y envío de resultados.
"""
import logging
import socket
import sys
import time

from common.protocol.constants import (
    ExternalType,
    FLAG_IS_LAST_PART,
    FLAG_IS_SESSION_END,
    ResultType,
)
from common.protocol.external import framing, serializer

logging.basicConfig(level=logging.INFO, format="[GATEWAY] %(message)s")

def run_dummy_gateway(host: str = "127.0.0.1", port: int = 8080):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Permite reutilizar el puerto inmediatamente si reiniciamos el script
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    server_sock.bind((host, port))
    server_sock.listen(1)
    logging.info(f"Escuchando conexiones en {host}:{port}...")

    try:
        while True:
            client_sock, addr = server_sock.accept()
            logging.info(f"Cliente conectado desde {addr}")
            
            try:
                # 1. FASE DE RECEPCIÓN (Simulando Map)
                while True:
                    payload = framing.recv_message(client_sock)
                    if not payload:
                        break
                        
                    msg_type, data = serializer.deserialize_message(payload)
                    logging.info(f"Mensaje recibido: {msg_type}")
                    
                    # Simulamos procesamiento y enviamos ACK (Backpressure)
                    time.sleep(0.1) 
                    framing.send_ack(client_sock)
                    
                    if msg_type == ExternalType.EOF:
                        logging.info("EOF del cliente recibido. Terminando ingesta.")
                        break

                # 2. FASE DE PROCESAMIENTO DISTRIBUIDO (Simulando Reduce)
                logging.info("Simulando latencia del clúster de Map-Reduce (5 segundos)...")
                for _ in range(5):
                    time.sleep(1)
                    # Enviamos latidos (ACKs) para que el ResultReceiver no muera por timeout
                    framing.send_ack(client_sock)
                    logging.info("Latido enviado.")

                # 3. FASE DE DEVOLUCIÓN DE RESULTADOS
                logging.info("Enviando resultados al cliente...")
                
                result_payload = serializer.serialize_result(
                    result_type=ResultType.Q1,
                    doc_id=1,
                    part_id=1,
                    flags=FLAG_IS_LAST_PART | FLAG_IS_SESSION_END,
                    payload=serializer.serialize_q1_payload(
                        "https://en.wikipedia.org/wiki/Dummy", 1577836800
                    ),
                )
                framing.send_message(client_sock, result_payload)
                logging.info("Sesión completada y cerrada limpiamente.")

            except Exception as e:
                logging.error(f"Error procesando al cliente: {e}")
            finally:
                client_sock.close()
                logging.info("Esperando nuevo cliente...\n")
                
    except KeyboardInterrupt:
        logging.info("Apagando Dummy Gateway.")
        server_sock.close()
        sys.exit(0)

if __name__ == "__main__":
    run_dummy_gateway()