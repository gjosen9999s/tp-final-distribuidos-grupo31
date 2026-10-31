"""
Entry point del cliente. Diseñado para ejecucion en contenedores.
Gestiona una conexion TCP unificada para garantizar la continuidad de la sesion.
"""
import argparse
import logging
import os
import signal
import socket
import sys
from dataclasses import dataclass
from types import FrameType
from typing import Optional

from client.batch_sender import BatchSender
from client.parquet_reader import ParquetReader
from client.result_receiver import ResultReceiver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


@dataclass
class ClientConfig:
    """Validacion de configuracion"""
    input_file: str
    server_host: str
    server_port: int
    timeout: float
    summary_file: str 

    @classmethod
    def from_env_and_args(cls) -> "ClientConfig":
        parser = argparse.ArgumentParser(description="Cliente Map-Reduce")
        parser.add_argument("--input", default=os.getenv("INPUT_FILE"))
        parser.add_argument("--host", default=os.getenv("SERVER_HOST"))
        parser.add_argument("--port", type=int, default=os.getenv("SERVER_PORT"))
        parser.add_argument("--timeout", type=float, default=os.getenv("TIMEOUT", "60.0"))
        parser.add_argument("--summary", default=os.getenv("SUMMARY_FILE"))
        
        args = parser.parse_args()

        if not args.input:
            raise ValueError("Falta la configuracion critica: INPUT_FILE (o argumento --input)")
        if not args.host:
            raise ValueError("Falta la configuracion critica: SERVER_HOST (o argumento --host)")
        if not args.port:
            raise ValueError("Falta la configuracion critica: SERVER_PORT (o argumento --port)")

        return cls(
            input_file=args.input,
            server_host=args.host,
            server_port=args.port,
            timeout=args.timeout,
            summary_file=args.summary
        )


class GracefulShutdown:
    """Manejador de señales del sistema para un cierre limpio en contenedores."""
    def __init__(self) -> None:
        self._shutdown_requested: bool = False
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, frame: Optional[FrameType]) -> None:
        signal_name = signal.Signals(signum).name
        logger.warning(f"Señal del sistema {signal_name} recibida. Iniciando Graceful Shutdown...")
        self._shutdown_requested = True

    @property
    def is_requested(self) -> bool:
        return self._shutdown_requested


def establish_connection(host: str, port: int, timeout: float) -> socket.socket:
    """Crea y conecta el cliente, utiliza un solo socket para envio y espera
    de los resultados del sistema"""
    logger.info(f"Intentando conectar al Gateway en {host}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        logger.info("Conexion TCP unificada establecida con exito.")
        return sock
    except Exception as e:
        sock.close()
        logger.critical(f"Fallo fatal al establecer conexion inicial: {e}")
        raise ConnectionError(f"Gateway inaccesible: {e}") from e



def main() -> int:
    # Configuracion, falla rapido si algo falta.
    try:
        config = ClientConfig.from_env_and_args()
    except ValueError as e:
        logger.critical(f"Error de configuracion en el arranque: {e}")
        return 1

    shutdown_handler = GracefulShutdown()
    shared_socket: Optional[socket.socket] = None
    reader: Optional[ParquetReader] = None

    try:
        # Recursos Compartidos
        shared_socket = establish_connection(
            config.server_host, 
            config.server_port, 
            config.timeout
        )
        
        reader = ParquetReader(config.input_file)
        
        # El mismo socket para envio y recepcion
        sender = BatchSender(shared_socket)
        receiver = ResultReceiver(shared_socket)

        # Ingesta
        logger.info("Iniciando lectura y transmision de lotes Parquet...")
        for batch, is_last in reader.batches():
            if shutdown_handler.is_requested:
                logger.warning("Abortando ingesta por solicitud del sistema. Enviando EOF prematuro.")
                # Enviamos un EOF forzado para liberar recursos en el cluster
                sender.send(batch=None, is_last=True)
                return 0

            sender.send(batch, is_last)

        # Recepcion
        if not shutdown_handler.is_requested:
            logger.info("Transmision finalizada. Delegando control al ResultReceiver...")
            receiver.receive_until_last()

        # Escritura de totales de control
        with open(config.summary_file, "w", encoding="utf-8") as file:
            for query_id, count in sorted(receiver._results_by_query_type.items()):
                file.write(f"Q{query_id}: {count}\n")

        logger.info("Ejecucion del cliente finalizada con exito.")
        return 0

    except ConnectionError as e:
        logger.error(f"La sesion fue abortada debido a un error de red irreparable: {e}")
        return 1
    except Exception as e:
        logger.critical(f"Fallo inesperado en la aplicacion: {e}", exc_info=True)
        return 1
    finally:
        logger.info("Ejecutando limpieza final de recursos...")
        if reader is not None:
            try:
                reader.close()
            except Exception as e:
                logger.debug(f"Aviso al cerrar ParquetReader: {e}")
        
        if shared_socket is not None:
            try:
                shared_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                shared_socket.close()
                logger.info("Socket unificado cerrado y puerto liberado.")
            except OSError:
                pass

if __name__ == "__main__":
    sys.exit(main())