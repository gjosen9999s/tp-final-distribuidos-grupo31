"""
ParquetReader.
Modulo que lee archivos *.parquet y retorna su informacion por lotes.
"""
import logging
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.lib import ArrowException

logger = logging.getLogger(__name__)


class ParquetReader:
    """
    Lector de archivos Parquet con lectura iterativa.
    """

    def __init__(
        self,
        file_path: str,
        batch_size: int = 10000,
        columns: Optional[List[str]] = None,
    ) -> None:
        self.file_path = Path(file_path)
        self.batch_size = batch_size
        self.columns = columns
        self.parquet_file: Optional[pq.ParquetFile] = None

        if not self.file_path.exists():
            error_msg = f"El archivo Parquet no existe en la ruta: {self.file_path}"
            logger.critical(error_msg)
            raise FileNotFoundError(error_msg)

        try:
            self.parquet_file = pq.ParquetFile(self.file_path)
            logger.info(
                f"Archivo Parquet cargado exitosamente: {self.file_path.name} "
                f"(Total filas aproximadas: {self.parquet_file.metadata.num_rows})"
            )
        except ArrowException as e:
            logger.critical(f"El archivo Parquet está corrupto o tiene un formato inválido: {e}")
            raise
        except OSError as e:
            logger.critical(f"Error de I/O al intentar acceder al archivo Parquet: {e}")
            raise

    def batches(self) -> Iterator[Tuple[pa.RecordBatch, bool]]:
        """
        Generador que lee el archivo en lotes, limitando el uso de memoria.
        
        Yields:
            Tuple[pa.RecordBatch, bool]: Una tupla con el bloque de datos y un flag 
                                         indicando si es el último bloque.
        """
        if self.parquet_file is None:
            raise RuntimeError("El archivo Parquet no fue inicializado correctamente.")

        iterator = self.parquet_file.iter_batches(
            batch_size=self.batch_size, 
            columns=self.columns
        )

        try:
            current_batch = next(iterator)
        except StopIteration:
            logger.warning("El archivo Parquet está vacío. No hay lotes para procesar.")
            return

        # Enviar el flag is_last
        for next_batch in iterator:
            yield current_batch, False
            current_batch = next_batch
            
        yield current_batch, True
        logger.info("Lectura del archivo Parquet completada.")

    def close(self) -> None:
        """
        Libera la referencia al archivo Parquet
        """
        if self.parquet_file is not None:
            self.parquet_file = None
            logger.info("Recursos de lectura de Parquet liberados.")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Prueba individual del lector de Parquet.")
    parser.add_argument(
        "--file", 
        type=str, 
        required=True, 
        help="Ruta absoluta o relativa al archivo .parquet a procesar"
    )
    parser.add_argument(
        "--batch_size", 
        type=int, 
        default=10, 
        help="Cantidad de filas por lote (default: 10)"
    )
    
    args = parser.parse_args()

    try:
        reader = ParquetReader(args.file, batch_size=args.batch_size, columns=["imageUrl"])
        for batch, is_last in reader.batches():
            print(f"Lote recibido. Filas: {batch.num_rows}, ¿Es el último?: {is_last}")
            print(batch)
    except Exception as e:
        logger.error(f"Fallo durante la ejecución de prueba: {e}")
    finally:
        if 'reader' in locals():
            reader.close()