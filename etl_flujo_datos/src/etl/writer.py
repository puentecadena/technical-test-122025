# Data Writer Module
"""
Módulo para la escritura de datos de salida usando PySpark.
"""

from pyspark.sql import DataFrame
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)


class DataWriter:
    """
    Clase para escribir datos procesados a diferentes formatos y destinos.
    """

    def __init__(
        self,
        output_base_path: str,
        output_format: str = "parquet",
        partition_column: str = "process_date",
        mode: str = "overwrite",
        compression: str = "snappy",
    ):
        """
        Inicializa el DataWriter con la configuración de salida.

        Args:
            output_base_path: Ruta base para los archivos de salida.
            output_format: Formato de salida (parquet, csv, json).
            partition_column: Columna para particionar los datos.
            mode: Modo de escritura (overwrite, append, error, ignore).
            compression: Algoritmo de compresión para parquet.
        """
        self.output_base_path = output_base_path
        self.output_format = output_format.lower()
        self.partition_column = partition_column
        self.mode = mode
        self.compression = compression

    def write(
        self,
        df: DataFrame,
        partition_by: Optional[str] = None,
    ) -> str:
        """
        Escribe el DataFrame al destino configurado.

        Args:
            df: DataFrame a escribir.
            partition_by: Columna de partición (override de la configuración).

        Returns:
            str: Ruta donde se escribieron los datos.
        """
        partition_col = partition_by or self.partition_column
        output_path = self.output_base_path

        logger.info(f"Escribiendo datos a: {output_path}")
        logger.info(f"Formato: {self.output_format}")
        logger.info(f"Partición por: {partition_col}")
        logger.info(f"Modo: {self.mode}")

        writer = df.write.mode(self.mode)

        if self.output_format == "parquet":
            self._write_parquet(writer, output_path, partition_col)
        elif self.output_format == "csv":
            self._write_csv(writer, output_path, partition_col)
        elif self.output_format == "json":
            self._write_json(writer, output_path, partition_col)
        else:
            raise ValueError(f"Formato no soportado: {self.output_format}")

        logger.info(f"Datos escritos exitosamente a: {output_path}")
        return output_path

    def _write_parquet(
        self,
        writer,
        output_path: str,
        partition_col: str,
    ) -> None:
        """
        Escribe datos en formato Parquet.

        Args:
            writer: DataFrameWriter de Spark.
            output_path: Ruta de salida.
            partition_col: Columna de partición.
        """
        writer = writer.option("compression", self.compression)

        if partition_col and partition_col in writer._df.columns:
            writer.partitionBy(partition_col).parquet(output_path)
        else:
            writer.parquet(output_path)

    def _write_csv(
        self,
        writer,
        output_path: str,
        partition_col: str,
    ) -> None:
        """
        Escribe datos en formato CSV.

        Args:
            writer: DataFrameWriter de Spark.
            output_path: Ruta de salida.
            partition_col: Columna de partición.
        """
        writer = writer.option("header", "true")
        writer = writer.option("quote", '"')
        writer = writer.option("escape", '"')

        if partition_col and partition_col in writer._df.columns:
            writer.partitionBy(partition_col).csv(output_path)
        else:
            writer.csv(output_path)

    def _write_json(
        self,
        writer,
        output_path: str,
        partition_col: str,
    ) -> None:
        """
        Escribe datos en formato JSON.

        Args:
            writer: DataFrameWriter de Spark.
            output_path: Ruta de salida.
            partition_col: Columna de partición.
        """
        if partition_col and partition_col in writer._df.columns:
            writer.partitionBy(partition_col).json(output_path)
        else:
            writer.json(output_path)

    def write_by_date(
        self,
        df: DataFrame,
        date_column: str = "process_date",
    ) -> dict:
        """
        Escribe los datos particionados por fecha en carpetas separadas.

        Args:
            df: DataFrame a escribir.
            date_column: Columna de fecha para particionar.

        Returns:
            dict: Diccionario con las fechas y rutas donde se escribieron los datos.
        """
        logger.info(f"Escribiendo datos particionados por: {date_column}")

        # Obtener fechas únicas
        dates = (
            df.select(date_column)
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

        output_paths = {}

        for date_value in dates:
            # Filtrar datos para esta fecha
            date_df = df.filter(df[date_column] == date_value)

            # Crear path para esta fecha
            date_path = os.path.join(self.output_base_path, f"{date_column}={date_value}")

            logger.info(f"Escribiendo datos para {date_column}={date_value}")

            # Escribir sin partición adicional
            writer = date_df.write.mode(self.mode)

            if self.output_format == "parquet":
                writer.option("compression", self.compression).parquet(date_path)
            elif self.output_format == "csv":
                writer.option("header", "true").csv(date_path)
            elif self.output_format == "json":
                writer.json(date_path)

            output_paths[str(date_value)] = date_path

        logger.info(f"Datos escritos para {len(dates)} fechas diferentes")
        return output_paths

    def get_output_summary(self, df: DataFrame) -> dict:
        """
        Genera un resumen de los datos a escribir.

        Args:
            df: DataFrame a analizar.

        Returns:
            dict: Resumen de los datos.
        """
        summary = {
            "total_records": df.count(),
            "columns": df.columns,
            "output_path": self.output_base_path,
            "output_format": self.output_format,
            "partition_column": self.partition_column,
        }

        # Contar registros por partición
        if self.partition_column and self.partition_column in df.columns:
            partition_counts = (
                df.groupBy(self.partition_column)
                .count()
                .collect()
            )
            summary["records_per_partition"] = {
                str(row[self.partition_column]): row["count"]
                for row in partition_counts
            }

        logger.info(f"Output Summary: {summary}")
        return summary
