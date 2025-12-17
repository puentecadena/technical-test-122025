# Data Reader Module
"""
Módulo para la lectura de datos de entrada usando PySpark.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType,
)
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class DataReader:
    """
    Clase para leer datos de entrada desde archivos CSV.
    """

    # Schema definido para el archivo CSV de entregas
    ENTREGAS_SCHEMA = StructType([
        StructField("pais", StringType(), True),
        StructField("fecha_proceso", StringType(), True),
        StructField("transporte", StringType(), True),
        StructField("ruta", StringType(), True),
        StructField("tipo_entrega", StringType(), True),
        StructField("material", StringType(), True),
        StructField("precio", DecimalType(38, 18), True),
        StructField("cantidad", DecimalType(38, 18), True),
        StructField("unidad", StringType(), True),
    ])

    def __init__(self, spark: SparkSession):
        """
        Inicializa el DataReader con una sesión de Spark.

        Args:
            spark: Sesión de Spark activa.
        """
        self.spark = spark

    def read_csv(
        self,
        file_path: str,
        header: bool = True,
        schema: Optional[StructType] = None,
        delimiter: str = ",",
    ) -> DataFrame:
        """
        Lee un archivo CSV y retorna un DataFrame de Spark.

        Args:
            file_path: Ruta al archivo CSV.
            header: Si el archivo tiene encabezado.
            schema: Schema opcional para el DataFrame.
            delimiter: Delimitador del CSV.

        Returns:
            DataFrame: DataFrame de Spark con los datos leídos.
        """
        logger.info(f"Leyendo archivo CSV: {file_path}")

        reader = self.spark.read.format("csv")
        reader = reader.option("header", str(header).lower())
        reader = reader.option("delimiter", delimiter)
        reader = reader.option("quote", '"')
        reader = reader.option("escape", '"')
        reader = reader.option("multiLine", "true")

        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")

        df = reader.load(file_path)

        record_count = df.count()
        logger.info(f"Registros leídos: {record_count}")

        return df

    def read_entregas(self, file_path: str) -> DataFrame:
        """
        Lee el archivo de entregas de productos con el schema predefinido.

        Args:
            file_path: Ruta al archivo CSV de entregas.

        Returns:
            DataFrame: DataFrame con los datos de entregas.
        """
        logger.info("Leyendo archivo de entregas de productos")
        return self.read_csv(
            file_path=file_path,
            header=True,
            schema=self.ENTREGAS_SCHEMA,
            delimiter=",",
        )

    def get_dataframe_info(self, df: DataFrame) -> dict:
        """
        Obtiene información del DataFrame para logging y debugging.

        Args:
            df: DataFrame a analizar.

        Returns:
            dict: Diccionario con información del DataFrame.
        """
        info = {
            "total_records": df.count(),
            "columns": df.columns,
            "schema": str(df.schema),
            "distinct_countries": (
                df.select("pais").distinct().rdd.flatMap(lambda x: x).collect()
                if "pais" in df.columns else []
            ),
            "distinct_dates": (
                df.select("fecha_proceso").distinct().rdd.flatMap(lambda x: x).collect()
                if "fecha_proceso" in df.columns else []
            ),
            "distinct_delivery_types": (
                df.select("tipo_entrega").distinct().rdd.flatMap(lambda x: x).collect()
                if "tipo_entrega" in df.columns else []
            ),
        }

        logger.info(f"DataFrame Info: {info}")
        return info
