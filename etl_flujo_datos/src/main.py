#!/usr/bin/env python3
# Main ETL Script
"""
Script principal para ejecutar el ETL de entregas de productos.
Utiliza PySpark y OmegaConf para el procesamiento de datos.
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from omegaconf import OmegaConf

# Agregar el directorio src al path
sys.path.insert(0, "/app")

from src.utils.config_loader import ConfigLoader, parse_cli_arguments
from src.etl.reader import DataReader
from src.etl.transformer import DataTransformer
from src.etl.writer import DataWriter

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def create_spark_session(config: ConfigLoader) -> SparkSession:
    """
    Crea y configura una sesión de Spark.

    Args:
        config: Configuración del ETL.

    Returns:
        SparkSession: Sesión de Spark configurada.
    """
    logger.info("Creando sesión de Spark")

    app_name = config.get("spark.app_name", "ETL_Entregas_Productos")
    master = config.get("spark.master", "local[*]")

    builder = SparkSession.builder.appName(app_name)

    # Configurar master solo si no es local
    if master and not master.startswith("local"):
        builder = builder.master(master)

    # Aplicar configuraciones adicionales de Spark
    spark_config = config.get_spark_config()
    for key, value in spark_config.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Configurar log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Sesión de Spark creada: {app_name}")
    logger.info(f"Master: {spark.sparkContext.master}")

    return spark


def run_etl(config: ConfigLoader, spark: SparkSession) -> dict:
    """
    Ejecuta el pipeline ETL completo.

    Args:
        config: Configuración del ETL.
        spark: Sesión de Spark.

    Returns:
        dict: Resumen de la ejecución.
    """
    logger.info("=" * 60)
    logger.info("INICIANDO ETL DE ENTREGAS DE PRODUCTOS")
    logger.info("=" * 60)

    start_time = datetime.now()

    # Obtener parámetros de configuración
    input_file = config.get("paths.input_file")
    output_base = config.get("paths.output_base")
    start_date = config.get("filters.start_date")
    end_date = config.get("filters.end_date")
    country = config.get("filters.country")

    logger.info(f"Input file: {input_file}")
    logger.info(f"Output path: {output_base}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Country filter: {country if country else 'ALL'}")

    # 1. LECTURA DE DATOS
    logger.info("-" * 40)
    logger.info("FASE 1: Lectura de datos")
    logger.info("-" * 40)

    reader = DataReader(spark)
    df_raw = reader.read_entregas(input_file)
    reader.get_dataframe_info(df_raw)

    # 2. TRANSFORMACIÓN DE DATOS
    logger.info("-" * 40)
    logger.info("FASE 2: Transformación de datos")
    logger.info("-" * 40)

    transformer = DataTransformer(
        cs_to_st_factor=config.get("transformations.unit_conversion.cs_to_st_factor", 20),
        routine_delivery_types=config.get_routine_delivery_types(),
        bonus_delivery_types=config.get_bonus_delivery_types(),
    )

    # Obtener configuración de calidad de datos
    dq_config = OmegaConf.to_container(
        config.config.data_quality, resolve=True
    )

    df_transformed = transformer.transform(
        df=df_raw,
        start_date=start_date,
        end_date=end_date,
        country=country,
        data_quality_config=dq_config,
    )

    # 3. ESCRITURA DE DATOS
    logger.info("-" * 40)
    logger.info("FASE 3: Escritura de datos")
    logger.info("-" * 40)

    writer = DataWriter(
        output_base_path=output_base,
        output_format=config.get("output.format", "parquet"),
        partition_column=config.get("output.partition_column", "process_date"),
        mode=config.get("output.mode", "overwrite"),
        compression=config.get("output.compression", "snappy"),
    )

    # Mostrar resumen antes de escribir
    output_summary = writer.get_output_summary(df_transformed)

    # Escribir datos particionados
    output_path = writer.write(df_transformed)

    # Calcular tiempo de ejecución
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()

    # Generar resumen de ejecución
    execution_summary = {
        "status": "SUCCESS",
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "execution_time_seconds": execution_time,
        "input_file": input_file,
        "output_path": output_path,
        "filters": {
            "start_date": start_date,
            "end_date": end_date,
            "country": country,
        },
        "output_summary": output_summary,
    }

    logger.info("=" * 60)
    logger.info("ETL COMPLETADO EXITOSAMENTE")
    logger.info("=" * 60)
    logger.info(f"Tiempo de ejecución: {execution_time:.2f} segundos")
    logger.info(f"Registros procesados: {output_summary['total_records']}")
    logger.info(f"Output path: {output_path}")

    return execution_summary


def main():
    """
    Función principal del ETL.
    """
    try:
        # Parsear argumentos CLI
        args = parse_cli_arguments()

        logger.info("Cargando configuración...")
        logger.info(f"Config file: {args.config}")

        # Cargar configuración
        config = ConfigLoader(args.config)

        # Sobreescribir con argumentos CLI si se proporcionan
        config.override_with_cli(
            start_date=args.start_date,
            end_date=args.end_date,
            country=args.country,
        )

        logger.info("Configuración cargada:")
        logger.info(f"\n{config}")

        # Crear sesión de Spark
        spark = create_spark_session(config)

        try:
            # Ejecutar ETL
            summary = run_etl(config, spark)

            # Mostrar resumen final
            logger.info("\n=== RESUMEN DE EJECUCIÓN ===")
            for key, value in summary.items():
                if isinstance(value, dict):
                    logger.info(f"{key}:")
                    for k, v in value.items():
                        logger.info(f"  {k}: {v}")
                else:
                    logger.info(f"{key}: {value}")

        finally:
            # Cerrar sesión de Spark
            spark.stop()
            logger.info("Sesión de Spark cerrada")

    except Exception as e:
        logger.error(f"Error en la ejecución del ETL: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
