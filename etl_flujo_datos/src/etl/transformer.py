# Data Transformer Module
"""
Módulo para las transformaciones de datos usando PySpark.
Implementa todas las reglas de negocio del ETL.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, BooleanType, TimestampType
from datetime import datetime
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Clase para transformar los datos de entregas de productos.
    Implementa limpieza de datos, conversión de unidades y clasificación de entregas.
    """

    def __init__(
        self,
        cs_to_st_factor: int = 20,
        routine_delivery_types: List[str] = None,
        bonus_delivery_types: List[str] = None,
    ):
        """
        Inicializa el DataTransformer con la configuración de transformaciones.

        Args:
            cs_to_st_factor: Factor de conversión de cajas (CS) a unidades (ST).
            routine_delivery_types: Lista de códigos de entregas de rutina.
            bonus_delivery_types: Lista de códigos de entregas con bonificaciones.
        """
        self.cs_to_st_factor = cs_to_st_factor
        self.routine_delivery_types = routine_delivery_types or ["ZPRE", "ZVE1"]
        self.bonus_delivery_types = bonus_delivery_types or ["Z04", "Z05"]
        self.valid_delivery_types = (
            self.routine_delivery_types + self.bonus_delivery_types
        )

    def clean_data(
        self,
        df: DataFrame,
        remove_empty_material: bool = True,
        remove_duplicates: bool = True,
        fix_scientific_notation: bool = True,
    ) -> DataFrame:
        """
        Limpia los datos eliminando anomalías.

        Args:
            df: DataFrame de entrada.
            remove_empty_material: Eliminar registros con material vacío.
            remove_duplicates: Eliminar registros duplicados.
            fix_scientific_notation: Convertir notación científica a decimal.

        Returns:
            DataFrame: DataFrame limpio.
        """
        logger.info("Iniciando limpieza de datos")
        initial_count = df.count()

        # 1. Eliminar registros con material vacío o nulo
        if remove_empty_material:
            df = df.filter(
                (F.col("material").isNotNull()) &
                (F.trim(F.col("material")) != "") &
                (F.col("material") != '""')
            )
            after_material_count = df.count()
            logger.info(
                f"Registros eliminados por material vacío: "
                f"{initial_count - after_material_count}"
            )

        # 2. Corregir notación científica en precio (0E-18 -> 0)
        if fix_scientific_notation:
            # El DecimalType ya maneja esto, pero nos aseguramos
            df = df.withColumn(
                "precio",
                F.when(
                    F.col("precio").cast("string").contains("E"),
                    F.lit(0).cast(DecimalType(38, 18))
                ).otherwise(F.col("precio"))
            )

        # 3. Eliminar duplicados
        if remove_duplicates:
            before_dedup = df.count()
            df = df.dropDuplicates()
            after_dedup = df.count()
            logger.info(
                f"Registros duplicados eliminados: {before_dedup - after_dedup}"
            )

        final_count = df.count()
        logger.info(
            f"Limpieza completada. Registros: {initial_count} -> {final_count}"
        )

        return df

    def filter_by_date_range(
        self,
        df: DataFrame,
        start_date: str,
        end_date: str,
    ) -> DataFrame:
        """
        Filtra los datos por rango de fechas.

        Args:
            df: DataFrame de entrada.
            start_date: Fecha de inicio (YYYY-MM-DD).
            end_date: Fecha de fin (YYYY-MM-DD).

        Returns:
            DataFrame: DataFrame filtrado por fecha.
        """
        logger.info(f"Filtrando por rango de fechas: {start_date} a {end_date}")

        # Convertir fecha_proceso de YYYYMMDD a formato comparable
        df = df.withColumn(
            "fecha_proceso_parsed",
            F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
        )

        # Convertir fechas de filtro
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Filtrar por rango
        df = df.filter(
            (F.col("fecha_proceso_parsed") >= F.lit(start_dt)) &
            (F.col("fecha_proceso_parsed") <= F.lit(end_dt))
        )

        # Eliminar columna temporal
        df = df.drop("fecha_proceso_parsed")

        record_count = df.count()
        logger.info(f"Registros después del filtro de fechas: {record_count}")

        return df

    def filter_by_country(
        self,
        df: DataFrame,
        country: str,
    ) -> DataFrame:
        """
        Filtra los datos por país.

        Args:
            df: DataFrame de entrada.
            country: Código de país (GT, PE, EC, SV, HN, JM).

        Returns:
            DataFrame: DataFrame filtrado por país.
        """
        logger.info(f"Filtrando por país: {country}")

        df = df.filter(F.upper(F.col("pais")) == country.upper())

        record_count = df.count()
        logger.info(f"Registros después del filtro de país: {record_count}")

        return df

    def filter_valid_delivery_types(self, df: DataFrame) -> DataFrame:
        """
        Filtra solo los tipos de entrega válidos.

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con solo tipos de entrega válidos.
        """
        logger.info(f"Filtrando tipos de entrega válidos: {self.valid_delivery_types}")

        df = df.filter(F.col("tipo_entrega").isin(self.valid_delivery_types))

        record_count = df.count()
        logger.info(
            f"Registros después del filtro de tipos de entrega: {record_count}"
        )

        return df

    def convert_units(self, df: DataFrame) -> DataFrame:
        """
        Convierte todas las cantidades a unidades estándar (ST).
        CS (cajas) = 20 unidades (ST).

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con cantidades convertidas.
        """
        logger.info(f"Convirtiendo unidades (CS -> ST, factor: {self.cs_to_st_factor})")

        # Crear columna con cantidad estandarizada
        df = df.withColumn(
            "quantity_standardized",
            F.when(
                F.upper(F.col("unidad")) == "CS",
                F.col("cantidad") * self.cs_to_st_factor
            ).otherwise(F.col("cantidad")).cast(DecimalType(38, 6))
        )

        # Actualizar unidad a ST (estandarizada)
        df = df.withColumn("unit_standardized", F.lit("ST"))

        return df

    def classify_delivery_types(self, df: DataFrame) -> DataFrame:
        """
        Clasifica los tipos de entrega agregando columnas booleanas.

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con columnas de clasificación.
        """
        logger.info("Clasificando tipos de entrega")

        # Columna para entregas de rutina
        df = df.withColumn(
            "is_routine_delivery",
            F.when(
                F.col("tipo_entrega").isin(self.routine_delivery_types),
                F.lit(True)
            ).otherwise(F.lit(False)).cast(BooleanType())
        )

        # Columna para entregas con bonificaciones
        df = df.withColumn(
            "is_bonus_delivery",
            F.when(
                F.col("tipo_entrega").isin(self.bonus_delivery_types),
                F.lit(True)
            ).otherwise(F.lit(False)).cast(BooleanType())
        )

        return df

    def add_calculated_columns(self, df: DataFrame) -> DataFrame:
        """
        Agrega columnas calculadas adicionales (puntos extras).

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con columnas adicionales.
        """
        logger.info("Agregando columnas calculadas")

        # Valor total (precio * cantidad estandarizada)
        df = df.withColumn(
            "total_value",
            (F.col("precio") * F.col("quantity_standardized")).cast(DecimalType(38, 6))
        )

        # Precio unitario (precio / cantidad original)
        df = df.withColumn(
            "unit_price",
            F.when(
                F.col("cantidad") > 0,
                (F.col("precio") / F.col("cantidad")).cast(DecimalType(38, 6))
            ).otherwise(F.lit(0).cast(DecimalType(38, 6)))
        )

        # Timestamp de procesamiento
        df = df.withColumn(
            "processing_timestamp",
            F.current_timestamp()
        )

        # Flag de calidad de datos
        df = df.withColumn(
            "data_quality_flag",
            F.when(
                (F.col("precio") > 0) &
                (F.col("cantidad") > 0) &
                (F.col("material").isNotNull()) &
                (F.trim(F.col("material")) != ""),
                F.lit("VALID")
            ).otherwise(F.lit("REVIEW"))
        )

        return df

    def rename_columns_to_standard(self, df: DataFrame) -> DataFrame:
        """
        Renombra las columnas al estándar snake_case en inglés.

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con columnas renombradas.
        """
        logger.info("Renombrando columnas al estándar")

        # Mapeo de nombres originales a estándar
        column_mapping = {
            "pais": "country_code",
            "fecha_proceso": "process_date",
            "transporte": "transport_id",
            "ruta": "route_id",
            "tipo_entrega": "delivery_type",
            "material": "material_code",
            "precio": "price",
            "cantidad": "quantity_original",
            "unidad": "unit_original",
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)

        return df

    def select_final_columns(self, df: DataFrame) -> DataFrame:
        """
        Selecciona y ordena las columnas finales del dataset.

        Args:
            df: DataFrame de entrada.

        Returns:
            DataFrame: DataFrame con columnas finales ordenadas.
        """
        final_columns = [
            # Identificadores
            "country_code",
            "process_date",
            "transport_id",
            "route_id",
            "material_code",
            # Tipo de entrega
            "delivery_type",
            "is_routine_delivery",
            "is_bonus_delivery",
            # Cantidades y unidades
            "quantity_original",
            "unit_original",
            "quantity_standardized",
            "unit_standardized",
            # Valores monetarios
            "price",
            "unit_price",
            "total_value",
            # Metadatos
            "data_quality_flag",
            "processing_timestamp",
        ]

        # Seleccionar solo las columnas que existen
        existing_columns = [col for col in final_columns if col in df.columns]

        return df.select(existing_columns)

    def transform(
        self,
        df: DataFrame,
        start_date: str,
        end_date: str,
        country: Optional[str] = None,
        data_quality_config: dict = None,
    ) -> DataFrame:
        """
        Ejecuta todas las transformaciones en el DataFrame.

        Args:
            df: DataFrame de entrada.
            start_date: Fecha de inicio del rango.
            end_date: Fecha de fin del rango.
            country: Código de país (opcional).
            data_quality_config: Configuración de calidad de datos.

        Returns:
            DataFrame: DataFrame transformado.
        """
        logger.info("=== Iniciando transformaciones ===")

        # Configuración por defecto
        dq_config = data_quality_config or {
            "remove_empty_material": True,
            "remove_duplicates": True,
            "fix_scientific_notation": True,
            "filter_valid_delivery_types": True,
        }

        # 1. Limpieza de datos
        df = self.clean_data(
            df,
            remove_empty_material=dq_config.get("remove_empty_material", True),
            remove_duplicates=dq_config.get("remove_duplicates", True),
            fix_scientific_notation=dq_config.get("fix_scientific_notation", True),
        )

        # 2. Filtrar tipos de entrega válidos
        if dq_config.get("filter_valid_delivery_types", True):
            df = self.filter_valid_delivery_types(df)

        # 3. Filtrar por rango de fechas
        df = self.filter_by_date_range(df, start_date, end_date)

        # 4. Filtrar por país (si se especifica)
        if country:
            df = self.filter_by_country(df, country)

        # 5. Convertir unidades
        df = self.convert_units(df)

        # 6. Clasificar tipos de entrega
        df = self.classify_delivery_types(df)

        # 7. Agregar columnas calculadas
        df = self.add_calculated_columns(df)

        # 8. Renombrar columnas al estándar
        df = self.rename_columns_to_standard(df)

        # 9. Seleccionar columnas finales
        df = self.select_final_columns(df)

        logger.info("=== Transformaciones completadas ===")
        logger.info(f"Total de registros en output: {df.count()}")

        return df
