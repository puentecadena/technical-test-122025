# Pytest Configuration and Fixtures
"""
Fixtures compartidos para las pruebas unitarias del ETL.
Incluye configuración de SparkSession y DataFrames de prueba.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
)
from decimal import Decimal


@pytest.fixture(scope="session")
def spark():
    """
    Crea una SparkSession para las pruebas.
    Se usa scope="session" para reutilizar la misma sesión en todas las pruebas.
    """
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("ETL_Tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "512m")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_schema():
    """Schema del CSV de entregas."""
    return StructType([
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


@pytest.fixture
def sample_data(spark, sample_schema):
    """
    DataFrame de prueba con datos representativos.
    Incluye casos para probar todas las transformaciones.
    """
    data = [
        # Registros válidos de rutina
        ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", Decimal("100.50"), Decimal("10"), "ST"),
        ("GT", "20250115", "T001", "R001", "ZVE1", "MAT002", Decimal("200.00"), Decimal("5"), "CS"),
        # Registros válidos de bonificación
        ("PE", "20250220", "T002", "R002", "Z04", "MAT003", Decimal("50.00"), Decimal("20"), "ST"),
        ("EC", "20250315", "T003", "R003", "Z05", "MAT004", Decimal("75.25"), Decimal("2"), "CS"),
        # Registro con tipo COBR (debe ser filtrado)
        ("GT", "20250115", "T001", "R001", "COBR", "MAT005", Decimal("30.00"), Decimal("15"), "ST"),
        # Registro con material vacío (debe ser filtrado)
        ("GT", "20250115", "T001", "R001", "ZPRE", "", Decimal("25.00"), Decimal("8"), "ST"),
        ("GT", "20250115", "T001", "R001", "ZPRE", None, Decimal("25.00"), Decimal("8"), "ST"),
        # Registro duplicado (para probar deduplicación)
        ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", Decimal("100.50"), Decimal("10"), "ST"),
        # Registro fuera de rango de fechas
        ("GT", "20241215", "T004", "R004", "ZPRE", "MAT006", Decimal("80.00"), Decimal("12"), "ST"),
        # Registro con precio cero
        ("SV", "20250325", "T005", "R005", "ZPRE", "MAT007", Decimal("0"), Decimal("5"), "ST"),
    ]
    
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def sample_data_clean(spark, sample_schema):
    """
    DataFrame de prueba ya limpio (sin duplicados, sin vacíos).
    Útil para probar transformaciones que asumen datos limpios.
    """
    data = [
        ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", Decimal("100.50"), Decimal("10"), "ST"),
        ("GT", "20250115", "T001", "R001", "ZVE1", "MAT002", Decimal("200.00"), Decimal("5"), "CS"),
        ("PE", "20250220", "T002", "R002", "Z04", "MAT003", Decimal("50.00"), Decimal("20"), "ST"),
        ("EC", "20250315", "T003", "R003", "Z05", "MAT004", Decimal("75.25"), Decimal("2"), "CS"),
    ]
    
    return spark.createDataFrame(data, sample_schema)


@pytest.fixture
def transformed_schema():
    """Schema esperado después de las transformaciones."""
    return [
        "country_code",
        "process_date",
        "transport_id",
        "route_id",
        "material_code",
        "delivery_type",
        "is_routine_delivery",
        "is_bonus_delivery",
        "quantity_original",
        "unit_original",
        "quantity_standardized",
        "unit_standardized",
        "price",
        "unit_price",
        "total_value",
        "data_quality_flag",
        "processing_timestamp",
    ]


@pytest.fixture
def temp_output_path(tmp_path):
    """Directorio temporal para pruebas de escritura."""
    return str(tmp_path / "output")
