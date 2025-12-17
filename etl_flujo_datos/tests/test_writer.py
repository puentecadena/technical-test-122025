# Tests for DataWriter
"""
Pruebas unitarias para el módulo writer.py
Valida la escritura de datos del ETL.
"""

import pytest
import os
import sys
from decimal import Decimal

# Agregar src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.writer import DataWriter


class TestDataWriterInit:
    """Pruebas de inicialización del DataWriter."""

    def test_default_values(self, temp_output_path):
        """Verifica valores por defecto del writer."""
        writer = DataWriter(output_base_path=temp_output_path)
        
        assert writer.output_base_path == temp_output_path
        assert writer.output_format == "parquet"
        assert writer.partition_column == "process_date"
        assert writer.mode == "overwrite"
        assert writer.compression == "snappy"

    def test_custom_values(self, temp_output_path):
        """Verifica configuración personalizada."""
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="csv",
            partition_column="country_code",
            mode="append",
            compression="gzip",
        )
        
        assert writer.output_format == "csv"
        assert writer.partition_column == "country_code"
        assert writer.mode == "append"
        assert writer.compression == "gzip"


class TestWriteParquet:
    """Pruebas para escritura en formato Parquet."""

    def test_writes_parquet_file(self, spark, sample_data_clean, temp_output_path):
        """Verifica que escribe archivos Parquet correctamente."""
        # Agregar columna process_date para partición
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="parquet",
            partition_column="process_date",
        )
        
        output_path = writer.write(df)
        
        # Verificar que se creó el directorio
        assert os.path.exists(output_path)
        
        # Verificar que hay archivos parquet
        has_parquet = any(
            f.endswith(".parquet") 
            for root, dirs, files in os.walk(output_path) 
            for f in files
        )
        assert has_parquet

    def test_writes_with_partition(self, spark, sample_data_clean, temp_output_path):
        """Verifica que particiona los datos correctamente."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="parquet",
            partition_column="process_date",
        )
        
        writer.write(df)
        
        # Verificar que hay directorios de partición
        partition_dirs = [
            d for d in os.listdir(temp_output_path) 
            if d.startswith("process_date=")
        ]
        assert len(partition_dirs) > 0


class TestWriteCsv:
    """Pruebas para escritura en formato CSV."""

    def test_writes_csv_file(self, spark, sample_data_clean, temp_output_path):
        """Verifica que escribe archivos CSV correctamente."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="csv",
            partition_column="process_date",
        )
        
        output_path = writer.write(df)
        
        # Verificar que se creó el directorio
        assert os.path.exists(output_path)
        
        # Verificar que hay archivos csv
        has_csv = any(
            f.endswith(".csv") 
            for root, dirs, files in os.walk(output_path) 
            for f in files
        )
        assert has_csv


class TestWriteJson:
    """Pruebas para escritura en formato JSON."""

    def test_writes_json_file(self, spark, sample_data_clean, temp_output_path):
        """Verifica que escribe archivos JSON correctamente."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="json",
            partition_column="process_date",
        )
        
        output_path = writer.write(df)
        
        # Verificar que se creó el directorio
        assert os.path.exists(output_path)
        
        # Verificar que hay archivos json
        has_json = any(
            f.endswith(".json") 
            for root, dirs, files in os.walk(output_path) 
            for f in files
        )
        assert has_json


class TestGetOutputSummary:
    """Pruebas para el método get_output_summary."""

    def test_returns_correct_summary(self, spark, sample_data_clean, temp_output_path):
        """Verifica que retorna resumen correcto."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="parquet",
            partition_column="process_date",
        )
        
        summary = writer.get_output_summary(df)
        
        assert "total_records" in summary
        assert "columns" in summary
        assert "output_path" in summary
        assert "output_format" in summary
        assert "partition_column" in summary
        
        assert summary["total_records"] == df.count()
        assert summary["output_format"] == "parquet"
        assert summary["partition_column"] == "process_date"

    def test_includes_records_per_partition(self, spark, sample_data_clean, temp_output_path):
        """Verifica que incluye conteo por partición."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            partition_column="process_date",
        )
        
        summary = writer.get_output_summary(df)
        
        assert "records_per_partition" in summary
        assert len(summary["records_per_partition"]) > 0


class TestUnsupportedFormat:
    """Pruebas para formatos no soportados."""

    def test_raises_error_for_invalid_format(self, spark, sample_data_clean, temp_output_path):
        """Verifica que lanza error para formato no soportado."""
        from pyspark.sql import functions as F
        df = sample_data_clean.withColumn("process_date", F.col("fecha_proceso"))
        
        writer = DataWriter(
            output_base_path=temp_output_path,
            output_format="invalid_format",
        )
        
        with pytest.raises(ValueError, match="Formato no soportado"):
            writer.write(df)
