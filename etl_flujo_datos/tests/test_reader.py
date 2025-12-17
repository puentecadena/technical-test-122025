# Tests for DataReader
"""
Pruebas unitarias para el módulo reader.py
Valida la lectura de datos CSV del ETL.
"""

import pytest
import os
import sys
from decimal import Decimal

# Agregar src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.reader import DataReader


class TestDataReaderInit:
    """Pruebas de inicialización del DataReader."""

    def test_init_with_spark_session(self, spark):
        """Verifica que se inicializa correctamente con SparkSession."""
        reader = DataReader(spark)
        
        assert reader.spark is not None
        assert reader.spark == spark

    def test_schema_definition(self):
        """Verifica que el schema de entregas está definido correctamente."""
        schema = DataReader.ENTREGAS_SCHEMA
        
        expected_fields = [
            "pais", "fecha_proceso", "transporte", "ruta",
            "tipo_entrega", "material", "precio", "cantidad", "unidad"
        ]
        
        field_names = [field.name for field in schema.fields]
        assert field_names == expected_fields


class TestReadCsv:
    """Pruebas para el método read_csv."""

    def test_read_csv_with_schema(self, spark, tmp_path):
        """Verifica que lee CSV con schema correctamente."""
        # Crear archivo CSV temporal
        csv_content = """pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad
GT,20250115,T001,R001,ZPRE,MAT001,100.50,10,ST
PE,20250220,T002,R002,Z04,MAT002,50.00,5,CS"""
        
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        
        reader = DataReader(spark)
        df = reader.read_csv(
            str(csv_file),
            header=True,
            schema=DataReader.ENTREGAS_SCHEMA
        )
        
        assert df.count() == 2
        assert "pais" in df.columns
        assert "precio" in df.columns

    def test_read_csv_infers_schema(self, spark, tmp_path):
        """Verifica que infiere schema cuando no se proporciona."""
        csv_content = """col1,col2,col3
a,1,2.5
b,2,3.5"""
        
        csv_file = tmp_path / "test_infer.csv"
        csv_file.write_text(csv_content)
        
        reader = DataReader(spark)
        df = reader.read_csv(str(csv_file), header=True)
        
        assert df.count() == 2
        assert df.columns == ["col1", "col2", "col3"]

    def test_read_csv_with_delimiter(self, spark, tmp_path):
        """Verifica que lee CSV con delimitador personalizado."""
        csv_content = """col1;col2;col3
a;1;2.5
b;2;3.5"""
        
        csv_file = tmp_path / "test_semicolon.csv"
        csv_file.write_text(csv_content)
        
        reader = DataReader(spark)
        df = reader.read_csv(str(csv_file), header=True, delimiter=";")
        
        assert df.count() == 2
        assert df.columns == ["col1", "col2", "col3"]


class TestReadEntregas:
    """Pruebas para el método read_entregas."""

    def test_read_entregas_with_correct_schema(self, spark, tmp_path):
        """Verifica que lee archivo de entregas con el schema correcto."""
        csv_content = """pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad
GT,20250115,T001,R001,ZPRE,MAT001,100.500000000000000000,10.000000000000000000,ST
PE,20250220,T002,R002,Z04,MAT002,50.000000000000000000,5.000000000000000000,CS"""
        
        csv_file = tmp_path / "entregas.csv"
        csv_file.write_text(csv_content)
        
        reader = DataReader(spark)
        df = reader.read_entregas(str(csv_file))
        
        assert df.count() == 2
        
        # Verificar tipos de datos
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        assert "StringType()" in schema_dict["pais"]
        assert "Decimal" in schema_dict["precio"]
        assert "Decimal" in schema_dict["cantidad"]

    def test_read_entregas_handles_empty_values(self, spark, tmp_path):
        """Verifica que maneja valores vacíos correctamente."""
        csv_content = """pais,fecha_proceso,transporte,ruta,tipo_entrega,material,precio,cantidad,unidad
GT,20250115,T001,R001,ZPRE,,100.50,10,ST
PE,20250220,T002,R002,Z04,MAT002,50.00,5,CS"""
        
        csv_file = tmp_path / "entregas_empty.csv"
        csv_file.write_text(csv_content)
        
        reader = DataReader(spark)
        df = reader.read_entregas(str(csv_file))
        
        # Debería leer ambos registros (limpieza es responsabilidad del transformer)
        assert df.count() == 2


class TestGetDataframeInfo:
    """Pruebas para el método get_dataframe_info."""

    def test_returns_correct_info(self, spark, sample_data):
        """Verifica que retorna información correcta del DataFrame."""
        reader = DataReader(spark)
        info = reader.get_dataframe_info(sample_data)
        
        assert "total_records" in info
        assert "columns" in info
        assert "schema" in info
        assert "distinct_countries" in info
        assert "distinct_dates" in info
        assert "distinct_delivery_types" in info
        
        assert info["total_records"] == sample_data.count()
        assert set(info["columns"]) == set(sample_data.columns)

    def test_returns_distinct_values(self, spark, sample_data):
        """Verifica que retorna valores distintos correctamente."""
        reader = DataReader(spark)
        info = reader.get_dataframe_info(sample_data)
        
        # Verificar que hay países distintos
        assert len(info["distinct_countries"]) > 0
        
        # Verificar que hay tipos de entrega distintos
        assert len(info["distinct_delivery_types"]) > 0

    def test_handles_missing_columns(self, spark):
        """Verifica que maneja DataFrames sin las columnas esperadas."""
        # Crear DataFrame sin columnas de entregas
        df = spark.createDataFrame(
            [("a", 1), ("b", 2)],
            ["col1", "col2"]
        )
        
        reader = DataReader(spark)
        info = reader.get_dataframe_info(df)
        
        # Debería retornar listas vacías para columnas que no existen
        assert info["distinct_countries"] == []
        assert info["distinct_dates"] == []
        assert info["distinct_delivery_types"] == []
