# Tests for DataTransformer
"""
Pruebas unitarias para el módulo transformer.py
Valida todas las transformaciones de datos del ETL.
"""

import pytest
from decimal import Decimal
import sys
import os

# Agregar src al path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from etl.transformer import DataTransformer


class TestDataTransformerInit:
    """Pruebas de inicialización del DataTransformer."""

    def test_default_values(self):
        """Verifica valores por defecto del transformer."""
        transformer = DataTransformer()
        
        assert transformer.cs_to_st_factor == 20
        assert transformer.routine_delivery_types == ["ZPRE", "ZVE1"]
        assert transformer.bonus_delivery_types == ["Z04", "Z05"]
        assert transformer.valid_delivery_types == ["ZPRE", "ZVE1", "Z04", "Z05"]

    def test_custom_values(self):
        """Verifica configuración personalizada."""
        transformer = DataTransformer(
            cs_to_st_factor=10,
            routine_delivery_types=["TYPE1"],
            bonus_delivery_types=["TYPE2"],
        )
        
        assert transformer.cs_to_st_factor == 10
        assert transformer.routine_delivery_types == ["TYPE1"]
        assert transformer.bonus_delivery_types == ["TYPE2"]


class TestCleanData:
    """Pruebas para el método clean_data."""

    def test_removes_empty_material(self, spark, sample_data):
        """Verifica que elimina registros con material vacío."""
        transformer = DataTransformer()
        
        # Contar registros con material vacío antes
        empty_count = sample_data.filter(
            (sample_data.material.isNull()) | 
            (sample_data.material == "")
        ).count()
        assert empty_count == 2  # Hay 2 registros con material vacío
        
        # Limpiar datos
        result = transformer.clean_data(sample_data, remove_empty_material=True)
        
        # Verificar que no hay registros con material vacío
        empty_after = result.filter(
            (result.material.isNull()) | 
            (result.material == "")
        ).count()
        assert empty_after == 0

    def test_removes_duplicates(self, spark, sample_data):
        """Verifica que elimina registros duplicados."""
        transformer = DataTransformer()
        
        # Limpiar sin dedup
        result_no_dedup = transformer.clean_data(
            sample_data, 
            remove_empty_material=True,
            remove_duplicates=False
        )
        
        # Limpiar con dedup
        result_with_dedup = transformer.clean_data(
            sample_data, 
            remove_empty_material=True,
            remove_duplicates=True
        )
        
        # Debería haber menos registros con deduplicación
        assert result_with_dedup.count() <= result_no_dedup.count()

    def test_keeps_data_when_disabled(self, spark, sample_data):
        """Verifica que no modifica datos cuando las opciones están deshabilitadas."""
        transformer = DataTransformer()
        
        result = transformer.clean_data(
            sample_data,
            remove_empty_material=False,
            remove_duplicates=False,
            fix_scientific_notation=False,
        )
        
        # El conteo debería ser igual al original
        assert result.count() == sample_data.count()


class TestFilterByDateRange:
    """Pruebas para el método filter_by_date_range."""

    def test_filters_dates_correctly(self, spark, sample_data_clean):
        """Verifica que filtra por rango de fechas correctamente."""
        transformer = DataTransformer()
        
        # Filtrar solo enero 2025
        result = transformer.filter_by_date_range(
            sample_data_clean,
            start_date="2025-01-01",
            end_date="2025-01-31"
        )
        
        # Solo deberían quedar los registros de enero
        assert result.count() == 2  # 2 registros del 20250115

    def test_includes_boundary_dates(self, spark, sample_data_clean):
        """Verifica que incluye las fechas límite."""
        transformer = DataTransformer()
        
        # Filtrar incluyendo todas las fechas de prueba
        result = transformer.filter_by_date_range(
            sample_data_clean,
            start_date="2025-01-15",
            end_date="2025-03-15"
        )
        
        # Debería incluir 20250115, 20250220, 20250315
        assert result.count() == 4

    def test_empty_result_for_out_of_range(self, spark, sample_data_clean):
        """Verifica que retorna vacío si no hay datos en el rango."""
        transformer = DataTransformer()
        
        result = transformer.filter_by_date_range(
            sample_data_clean,
            start_date="2024-01-01",
            end_date="2024-12-31"
        )
        
        assert result.count() == 0


class TestFilterByCountry:
    """Pruebas para el método filter_by_country."""

    def test_filters_country_correctly(self, spark, sample_data_clean):
        """Verifica que filtra por país correctamente."""
        transformer = DataTransformer()
        
        result = transformer.filter_by_country(sample_data_clean, country="GT")
        
        # Solo registros de Guatemala
        assert result.count() == 2
        
        # Verificar que todos son de GT
        countries = result.select("pais").distinct().collect()
        assert len(countries) == 1
        assert countries[0].pais == "GT"

    def test_case_insensitive(self, spark, sample_data_clean):
        """Verifica que el filtro es case-insensitive."""
        transformer = DataTransformer()
        
        result_upper = transformer.filter_by_country(sample_data_clean, country="GT")
        result_lower = transformer.filter_by_country(sample_data_clean, country="gt")
        
        assert result_upper.count() == result_lower.count()


class TestFilterValidDeliveryTypes:
    """Pruebas para el método filter_valid_delivery_types."""

    def test_filters_valid_types(self, spark, sample_data):
        """Verifica que filtra solo tipos de entrega válidos."""
        transformer = DataTransformer()
        
        result = transformer.filter_valid_delivery_types(sample_data)
        
        # Verificar que no hay COBR
        cobr_count = result.filter(result.tipo_entrega == "COBR").count()
        assert cobr_count == 0
        
        # Verificar que solo hay tipos válidos
        types = [row.tipo_entrega for row in result.select("tipo_entrega").distinct().collect()]
        valid_types = transformer.valid_delivery_types
        for t in types:
            assert t in valid_types


class TestConvertUnits:
    """Pruebas para el método convert_units."""

    def test_converts_cs_to_st(self, spark, sample_data_clean):
        """Verifica que convierte CS a ST correctamente (×20)."""
        transformer = DataTransformer()
        
        result = transformer.convert_units(sample_data_clean)
        
        # Buscar el registro con unidad CS (MAT002, cantidad=5)
        cs_record = result.filter(result.material == "MAT002").first()
        
        # 5 CS * 20 = 100 ST
        assert float(cs_record.quantity_standardized) == 100.0
        assert cs_record.unit_standardized == "ST"

    def test_st_unchanged(self, spark, sample_data_clean):
        """Verifica que ST se mantiene sin cambios."""
        transformer = DataTransformer()
        
        result = transformer.convert_units(sample_data_clean)
        
        # Buscar el registro con unidad ST (MAT001, cantidad=10)
        st_record = result.filter(result.material == "MAT001").first()
        
        # 10 ST debería seguir siendo 10
        assert float(st_record.quantity_standardized) == 10.0
        assert st_record.unit_standardized == "ST"

    def test_custom_conversion_factor(self, spark, sample_data_clean):
        """Verifica que usa el factor de conversión personalizado."""
        transformer = DataTransformer(cs_to_st_factor=10)
        
        result = transformer.convert_units(sample_data_clean)
        
        cs_record = result.filter(result.material == "MAT002").first()
        
        # 5 CS * 10 = 50 ST (con factor personalizado)
        assert float(cs_record.quantity_standardized) == 50.0


class TestClassifyDeliveryTypes:
    """Pruebas para el método classify_delivery_types."""

    def test_classifies_routine_delivery(self, spark, sample_data_clean):
        """Verifica que clasifica entregas de rutina correctamente."""
        transformer = DataTransformer()
        
        result = transformer.classify_delivery_types(sample_data_clean)
        
        # ZPRE y ZVE1 deben ser routine=True
        zpre_record = result.filter(result.tipo_entrega == "ZPRE").first()
        zve1_record = result.filter(result.tipo_entrega == "ZVE1").first()
        
        assert zpre_record.is_routine_delivery == True
        assert zve1_record.is_routine_delivery == True
        assert zpre_record.is_bonus_delivery == False
        assert zve1_record.is_bonus_delivery == False

    def test_classifies_bonus_delivery(self, spark, sample_data_clean):
        """Verifica que clasifica entregas con bonificación correctamente."""
        transformer = DataTransformer()
        
        result = transformer.classify_delivery_types(sample_data_clean)
        
        # Z04 y Z05 deben ser bonus=True
        z04_record = result.filter(result.tipo_entrega == "Z04").first()
        z05_record = result.filter(result.tipo_entrega == "Z05").first()
        
        assert z04_record.is_bonus_delivery == True
        assert z05_record.is_bonus_delivery == True
        assert z04_record.is_routine_delivery == False
        assert z05_record.is_routine_delivery == False


class TestAddCalculatedColumns:
    """Pruebas para el método add_calculated_columns."""

    def test_calculates_total_value(self, spark, sample_data_clean):
        """Verifica que calcula el valor total correctamente."""
        transformer = DataTransformer()
        
        # Primero convertir unidades para tener quantity_standardized
        df = transformer.convert_units(sample_data_clean)
        result = transformer.add_calculated_columns(df)
        
        # MAT001: precio=100.50, quantity_standardized=10 -> total_value=1005.0
        mat001 = result.filter(result.material == "MAT001").first()
        assert float(mat001.total_value) == pytest.approx(1005.0, rel=0.01)

    def test_calculates_unit_price(self, spark, sample_data_clean):
        """Verifica que calcula el precio unitario correctamente."""
        transformer = DataTransformer()
        
        df = transformer.convert_units(sample_data_clean)
        result = transformer.add_calculated_columns(df)
        
        # MAT001: precio=100.50, cantidad=10 -> unit_price=10.05
        mat001 = result.filter(result.material == "MAT001").first()
        assert float(mat001.unit_price) == pytest.approx(10.05, rel=0.01)

    def test_adds_processing_timestamp(self, spark, sample_data_clean):
        """Verifica que agrega el timestamp de procesamiento."""
        transformer = DataTransformer()
        
        df = transformer.convert_units(sample_data_clean)
        result = transformer.add_calculated_columns(df)
        
        # Verificar que la columna existe y no es nula
        assert "processing_timestamp" in result.columns
        first_row = result.first()
        assert first_row.processing_timestamp is not None

    def test_adds_data_quality_flag(self, spark, sample_data_clean):
        """Verifica que agrega el flag de calidad de datos."""
        transformer = DataTransformer()
        
        df = transformer.convert_units(sample_data_clean)
        result = transformer.add_calculated_columns(df)
        
        # Todos los registros limpios deberían ser VALID
        flags = [row.data_quality_flag for row in result.collect()]
        assert all(flag == "VALID" for flag in flags)


class TestRenameColumns:
    """Pruebas para el método rename_columns_to_standard."""

    def test_renames_all_columns(self, spark, sample_data_clean):
        """Verifica que renombra todas las columnas al estándar."""
        transformer = DataTransformer()
        
        result = transformer.rename_columns_to_standard(sample_data_clean)
        
        expected_columns = {
            "country_code", "process_date", "transport_id", "route_id",
            "delivery_type", "material_code", "price", "quantity_original",
            "unit_original"
        }
        
        assert expected_columns.issubset(set(result.columns))

    def test_preserves_data(self, spark, sample_data_clean):
        """Verifica que los datos se preservan después del renombrado."""
        transformer = DataTransformer()
        
        result = transformer.rename_columns_to_standard(sample_data_clean)
        
        assert result.count() == sample_data_clean.count()


class TestTransformIntegration:
    """Pruebas de integración del método transform completo."""

    def test_full_transform_pipeline(self, spark, sample_data, transformed_schema):
        """Verifica que el pipeline completo funciona correctamente."""
        transformer = DataTransformer()
        
        result = transformer.transform(
            sample_data,
            start_date="2025-01-01",
            end_date="2025-12-31",
            country=None,
            data_quality_config={
                "remove_empty_material": True,
                "remove_duplicates": True,
                "fix_scientific_notation": True,
                "filter_valid_delivery_types": True,
            }
        )
        
        # Verificar que el resultado no está vacío
        assert result.count() > 0
        
        # Verificar que tiene las columnas esperadas
        for col in transformed_schema:
            assert col in result.columns

    def test_transform_with_country_filter(self, spark, sample_data):
        """Verifica que el filtro de país funciona en el pipeline."""
        transformer = DataTransformer()
        
        result = transformer.transform(
            sample_data,
            start_date="2025-01-01",
            end_date="2025-12-31",
            country="GT",
        )
        
        # Todos los registros deben ser de GT
        countries = [row.country_code for row in result.select("country_code").distinct().collect()]
        assert countries == ["GT"]
