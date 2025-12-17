"""
Tests para el módulo config_loader.

Este módulo contiene tests unitarios para la clase ConfigLoader
y la función parse_cli_arguments.
"""

import pytest
import tempfile
import os
from unittest.mock import patch
from omegaconf import OmegaConf

from src.utils.config_loader import ConfigLoader, parse_cli_arguments


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_config_content():
    """Contenido YAML de configuración de ejemplo."""
    return """
etl:
  name: "test_etl"
  version: "1.0.0"

paths:
  input_file: "data/input/test.csv"
  output_base: "data/processed"

filters:
  start_date: "2025-01-01"
  end_date: "2025-06-30"
  country: null

transformations:
  unit_conversion:
    cs_to_st_factor: 20
    standard_unit: "ST"
  delivery_types:
    routine:
      - "ZPRE"
      - "ZVE1"
    bonus:
      - "Z04"
      - "Z05"

spark:
  app_name: "Test_ETL"
  master: "local[*]"
  config:
    spark.sql.shuffle.partitions: "4"
    spark.driver.memory: "1g"

output:
  format: "parquet"
  mode: "overwrite"
"""


@pytest.fixture
def config_file(sample_config_content):
    """Crea un archivo de configuración temporal."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(sample_config_content)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def config_loader(config_file):
    """Instancia de ConfigLoader con configuración de prueba."""
    return ConfigLoader(config_file)


# =============================================================================
# Tests para ConfigLoader.__init__ y _load_config
# =============================================================================


class TestConfigLoaderInit:
    """Tests para la inicialización del ConfigLoader."""

    def test_init_loads_config(self, config_file):
        """Verifica que __init__ carga la configuración correctamente."""
        loader = ConfigLoader(config_file)
        
        assert loader.config is not None
        assert loader.config.etl.name == "test_etl"
        assert loader.config.etl.version == "1.0.0"

    def test_init_raises_error_for_missing_file(self):
        """Verifica que se lanza error si el archivo no existe."""
        with pytest.raises(RuntimeError) as exc_info:
            ConfigLoader("/path/to/nonexistent/config.yaml")
        
        assert "Error al cargar configuración" in str(exc_info.value)

    def test_init_stores_config_path(self, config_file):
        """Verifica que se almacena la ruta del archivo de configuración."""
        loader = ConfigLoader(config_file)
        
        assert loader.config_path == config_file


# =============================================================================
# Tests para override_with_cli
# =============================================================================


class TestOverrideWithCli:
    """Tests para el método override_with_cli."""

    def test_override_with_cli_start_date(self, config_loader):
        """Verifica que se puede sobreescribir start_date."""
        config_loader.override_with_cli(start_date="2025-03-01")
        
        assert config_loader.config.filters.start_date == "2025-03-01"

    def test_override_with_cli_end_date(self, config_loader):
        """Verifica que se puede sobreescribir end_date."""
        config_loader.override_with_cli(end_date="2025-12-31")
        
        assert config_loader.config.filters.end_date == "2025-12-31"

    def test_override_with_cli_country(self, config_loader):
        """Verifica que se puede sobreescribir country."""
        config_loader.override_with_cli(country="GT")
        
        assert config_loader.config.filters.country == "GT"

    def test_override_with_cli_multiple_values(self, config_loader):
        """Verifica que se pueden sobreescribir múltiples valores."""
        config_loader.override_with_cli(
            start_date="2025-02-01",
            end_date="2025-08-31",
            country="PE"
        )
        
        assert config_loader.config.filters.start_date == "2025-02-01"
        assert config_loader.config.filters.end_date == "2025-08-31"
        assert config_loader.config.filters.country == "PE"

    def test_override_with_cli_none_values_unchanged(self, config_loader):
        """Verifica que valores None no modifican la configuración."""
        original_start = config_loader.config.filters.start_date
        original_end = config_loader.config.filters.end_date
        
        config_loader.override_with_cli(start_date=None, end_date=None, country=None)
        
        assert config_loader.config.filters.start_date == original_start
        assert config_loader.config.filters.end_date == original_end


# =============================================================================
# Tests para _validate_date
# =============================================================================


class TestValidateDate:
    """Tests para el método _validate_date."""

    def test_validate_date_valid_format(self, config_loader):
        """Verifica que fechas válidas no lanzan excepción."""
        # No debe lanzar excepción
        config_loader._validate_date("2025-01-15", "test_field")
        config_loader._validate_date("2024-12-31", "test_field")
        config_loader._validate_date("2025-06-01", "test_field")

    def test_validate_date_invalid_format_wrong_separator(self, config_loader):
        """Verifica que fechas con separador incorrecto lanzan ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_date("2025/01/15", "test_field")
        
        assert "Formato de fecha inválido" in str(exc_info.value)
        assert "test_field" in str(exc_info.value)
        assert "YYYY-MM-DD" in str(exc_info.value)

    def test_validate_date_invalid_format_wrong_order(self, config_loader):
        """Verifica que fechas con orden incorrecto lanzan ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_date("15-01-2025", "start_date")
        
        assert "Formato de fecha inválido" in str(exc_info.value)

    def test_validate_date_invalid_format_incomplete(self, config_loader):
        """Verifica que fechas incompletas lanzan ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_date("2025-01", "end_date")
        
        assert "Formato de fecha inválido" in str(exc_info.value)

    def test_validate_date_invalid_format_text(self, config_loader):
        """Verifica que texto no válido lanza ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_date("invalid-date", "test_field")
        
        assert "Formato de fecha inválido" in str(exc_info.value)

    def test_validate_date_invalid_day(self, config_loader):
        """Verifica que días inválidos lanzan ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_date("2025-02-30", "test_field")
        
        assert "Formato de fecha inválido" in str(exc_info.value)


# =============================================================================
# Tests para _validate_country
# =============================================================================


class TestValidateCountry:
    """Tests para el método _validate_country."""

    @pytest.mark.parametrize("country", ["GT", "PE", "EC", "SV", "HN", "JM"])
    def test_validate_country_valid_codes(self, config_loader, country):
        """Verifica que códigos de país válidos no lanzan excepción."""
        # No debe lanzar excepción
        config_loader._validate_country(country)

    @pytest.mark.parametrize("country", ["gt", "pe", "ec", "sv", "hn", "jm"])
    def test_validate_country_valid_codes_lowercase(self, config_loader, country):
        """Verifica que códigos de país en minúsculas son válidos."""
        # No debe lanzar excepción
        config_loader._validate_country(country)

    def test_validate_country_invalid_code(self, config_loader):
        """Verifica que códigos de país inválidos lanzan ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_country("XX")
        
        assert "Código de país inválido" in str(exc_info.value)
        assert "XX" in str(exc_info.value)
        assert "GT, PE, EC, SV, HN, JM" in str(exc_info.value)

    def test_validate_country_invalid_empty(self, config_loader):
        """Verifica que cadena vacía lanza ValueError."""
        with pytest.raises(ValueError) as exc_info:
            config_loader._validate_country("")
        
        assert "Código de país inválido" in str(exc_info.value)


# =============================================================================
# Tests para get
# =============================================================================


class TestGet:
    """Tests para el método get."""

    def test_get_with_dot_notation_simple(self, config_loader):
        """Verifica obtención de valores con notación de punto simple."""
        assert config_loader.get("etl.name") == "test_etl"
        assert config_loader.get("etl.version") == "1.0.0"

    def test_get_with_dot_notation_nested(self, config_loader):
        """Verifica obtención de valores anidados."""
        assert config_loader.get("transformations.unit_conversion.cs_to_st_factor") == 20
        assert config_loader.get("transformations.unit_conversion.standard_unit") == "ST"

    def test_get_default_value_for_missing_key(self, config_loader):
        """Verifica que se retorna valor por defecto para claves inexistentes."""
        assert config_loader.get("nonexistent.key", default="default_value") == "default_value"
        assert config_loader.get("etl.missing_field", default=42) == 42

    def test_get_default_value_none(self, config_loader):
        """Verifica que se retorna None por defecto si no se especifica."""
        assert config_loader.get("nonexistent.key") is None

    def test_get_top_level_section(self, config_loader):
        """Verifica obtención de secciones completas."""
        etl_section = config_loader.get("etl")
        assert etl_section.name == "test_etl"
        assert etl_section.version == "1.0.0"


# =============================================================================
# Tests para get_spark_config
# =============================================================================


class TestGetSparkConfig:
    """Tests para el método get_spark_config."""

    def test_get_spark_config_returns_dict(self, config_loader):
        """Verifica que get_spark_config retorna un diccionario."""
        spark_config = config_loader.get_spark_config()
        
        assert isinstance(spark_config, dict)

    def test_get_spark_config_contains_expected_keys(self, config_loader):
        """Verifica que el diccionario contiene las claves esperadas."""
        spark_config = config_loader.get_spark_config()
        
        assert "spark.sql.shuffle.partitions" in spark_config
        assert "spark.driver.memory" in spark_config

    def test_get_spark_config_values(self, config_loader):
        """Verifica los valores de la configuración de Spark."""
        spark_config = config_loader.get_spark_config()
        
        assert spark_config["spark.sql.shuffle.partitions"] == "4"
        assert spark_config["spark.driver.memory"] == "1g"


# =============================================================================
# Tests para get_valid_delivery_types
# =============================================================================


class TestGetValidDeliveryTypes:
    """Tests para el método get_valid_delivery_types."""

    def test_get_valid_delivery_types_returns_list(self, config_loader):
        """Verifica que retorna una lista."""
        result = config_loader.get_valid_delivery_types()
        
        assert isinstance(result, list)

    def test_get_valid_delivery_types_contains_all_types(self, config_loader):
        """Verifica que contiene todos los tipos (routine + bonus)."""
        result = config_loader.get_valid_delivery_types()
        
        # Routine types
        assert "ZPRE" in result
        assert "ZVE1" in result
        # Bonus types
        assert "Z04" in result
        assert "Z05" in result

    def test_get_valid_delivery_types_correct_count(self, config_loader):
        """Verifica la cantidad correcta de tipos."""
        result = config_loader.get_valid_delivery_types()
        
        assert len(result) == 4  # 2 routine + 2 bonus


# =============================================================================
# Tests para get_routine_delivery_types
# =============================================================================


class TestGetRoutineDeliveryTypes:
    """Tests para el método get_routine_delivery_types."""

    def test_get_routine_delivery_types_returns_list(self, config_loader):
        """Verifica que retorna una lista."""
        result = config_loader.get_routine_delivery_types()
        
        assert isinstance(result, list)

    def test_get_routine_delivery_types_contains_expected(self, config_loader):
        """Verifica que contiene los tipos de rutina esperados."""
        result = config_loader.get_routine_delivery_types()
        
        assert "ZPRE" in result
        assert "ZVE1" in result

    def test_get_routine_delivery_types_correct_count(self, config_loader):
        """Verifica la cantidad correcta de tipos de rutina."""
        result = config_loader.get_routine_delivery_types()
        
        assert len(result) == 2

    def test_get_routine_delivery_types_not_contains_bonus(self, config_loader):
        """Verifica que no contiene tipos de bonificación."""
        result = config_loader.get_routine_delivery_types()
        
        assert "Z04" not in result
        assert "Z05" not in result


# =============================================================================
# Tests para get_bonus_delivery_types
# =============================================================================


class TestGetBonusDeliveryTypes:
    """Tests para el método get_bonus_delivery_types."""

    def test_get_bonus_delivery_types_returns_list(self, config_loader):
        """Verifica que retorna una lista."""
        result = config_loader.get_bonus_delivery_types()
        
        assert isinstance(result, list)

    def test_get_bonus_delivery_types_contains_expected(self, config_loader):
        """Verifica que contiene los tipos de bonificación esperados."""
        result = config_loader.get_bonus_delivery_types()
        
        assert "Z04" in result
        assert "Z05" in result

    def test_get_bonus_delivery_types_correct_count(self, config_loader):
        """Verifica la cantidad correcta de tipos de bonificación."""
        result = config_loader.get_bonus_delivery_types()
        
        assert len(result) == 2

    def test_get_bonus_delivery_types_not_contains_routine(self, config_loader):
        """Verifica que no contiene tipos de rutina."""
        result = config_loader.get_bonus_delivery_types()
        
        assert "ZPRE" not in result
        assert "ZVE1" not in result


# =============================================================================
# Tests para __repr__
# =============================================================================


class TestRepr:
    """Tests para el método __repr__."""

    def test_repr_returns_yaml_string(self, config_loader):
        """Verifica que __repr__ retorna una representación YAML."""
        result = repr(config_loader)
        
        assert isinstance(result, str)
        assert "etl:" in result
        assert "name: test_etl" in result


# =============================================================================
# Tests para parse_cli_arguments
# =============================================================================


class TestParseCliArguments:
    """Tests para la función parse_cli_arguments."""

    def test_parse_cli_arguments_config_required(self):
        """Verifica que --config es requerido."""
        with pytest.raises(SystemExit):
            with patch('sys.argv', ['main.py']):
                parse_cli_arguments()

    def test_parse_cli_arguments_config_only(self):
        """Verifica parseo con solo --config."""
        with patch('sys.argv', ['main.py', '--config', 'config/config.yaml']):
            args = parse_cli_arguments()
        
        assert args.config == 'config/config.yaml'
        assert args.start_date is None
        assert args.end_date is None
        assert args.country is None

    def test_parse_cli_arguments_with_start_date(self):
        """Verifica parseo con --start-date."""
        with patch('sys.argv', ['main.py', '--config', 'config.yaml', '--start-date', '2025-01-01']):
            args = parse_cli_arguments()
        
        assert args.start_date == '2025-01-01'

    def test_parse_cli_arguments_with_end_date(self):
        """Verifica parseo con --end-date."""
        with patch('sys.argv', ['main.py', '--config', 'config.yaml', '--end-date', '2025-12-31']):
            args = parse_cli_arguments()
        
        assert args.end_date == '2025-12-31'

    def test_parse_cli_arguments_with_country(self):
        """Verifica parseo con --country."""
        with patch('sys.argv', ['main.py', '--config', 'config.yaml', '--country', 'GT']):
            args = parse_cli_arguments()
        
        assert args.country == 'GT'

    def test_parse_cli_arguments_all_options(self):
        """Verifica parseo con todas las opciones."""
        with patch('sys.argv', [
            'main.py',
            '--config', 'config/config.yaml',
            '--start-date', '2025-01-01',
            '--end-date', '2025-06-30',
            '--country', 'PE'
        ]):
            args = parse_cli_arguments()
        
        assert args.config == 'config/config.yaml'
        assert args.start_date == '2025-01-01'
        assert args.end_date == '2025-06-30'
        assert args.country == 'PE'
