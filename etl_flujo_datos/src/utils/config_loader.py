# Config Loader - OmegaConf
"""
Módulo para cargar y gestionar la configuración del ETL usando OmegaConf.
"""

from typing import Any, Optional
from omegaconf import OmegaConf, DictConfig
import argparse
from datetime import datetime


class ConfigLoader:
    """
    Clase para cargar y gestionar la configuración del ETL.
    Permite cargar desde archivo YAML y sobreescribir con argumentos CLI.
    """

    def __init__(self, config_path: str):
        """
        Inicializa el ConfigLoader con un archivo de configuración.

        Args:
            config_path: Ruta al archivo de configuración YAML.
        """
        self.config_path = config_path
        self.config: DictConfig = self._load_config()

    def _load_config(self) -> DictConfig:
        """
        Carga la configuración desde el archivo YAML.

        Returns:
            DictConfig: Configuración cargada.
        """
        try:
            config = OmegaConf.load(self.config_path)
            return config
        except Exception as e:
            raise RuntimeError(f"Error al cargar configuración: {e}")

    def override_with_cli(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        country: Optional[str] = None,
    ) -> None:
        """
        Sobreescribe la configuración con argumentos CLI.

        Args:
            start_date: Fecha de inicio (YYYY-MM-DD).
            end_date: Fecha de fin (YYYY-MM-DD).
            country: Código de país (GT, PE, EC, SV, HN, JM).
        """
        if start_date:
            self._validate_date(start_date, "start_date")
            self.config.filters.start_date = start_date

        if end_date:
            self._validate_date(end_date, "end_date")
            self.config.filters.end_date = end_date

        if country:
            self._validate_country(country)
            self.config.filters.country = country

    def _validate_date(self, date_str: str, field_name: str) -> None:
        """
        Valida el formato de fecha.

        Args:
            date_str: Fecha en formato string.
            field_name: Nombre del campo para el mensaje de error.

        Raises:
            ValueError: Si el formato de fecha es inválido.
        """
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Formato de fecha inválido para {field_name}: {date_str}. "
                "Use el formato YYYY-MM-DD."
            )

    def _validate_country(self, country: str) -> None:
        """
        Valida el código de país.

        Args:
            country: Código de país.

        Raises:
            ValueError: Si el código de país no es válido.
        """
        valid_countries = ["GT", "PE", "EC", "SV", "HN", "JM"]
        if country.upper() not in valid_countries:
            raise ValueError(
                f"Código de país inválido: {country}. "
                f"Valores válidos: {', '.join(valid_countries)}"
            )

    def get(self, key: str, default: Any = None) -> Any:
        """
        Obtiene un valor de la configuración usando notación de punto.

        Args:
            key: Clave de configuración (e.g., "filters.start_date").
            default: Valor por defecto si la clave no existe.

        Returns:
            El valor de la configuración o el valor por defecto.
        """
        return OmegaConf.select(self.config, key, default=default)

    def get_spark_config(self) -> dict:
        """
        Obtiene la configuración de Spark como diccionario.

        Returns:
            dict: Configuración de Spark.
        """
        return OmegaConf.to_container(self.config.spark.config, resolve=True)

    def get_valid_delivery_types(self) -> list:
        """
        Obtiene la lista de tipos de entrega válidos.

        Returns:
            list: Lista de tipos de entrega válidos (routine + bonus).
        """
        routine = list(self.config.transformations.delivery_types.routine)
        bonus = list(self.config.transformations.delivery_types.bonus)
        return routine + bonus

    def get_routine_delivery_types(self) -> list:
        """
        Obtiene la lista de tipos de entrega de rutina.

        Returns:
            list: Lista de tipos de entrega de rutina.
        """
        return list(self.config.transformations.delivery_types.routine)

    def get_bonus_delivery_types(self) -> list:
        """
        Obtiene la lista de tipos de entrega con bonificaciones.

        Returns:
            list: Lista de tipos de entrega con bonificaciones.
        """
        return list(self.config.transformations.delivery_types.bonus)

    def __repr__(self) -> str:
        """Representación string de la configuración."""
        return OmegaConf.to_yaml(self.config)


def parse_cli_arguments() -> argparse.Namespace:
    """
    Parsea los argumentos de línea de comandos.

    Returns:
        argparse.Namespace: Argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="ETL de Entregas de Productos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Procesar todo el rango de fechas por defecto
  python main.py --config config/config.yaml

  # Procesar un rango de fechas específico
  python main.py --config config/config.yaml --start-date 2025-01-01 --end-date 2025-03-31

  # Procesar solo un país
  python main.py --config config/config.yaml --country GT

  # Combinación de filtros
  python main.py --config config/config.yaml --start-date 2025-01-01 --end-date 2025-06-30 --country PE
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Ruta al archivo de configuración YAML",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Fecha de inicio del rango (formato: YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Fecha de fin del rango (formato: YYYY-MM-DD)",
    )

    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Código de país a procesar (GT, PE, EC, SV, HN, JM)",
    )

    return parser.parse_args()
