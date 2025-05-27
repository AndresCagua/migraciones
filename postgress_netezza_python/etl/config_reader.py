import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    filename="libraries.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("output.log", mode="w", encoding="utf-8")
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.propagate = False


class ExcelTableConfigReader:
    """Lee la configuración de tablas desde archivo Excel."""

    def __init__(self, excel_path: str):
        self.excel_path = Path(excel_path)
        self.sheets: Optional[Dict[str, List[Dict[str, Any]]]] = None
        if not self.excel_path.exists():
            logger.error(
                f"El archivo Excel de configuración no existe: {self.excel_path}"
            )
            raise FileNotFoundError(
                f"El archivo Excel de configuración no existe: {self.excel_path}"
            )
        self.load_config()

    def load_config(self) -> None:
        """Carga todas las hojas del Excel en un diccionario."""
        try:
            xls = pd.ExcelFile(self.excel_path)
            self.sheets = {}
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                for col in df.columns:
                    df[col] = df[col].astype(str).replace("nan", None)
                self.sheets[sheet_name] = df.to_dict("records")
            logger.info(
                f"Configuración cargada desde Excel: {self.excel_path} para hojas: {list(self.sheets.keys())}"
            )
        except Exception as e:
            logger.error(
                f"Error al leer archivo Excel '{self.excel_path}': {e}", exc_info=True
            )
            self.sheets = {}

    def get_table_config(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Obtiene la configuración para una tabla específica."""
        if self.sheets is None:
            logger.error("La configuración del Excel no pudo ser cargada previamente.")
            return None
        config = self.sheets.get(table_name)
        if config is None:
            logger.warning(
                f"No se encontró configuración para la tabla '{table_name}' en el Excel."
            )
        return config
