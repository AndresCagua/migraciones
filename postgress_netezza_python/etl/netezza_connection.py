import configparser
import logging
from pathlib import Path
from typing import List, Optional, Tuple

import nzpy

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


class NetezzaConnection:
    """Conexión a Netezza."""

    def __init__(self, config_file="config.ini"):
        self.config = self._load_config(config_file)
        self.conn: Optional[nzpy.core.Connection] = None
        self.cursor: Optional[nzpy.core.Cursor] = None
        logger.info(f"NetezzaConnection inicializado con config '{config_file}'")

    def _load_config(self, config_file):
        config_path = Path(config_file)
        if not config_path.exists():
            logger.error(
                f"Archivo de configuración Netezza no encontrado: {config_path}"
            )
            raise FileNotFoundError(
                f"Archivo de configuración no encontrado: {config_path}"
            )
        parser = configparser.ConfigParser()
        defaults = {
            "host": "localhost",
            "port": "5480",
            "database": "system",
            "user": "admin",
            "password": "password",
            "securityLevel": "1",
            "ssl": "prefer",
        }
        parser.read_dict({"netezza": defaults})
        parser.read(config_path)
        return {
            "host": parser.get("netezza", "host"),
            "port": parser.getint("netezza", "port"),
            "database": parser.get("netezza", "database"),
            "user": parser.get("netezza", "user"),
            "password": parser.get("netezza", "password"),
            "securityLevel": parser.getint("netezza", "securityLevel"),
            "ssl": parser.get("netezza", "ssl"),
        }

    def connect(self) -> bool:
        if self.conn:
            return True
        try:
            logger.info(
                f"Conectando a Netezza: host={self.config['host']}, db={self.config['database']}, user={self.config['user']}"
            )
            self.conn = nzpy.connect(
                **self.config, logLevel=logging.INFO, logOptions=nzpy.LogOptions.Inherit
            )
            self.cursor = self.conn.cursor()
            logger.info("Conexión a Netezza establecida exitosamente.")
            return True
        except Exception as e:
            logger.error(f"Error al conectar a Netezza: {e}", exc_info=True)
            self.conn = None
            self.cursor = None
            return False

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            self.conn.close()
            self.conn = None
        logger.info("Conexión a Netezza cerrada.")

    def execute_query(self, query: str) -> Optional[List[Tuple]]:
        if not self.connect():
            return None
        assert self.cursor is not None, "Cursor no inicializado"
        try:
            logger.debug(f"Netezza ejecutando consulta: {query[:200]}...")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            logger.debug(
                f"Consulta Netezza devolvió {len(results) if results else 0} filas."
            )
            return results
        except Exception as e:
            logger.error(
                f"Error al ejecutar consulta Netezza: {e}\nQuery: {query}",
                exc_info=True,
            )
            return None

    def execute_command(self, command: str) -> bool:
        if not self.connect():
            return False
        assert self.cursor is not None, "Cursor no inicializado"
        try:
            logger.info(f"Netezza ejecutando comando: {command[:200]}...")
            self.cursor.execute(command)
            if self.conn and hasattr(self.conn, "commit"):
                self.conn.commit()
            logger.info(
                f"Comando Netezza ejecutado exitosamente. Filas afectadas: {self.cursor.rowcount if self.cursor.rowcount != -1 else 'N/A'}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error al ejecutar comando Netezza: {e}\nComando: {command}",
                exc_info=True,
            )
            if self.conn and hasattr(self.conn, "rollback"):
                self.conn.rollback()
            return False
