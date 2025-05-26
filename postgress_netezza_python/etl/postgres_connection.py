import configparser
import csv
import logging
from pathlib import Path
from typing import Optional

import psycopg2

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


class PostgresConnection:
    """Conexión a PostgreSQL para extracción de datos."""

    def __init__(self, schema: str, config_file="config.ini"):
        pg_settings = self._load_pg_config(config_file)
        self.config = {
            "host": pg_settings["host"],
            "port": pg_settings["port"],
            "database": pg_settings["database"],
            "user": pg_settings["user"],
            "password": pg_settings["password"],
            "options": f"-c search_path={schema},{pg_settings.get('search_path_default', '$user,public')}",
        }
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extensions.cursor] = None
        logger.info(
            f"PostgresConnection inicializado para esquema '{schema}' y config '{config_file}'"
        )

    def _load_pg_config(self, config_file: str) -> dict:
        config_path = Path(config_file)
        if not config_path.exists():
            logger.error(
                f"Archivo de configuración PostgreSQL no encontrado: {config_path}"
            )
            raise FileNotFoundError(
                f"Archivo de configuración no encontrado: {config_path}"
            )
        parser = configparser.ConfigParser()
        defaults = {
            "host": "localhost",
            "port": "5432",
            "database": "postgres",
            "user": "default_user",
            "password": "default_password",
            "search_path_default": "$user,public",
        }
        parser.read_dict({"postgresql": defaults})
        parser.read(config_path)
        return {
            "host": parser.get("postgresql", "host"),
            "port": parser.getint("postgresql", "port"),
            "database": parser.get("postgresql", "database"),
            "user": parser.get("postgresql", "user"),
            "password": parser.get("postgresql", "password"),
            "search_path_default": parser.get(
                "postgresql", "search_path_default", fallback="$user,public"
            ),
        }

    def connect(self) -> bool:
        if self.conn and not self.conn.closed:
            return True
        try:
            logger.info(
                f"Conectando a PostgreSQL: host={self.config['host']}, db={self.config['database']}, user={self.config['user']}"
            )
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
            logger.info("Conexión a PostgreSQL establecida exitosamente.")
            return True
        except Exception as e:
            logger.error(f"Error al conectar a PostgreSQL: {e}", exc_info=True)
            self.conn = None
            self.cursor = None
            return False

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.conn:
            if not self.conn.closed:
                self.conn.close()
            self.conn = None
        logger.info("Conexión a PostgreSQL cerrada.")

    def execute_query_to_csv(
        self, query: str, output_file: str, separator: str
    ) -> bool:
        if not self.connect():
            return False
        assert self.cursor is not None, "Cursor no inicializado después de conectar"
        try:
            logger.info(
                f"Ejecutando query en PostgreSQL (primeros 100 chars): {query[:100]}..."
            )
            self.cursor.execute(query)
            column_names = [desc[0] for desc in self.cursor.description or []]
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=separator)
                writer.writerow(column_names)
                fetch_count = 0
                while True:
                    rows = self.cursor.fetchmany(1000)
                    if not rows:
                        break
                    writer.writerows(rows)
                    fetch_count += len(rows)
                logger.info(
                    f"Datos de PostgreSQL ({fetch_count} filas) exportados a '{output_file}' con separador '{separator}'."
                )
            return True
        except Exception as e:
            logger.error(
                f"Error al exportar datos desde PostgreSQL: {e}", exc_info=True
            )
            return False
        finally:
            self.close()
