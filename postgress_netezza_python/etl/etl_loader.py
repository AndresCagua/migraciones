# Si tienes helpers, puedes importar de .utils
import configparser
import csv
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config_reader import ExcelTableConfigReader
from .netezza_connection import NetezzaConnection
from .postgres_connection import PostgresConnection

# Configuración de logging (igual que en migracion.py)
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

ALTERNATIVE_SEPARATORS = [
    "|",
    "ᛟ",
    "丿",
    "Δ",
    "‡",
]


class NetezzaETLLoader:
    """
    Orquesta el proceso ETL: extrae datos de PostgreSQL, los transforma y los carga en Netezza.
    """

    def __init__(
        self,
        target_table: str,
        excel_config_path: str,
        output_dir: str = "output",
        config_file: str = "config.ini",
    ):
        self.target_table = target_table
        self.netezza_schema = "ADMIN"
        self.output_dir = Path(output_dir)
        self.config_file = config_file

        self.upload_timestamp = datetime.now().replace(microsecond=0)
        self.inicio_carga = None  # Para guardar el timestamp de inicio
        self.excel_reader = ExcelTableConfigReader(excel_config_path)
        self.netezza_db = NetezzaConnection(config_file=self.config_file)
        self.postgres_db: Optional[PostgresConnection] = None

        self.raw_pg_file: Optional[Path] = None
        self.final_csv_file: Optional[Path] = None
        self.etl_config: Optional[Dict[str, Any]] = None

        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"NetezzaETLLoader inicializado para tabla Netezza '{self.target_table}'."
        )
        logger.info(f"Usando Excel de configuración: '{excel_config_path}'.")
        logger.info(f"Directorio de salida: '{self.output_dir}'.")

    def _conteo_base_origen(self):
        # Usa el mismo query de extracción, pero con COUNT(*)
        # query = ""
        query = self.etl_config["query_extracion"]
        clean_query = query[:-1] if query.endswith(";") else query
        count_query = f"SELECT COUNT(*) FROM ({clean_query}) AS subq"
        logger.info(f"Ejecutando conteo de registros en PostgreSQL: {count_query}")
        # Usa PostgresConnection para ejecutar el conteo
        self.postgres_db = PostgresConnection(
            schema=self.etl_config["esquema_postgres"], config_file=self.config_file
        )
        self.postgres_db.connect()
        self.postgres_db.cursor.execute(count_query)
        result = self.postgres_db.cursor.fetchone()
        self.postgres_db.close()
        return result[0] if result else 0

    def _conteo_archivo(self):
        # Cuenta las líneas del archivo CSV final, menos la cabecera
        if not self.final_csv_file or not self.final_csv_file.exists():
            return 0
        with open(self.final_csv_file, "r", encoding="utf-8") as f:
            return sum(1 for _ in f) - 1  # menos la cabecera

    def _conteo_base_destino(self):
        # Usa el timestamp de la carga actual
        upload_col = "UPLOAD_DATE"
        query = f"""
            SELECT COUNT(*) FROM "{self.netezza_schema}"."{self.target_table}"
            WHERE {upload_col} = '{self.upload_timestamp}'
        """
        result = self.netezza_db.execute_query(query)
        return result[0][0] if result else 0

    def _bitacora_insert_inicio(self):
        config_path = Path(self.config_file)
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
        database_name = parser.get("netezza", "database")
        self.inicio_carga = datetime.now().replace(microsecond=0)
        sql = f"""
        INSERT INTO "{database_name}"."{self.netezza_schema}"."DWH_BITACORA_CARGA_MIGRACION"
        (INICIO_CARGA, CARGADO, ESTADO, OBSERVACION)
        VALUES ('{self.inicio_carga}', 2, 'PASO 1', 'Paso 1: Extrayendo datos desde PostgreSQL')
        """
        logger.info(f"Insertando registro de inicio en bitácora, con el query {sql}")
        self.netezza_db.execute_command(sql)

    def _bitacora_update(self, **kwargs):
        config_path = Path(self.config_file)
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
        database_name = parser.get("netezza", "database")

        import datetime

        # kwargs: FIN_CARGA, CARGADO, ESTADO, OBSERVACION, CONTEO_BASE_ORIGEN, etc.
        set_clauses = []
        for k, v in kwargs.items():
            if isinstance(v, str):
                safe_v = v.replace("'", "''")  # Escapa comillas simples para SQL
                set_clauses.append(f"{k} = '{safe_v}'")
            elif v is None:
                set_clauses.append(f"{k} = NULL")
            elif isinstance(v, (datetime.datetime, datetime.date)):
                set_clauses.append(f"{k} = '{v}'")  # <-- Aquí, comillas simples
            else:
                set_clauses.append(f"{k} = {v}")
        set_sql = ", ".join(set_clauses)
        sql = f"""
        UPDATE "{database_name}"."{self.netezza_schema}"."DWH_BITACORA_CARGA_MIGRACION"
        SET {set_sql}
        WHERE INICIO_CARGA = '{self.inicio_carga}'
        """
        logger.info(f"Actualizando registro de bitácora con el query {sql}..")
        self.netezza_db.execute_command(sql)

    def _drop_external_table_if_exists(self) -> bool:
        """
        Borra la tabla externa (_ext) si existe, usando el esquema y nombre de tabla dinámicamente.
        """
        config_path = Path(self.config_file)
        parser = configparser.ConfigParser()
        parser.read(config_path)
        db_name = parser.get("netezza", "database", fallback="SYSTEM").upper()
        schema = self.netezza_schema.upper()
        table_ext = f"{self.target_table.upper()}_ext"
        table_3part = f'"{db_name}"."{schema}"."{table_ext}"'

        check_table_exists_sql = f"""
        SELECT COUNT(*) FROM _V_TABLE 
        WHERE OBJTYPE = 'EXTERNAL TABLE' AND SCHEMA = '{schema}' AND TABLENAME = '{table_ext}'
        """
        logger.info(
            f"Verificando existencia de tabla externa {table_3part} antes de crearla con el query {check_table_exists_sql}..."
        )

        result = self.netezza_db.execute_query(check_table_exists_sql)
        table_exists = result and result[0][0] > 0
        if table_exists:
            logger.info(
                f"Eliminando tabla externa existente {table_3part} antes de crearla..."
            )
            drop_sql = f"DROP TABLE {table_3part};"
            return self.netezza_db.execute_command(drop_sql)
        return True  # No existe, no hay nada que borrar

    def get_etl_config_from_netezza(self) -> bool:
        """Obtiene la configuración de extracción (esquema PG, query PG) desde una tabla en Netezza."""
        query = f"""
        SELECT esquema_postgres, query_extracion
        FROM {self.netezza_schema}.config_etl_cargas
        WHERE nombre_tabla = '{self.target_table}' AND activo = TRUE
        """
        logger.info(
            f"Obteniendo configuración ETL de Netezza para tabla '{self.target_table}'..."
        )
        result = self.netezza_db.execute_query(query)
        if not result or len(result) == 0:
            logger.error(
                f"No se encontró configuración ETL activa para la tabla '{self.target_table}' en '{self.netezza_schema}.config_etl_cargas'."
            )
            return False
        self.etl_config = {
            "esquema_postgres": result[0][0],
            "query_extracion": result[0][1],
        }
        logger.info(
            f"Configuración ETL obtenida: Esquema PG='{self.etl_config['esquema_postgres']}', Query PG (parcial)='{self.etl_config['query_extracion'][:100]}...'"
        )
        return True

    def _determine_csv_separator(self, file_path: Path) -> Optional[str]:
        """Determina un separador adecuado para el CSV final que no esté en una muestra de los datos."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                sample = f.read(8192)
            for sep in ALTERNATIVE_SEPARATORS:
                if sep not in sample:
                    logger.info(
                        f"Separador seleccionado para CSV final: '{sep}' (no encontrado en la muestra de '{file_path}')"
                    )
                    return sep
            logger.error(
                f"No se pudo encontrar un separador adecuado de ALTERNATIVE_SEPARATORS en la muestra de '{file_path}'. Todos están presentes."
            )
            return None
        except Exception as e:
            logger.error(
                f"Error al determinar separador para '{file_path}': {e}", exc_info=True
            )
            return None

    def _convert_raw_to_final_csv(
        self, raw_file: Path, final_file: Path, final_separator: str
    ) -> bool:
        """Convierte el archivo raw (delimitado por tabs) al formato CSV final con el separador elegido."""
        try:
            with (
                open(raw_file, "r", encoding="utf-8", newline="") as fin,
                open(final_file, "w", encoding="utf-8", newline="") as fout,
            ):
                reader = csv.reader(fin, delimiter="\t")
                writer = csv.writer(fout, delimiter=final_separator)
                count = 0
                for row in reader:
                    writer.writerow(row)
                    count += 1
            logger.info(
                f"Archivo raw '{raw_file}' convertido a CSV final '{final_file}' con separador '{final_separator}'. {count} filas procesadas."
            )
            return True
        except Exception as e:
            logger.error(f"Error al convertir raw CSV a final CSV: {e}", exc_info=True)
            return False

    def extract_data_from_postgres(self) -> bool:
        if not self.get_etl_config_from_netezza():
            return False
        assert self.etl_config is not None, "etl_config no debería ser None aquí."
        self.postgres_db = PostgresConnection(
            schema=self.etl_config["esquema_postgres"], config_file=self.config_file
        )
        temp_file_obj_raw_pg = tempfile.NamedTemporaryFile(
            mode="w+", delete=False, encoding="utf-8", suffix="_pg_raw.tmp", newline=""
        )
        self.raw_pg_file = Path(temp_file_obj_raw_pg.name)
        temp_file_obj_raw_pg.close()
        logger.info(
            f"Archivo temporal para datos crudos de PostgreSQL: '{self.raw_pg_file}'"
        )
        if not self.postgres_db.execute_query_to_csv(
            self.etl_config["query_extracion"],
            str(self.raw_pg_file),
            "\t",
        ):
            logger.error(
                "Fallo en la extracción de datos (execute_query_to_csv) desde PostgreSQL."
            )
            return False
        final_csv_separator = self._determine_csv_separator(self.raw_pg_file)
        if not final_csv_separator:
            logger.error(
                "No se pudo determinar un separador para el archivo CSV final."
            )
            return False
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.final_csv_file = self.output_dir / f"{self.target_table}_{timestamp}.csv"
        if not self._convert_raw_to_final_csv(
            self.raw_pg_file, self.final_csv_file, final_csv_separator
        ):
            return False
        logger.info(
            f"Datos de PostgreSQL extraídos y guardados en CSV final: '{self.final_csv_file}'"
        )
        return True

    def generate_tmp_table_script(self) -> Optional[str]:
        """Genera el script SQL para crear la tabla temporal en Netezza, basado en el Excel."""
        table_config_excel = self.excel_reader.get_table_config(self.target_table)
        if not table_config_excel:
            logger.error(
                f"No se encontró configuración para la tabla '{self.target_table}' en el Excel. No se puede generar script SQL."
            )
            return None
        distribute_col = None
        column_defs_sql = []
        for col_excel in table_config_excel:
            col_name = col_excel.get("COLUMNAS")
            if col_name.upper() == "UPLOAD_DATE":
                continue  # No incluir columna de fecha de carga en la tabla temporal
            col_type = col_excel.get("TIPO")
            nullable_val = str(col_excel.get("NULLABLE", "YES")).upper()
            not_null_clause = "NOT NULL" if nullable_val in ["NO", "N", "FALSE"] else ""
            if not col_name or not col_type:
                logger.error(
                    f"Definición de columna incompleta en Excel para tabla '{self.target_table}': falta 'COLUMNAS' o 'TIPO'. Col: {col_excel}"
                )
                return None
            column_defs_sql.append(
                f'    "{col_name}" {col_type} {not_null_clause}'.strip()
            )
            distribute_marker = str(col_excel.get("DISTRIBUTE", "")).upper()
            if distribute_marker in ["X", "YES", "TRUE"]:
                if distribute_col:
                    logger.warning(
                        f"Múltiples columnas marcadas para DISTRIBUTE ON para tabla '{self.target_table}'. Usando la primera: '{distribute_col}'."
                    )
                else:
                    distribute_col = f'"{col_name}"'
        if not column_defs_sql:
            logger.error(
                f"No se pudieron generar definiciones de columna para tabla temporal '{self.target_table}_tmp'."
            )
            return None
        tmp_table_name = f'"{self.netezza_schema}"."{self.target_table}_tmp"'
        script_lines = [
            f"-- Script generado desde Excel para {tmp_table_name}",
            f"DROP TABLE {tmp_table_name} IF EXISTS;",
            f"CREATE TABLE {tmp_table_name}",
            "(",
            ",\n".join(column_defs_sql),
            ")",
        ]
        if distribute_col:
            script_lines.append(f"DISTRIBUTE ON ({distribute_col});")
        else:
            script_lines.append("DISTRIBUTE ON RANDOM;")
        full_script = "\n".join(script_lines)
        logger.debug(
            f"Script SQL para tabla temporal '{tmp_table_name}' generado:\n{full_script}"
        )
        return full_script

    def update_production_table(self) -> bool:
        """Asegura que la tabla de producción en Netezza exista y tenga todas las columnas del Excel."""
        logger.info(
            f"Verificando/Actualizando estructura de tabla de producción Netezza: '{self.netezza_schema}.{self.target_table}'..."
        )
        table_config_excel = self.excel_reader.get_table_config(self.target_table)
        if not table_config_excel:
            logger.error(
                f"No hay configuración en Excel para '{self.target_table}', no se puede actualizar/crear."
            )
            return False
        prod_table_fqn = f'"{self.netezza_schema}"."{self.target_table}"'
        check_table_exists_sql = f"""
        SELECT COUNT(*) FROM _V_TABLE 
        WHERE SCHEMA = '{self.netezza_schema.upper()}' AND TABLENAME = '{self.target_table.upper()}'
        """
        result = self.netezza_db.execute_query(check_table_exists_sql)
        table_exists = result and result[0][0] > 0
        if not table_exists:
            logger.info(
                f"Tabla de producción {prod_table_fqn} no existe. Intentando crearla..."
            )
            distribute_col_prod = None
            column_defs_prod_sql = []
            for col_excel in table_config_excel:
                col_name = col_excel["COLUMNAS"]
                col_type = col_excel["TIPO"]
                not_null_clause = (
                    "NOT NULL"
                    if str(col_excel.get("NULLABLE", "YES")).upper()
                    in ["NO", "N", "FALSE"]
                    else ""
                )
                column_defs_prod_sql.append(
                    f'    "{col_name}" {col_type} {not_null_clause}'.strip()
                )
                if (
                    str(col_excel.get("DISTRIBUTE", "")).upper() in ["X", "YES", "TRUE"]
                    and not distribute_col_prod
                ):
                    distribute_col_prod = f'"{col_name}"'
            create_prod_sql_lines = [
                f"CREATE TABLE {prod_table_fqn}",
                "(",
                ",\n".join(column_defs_prod_sql),
                ")",
            ]
            create_prod_sql_lines.append(
                f"DISTRIBUTE ON ({distribute_col_prod});"
                if distribute_col_prod
                else "DISTRIBUTE ON RANDOM;"
            )
            create_prod_sql = "\n".join(create_prod_sql_lines)
            if self.netezza_db.execute_command(create_prod_sql):
                logger.info(
                    f"Tabla de producción {prod_table_fqn} creada exitosamente."
                )
            else:
                logger.error(f"Fallo al crear tabla de producción {prod_table_fqn}.")
                return False
        else:
            logger.info(
                f"Tabla de producción {prod_table_fqn} existe. Verificando columnas..."
            )
            current_columns_db = self._get_netezza_table_columns(
                self.netezza_schema, self.target_table
            )
            if current_columns_db is None:
                return False
            current_columns_db_set = {col.upper() for col in current_columns_db}
            for col_excel in table_config_excel:
                col_name_excel = col_excel["COLUMNAS"]
                if col_name_excel.upper() not in current_columns_db_set:
                    col_type_excel = col_excel["TIPO"]
                    not_null_clause = (
                        "NOT NULL"
                        if str(col_excel.get("NULLABLE", "YES")).upper()
                        in ["NO", "N", "FALSE"]
                        else ""
                    )
                    if not_null_clause:
                        logger.warning(
                            f"Intentando añadir columna '{col_name_excel}' como NOT NULL a tabla existente {prod_table_fqn}. Esto podría fallar si la tabla tiene datos y no hay DEFAULT."
                        )
                    alter_sql = f'ALTER TABLE {prod_table_fqn} ADD COLUMN "{col_name_excel}" {col_type_excel} {not_null_clause}'.strip()
                    if self.netezza_db.execute_command(alter_sql):
                        logger.info(
                            f"Columna '{col_name_excel}' agregada a tabla de producción {prod_table_fqn}."
                        )
                    else:
                        logger.error(
                            f"Fallo al agregar columna '{col_name_excel}' a {prod_table_fqn}."
                        )
                        return False
        return True

    def _get_netezza_table_columns(
        self, schema: str, table: str
    ) -> Optional[List[str]]:
        """Obtiene las columnas actuales de una tabla en Netezza."""
        sql = f"""
        SELECT ATTNAME 
        FROM _V_RELATION_COLUMN
        WHERE SCHEMA = '{schema.upper()}' AND NAME = '{table.upper()}'
        ORDER BY ATTNUM;
        """
        result = self.netezza_db.execute_query(sql)
        if result is None:
            logger.error(
                f"No se pudieron obtener columnas para {schema}.{table} desde Netezza."
            )
            return None
        return [row[0] for row in result]

    def create_tmp_table(self, script_sql_create_tmp: str) -> bool:
        """Crea la tabla temporal en Netezza."""
        tmp_table_name = f'"{self.netezza_schema}"."{self.target_table}_tmp"'
        logger.info(f"Creando tabla temporal {tmp_table_name} en Netezza...")
        return self.netezza_db.execute_command(script_sql_create_tmp)

    def create_external_table(self) -> bool:
        """
        Crea una tabla externa en Netezza apuntando al archivo CSV generado,
        usando remotesource 'python' para compatibilidad con nzpy.
        """
        table_config = self.excel_reader.get_table_config(self.target_table)
        if not table_config or not self.final_csv_file:
            logger.error(
                "No se pudo crear tabla externa: falta configuración o CSV final."
            )
            return False
        config_path = Path(self.config_file)
        parser = configparser.ConfigParser()
        parser.read(config_path)
        db_name = parser.get("netezza", "database", fallback="SYSTEM").upper()
        schema = self.netezza_schema.upper()
        table_ext = f"{self.target_table.upper()}_ext"
        table_3part = f'"{db_name}"."{schema}"."{table_ext}"'
        column_defs = []
        for col in table_config:
            col_name = col.get("COLUMNAS")
            if col_name.upper() == "UPLOAD_DATE":
                continue
            col_type = col.get("TIPO")
            if not col_name or not col_type:
                logger.error(f"Columna inválida en configuración Excel: {col}")
                return False
            column_defs.append(f'"{col_name}" {col_type}')
        try:
            with open(self.final_csv_file, "r", encoding="utf-8") as f:
                first_line = f.readline()
                separator = next(
                    (s for s in ALTERNATIVE_SEPARATORS if s in first_line), None
                )
        except Exception as e:
            logger.error(f"No se pudo abrir el archivo CSV final: {e}")
            return False
        if not separator:
            logger.error("No se pudo detectar separador válido en el archivo CSV.")
            return False
        ruta_csv_netezza = str(self.final_csv_file).replace("\\", "/")

        if not self._drop_external_table_if_exists():
            logger.error(
                f"No se pudo eliminar la tabla externa {table_3part} existente antes de crearla."
            )
            return False

        create_sql = f"""
        CREATE EXTERNAL TABLE {table_3part} (
            {", ".join(column_defs)}
        )
        USING (
            DATAOBJECT ('{ruta_csv_netezza}')
            DELIMITER '{separator}'
            REMOTESOURCE 'python'
            ENCODING 'internal'
            CTRLCHARS 'yes'
            MAXERRORS 10
            LOGDIR '/tmp'
        );
        """.strip()
        logger.info(
            f"Creando tabla externa en Netezza con remotesource 'python': {table_3part}"
        )
        return self.netezza_db.execute_command(create_sql)

    def load_data_from_external_to_tmp(self) -> bool:
        """
        Inserta los datos desde la tabla externa (_ext) hacia la tabla temporal (_tmp).
        """
        try:
            config_path = Path(self.config_file)
            parser = configparser.ConfigParser()
            parser.read(config_path)
            db_name = parser.get("netezza", "database", fallback="SYSTEM").upper()
            table_ext_fqn = (
                f'"{db_name}"."{self.netezza_schema}"."{self.target_table}_ext"'
            )
            table_tmp_fqn = (
                f'"{db_name}"."{self.netezza_schema}"."{self.target_table}_tmp"'
            )
            insert_sql = f"""
            INSERT INTO {table_tmp_fqn}
            SELECT * FROM {table_ext_fqn};
            """
            logger.info(
                f"Insertando datos desde tabla externa {table_ext_fqn} a temporal {table_tmp_fqn}..."
            )
            return self.netezza_db.execute_command(insert_sql)
        except Exception as e:
            logger.error(
                f"Fallo al insertar desde la tabla externa a TMP: {e}", exc_info=True
            )
            return False

    def _get_merge_columns(
        self,
    ) -> Tuple[
        Optional[List[Dict[str, Any]]], Optional[List[str]], Optional[List[str]]
    ]:
        """
        Obtiene la config completa de columnas, nombres de columnas clave para MERGE, y todos los nombres de columnas.
        Clave de MERGE: Columna 'MERGE_KEY' en Excel marcada con 'X', 'PK', o 'YES'.
        """
        table_config_excel = self.excel_reader.get_table_config(self.target_table)
        if not table_config_excel:
            logger.error(
                f"No se encontró config Excel para tabla '{self.target_table}' para MERGE."
            )
            return None, None, None
        merge_key_columns = []
        all_columns_sql = []
        for col_def_excel in table_config_excel:
            col_name = col_def_excel.get("COLUMNAS")
            if not col_name:
                logger.error(
                    f"Definición de columna inválida en Excel para '{self.target_table}': falta 'COLUMNAS'. Config: {col_def_excel}"
                )
                return None, None, None
            all_columns_sql.append(f'"{col_name}"')
            merge_key_marker = str(col_def_excel.get("MERGE_KEY", "")).upper()
            if merge_key_marker in ["X", "PK", "YES", "TRUE"]:
                merge_key_columns.append(f'"{col_name}"')
        if not merge_key_columns:
            logger.warning(
                f"No se definieron MERGE_KEY para tabla '{self.target_table}'. No se puede realizar un MERGE estándar."
            )
        return (
            table_config_excel,
            merge_key_columns,
            all_columns_sql,
        )

    def _generate_merge_statement(self) -> Optional[str]:
        """Genera la sentencia SQL MERGE para Netezza."""
        _, merge_keys, all_cols = self._get_merge_columns()
        if merge_keys is None or all_cols is None:
            logger.error(
                f"No se pudo obtener la configuración de columnas/claves para MERGE de tabla '{self.target_table}'."
            )
            return None
        if not merge_keys:
            logger.error(
                f"No se especificaron columnas MERGE_KEY para tabla '{self.target_table}'. Sentencia MERGE no puede ser generada con condición ON."
            )
            return None

        # Asegúrate de que UPLOAD_DATE esté en las columnas
        upload_col = '"UPLOAD_DATE"'
        if upload_col not in all_cols:
            all_cols.append(upload_col)

        target_fqn = f'"{self.netezza_schema}"."{self.target_table}"'
        tmp_fqn = f'"{self.netezza_schema}"."{self.target_table}_tmp"'
        on_conditions = [f"TGT.{pk_col} = SRC.{pk_col}" for pk_col in merge_keys]
        on_clause = " AND ".join(on_conditions)

        # SET para UPDATE: todas menos las claves, pero incluye UPLOAD_DATE
        update_columns = [col_sql for col_sql in all_cols if col_sql not in merge_keys]
        set_clauses = []
        for col_sql in update_columns:
            if col_sql == upload_col:
                set_clauses.append(f"TGT.{upload_col} = '{self.upload_timestamp}'")
            else:
                set_clauses.append(f"TGT.{col_sql} = SRC.{col_sql}")

        # INSERT: agrega UPLOAD_DATE con el timestamp
        insert_cols = list(all_cols)
        insert_values = []
        for col_sql in insert_cols:
            if col_sql == upload_col:
                insert_values.append(f"'{self.upload_timestamp}'")
            else:
                insert_values.append(f"SRC.{col_sql}")

        config_path = Path(self.config_file)
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
        database_name = parser.get("netezza", "database")
        database_name = f'"{database_name}"'

        merge_sql = f"MERGE INTO {database_name}.{target_fqn} TGT\n"
        merge_sql += f"USING {database_name}.{tmp_fqn} SRC\n"
        merge_sql += f"ON ({on_clause})\n"
        if set_clauses:
            merge_sql += "WHEN MATCHED THEN\n"
            merge_sql += f"  UPDATE SET {', '.join(set_clauses)}\n"
        merge_sql += "WHEN NOT MATCHED THEN\n"
        merge_sql += f"  INSERT ({', '.join(insert_cols)})\n"
        merge_sql += f"  VALUES ({', '.join(insert_values)});"

        logger.info(f"Sentencia MERGE generada para '{self.target_table}'.")
        logger.debug(f"SQL MERGE:\n{merge_sql}")

        # Opcional: GROOM TABLE después del MERGE
        groom_table = f'GROOM TABLE {database_name}."{self.netezza_schema}"."{self.target_table}";'
        self.netezza_db.execute_command(groom_table)
        return merge_sql

    def execute_merge_to_production(self) -> bool:
        """Ejecuta la sentencia MERGE para actualizar la tabla de producción Netezza."""
        logger.info(
            f"Iniciando MERGE para tabla Netezza '{self.netezza_schema}.{self.target_table}' desde tabla temporal."
        )
        merge_sql = self._generate_merge_statement()
        if not merge_sql:
            logger.error(
                "Fallo al generar sentencia MERGE. Abortando operación de merge."
            )
            return False
        logger.info(f"Ejecutando MERGE en Netezza para tabla '{self.target_table}'.")

        if self.netezza_db.execute_command(merge_sql):
            # Guarda el rowcount del cursor si está disponible
            if self.netezza_db.cursor and hasattr(self.netezza_db.cursor, "rowcount"):
                self.merge_rowcount = self.netezza_db.cursor.rowcount
            else:
                self.merge_rowcount = 0
            logger.info(
                f"MERGE en Netezza completado exitosamente para tabla '{self.target_table}'."
            )
            return True

        else:
            logger.error(
                f"Error al ejecutar MERGE en Netezza para tabla '{self.target_table}'."
            )
            return False

    def run(self) -> bool:
        """Ejecuta el proceso ETL completo."""
        tmp_table_created = False
        try:
            logger.info(
                f"--- INICIO DEL PROCESO ETL PARA TABLA DESTINO NETEZZA: {self.netezza_schema}.{self.target_table} ---"
            )
            # Se inserta el inicio de carga en la bitácora
            self._bitacora_insert_inicio()
            logger.info(
                "PASO 0: Verificando/Actualizando estructura de tabla de PRODUCCIÓN Netezza..."
            )
            if not self.update_production_table():
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo crítico al verificar/actualizar la tabla de producción Netezza. No se puede continuar.",
                )
                logger.error(
                    "Fallo crítico al verificar/actualizar la tabla de producción Netezza. No se puede continuar."
                )
                return False
            logger.info(
                "Estructura de tabla de PRODUCCIÓN Netezza verificada/actualizada."
            )
            # Se actualiza la bitácora con el estado inicial
            self._bitacora_update(
                ESTADO="PASO 1",
                OBSERVACION="Paso 1: Extrayendo datos desde PostgreSQL...",
            )
            logger.info("PASO 1: Extrayendo datos desde PostgreSQL...")
            if not self.extract_data_from_postgres():
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo en la extracción de datos desde PostgreSQL.",
                )
                logger.error("Fallo en la extracción de datos desde PostgreSQL.")
                return False
            # Se actualiza la bitácora con el estado de la carga
            self._bitacora_update(
                ESTADO="PASO 2",
                OBSERVACION="Paso 2: Generando script SQL para tabla TEMPORAL Netezza...",
            )
            logger.info("PASO 2: Generando script SQL para tabla TEMPORAL Netezza...")
            script_sql_create_tmp = self.generate_tmp_table_script()
            if not script_sql_create_tmp:
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo al generar el script SQL para la tabla TEMPORAL Netezza.",
                )
                logger.error(
                    "Fallo al generar el script SQL para la tabla TEMPORAL Netezza."
                )
                return False
            # Se actualiza la bitácora con el estado de la carga
            self._bitacora_update(
                ESTADO="PASO 3",
                OBSERVACION="Paso 3: Creando tabla TEMPORAL en Netezza...",
            )
            logger.info("PASO 3: Creando tabla TEMPORAL en Netezza...")
            if not self.create_tmp_table(script_sql_create_tmp):
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo al crear la tabla TEMPORAL Netezza.",
                )
                logger.error("Fallo al crear la tabla TEMPORAL Netezza.")
                return False
            tmp_table_created = True
            # Se actualiza la bitácora con el estado de la carga
            self._bitacora_update(
                ESTADO="PASO 4",
                OBSERVACION="Paso 4: Creando tabla EXTERNA en Netezza apuntando al CSV...",
            )
            logger.info("PASO 4: Creando tabla EXTERNA en Netezza apuntando al CSV...")
            if not self.create_external_table():
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo al crear la tabla EXTERNA Netezza.",
                )
                logger.error("Fallo al crear la tabla EXTERNA Netezza.")
                return False
            # Se actualiza la bitácora con el estado de la carga
            self._bitacora_update(
                ESTADO="PASO 5",
                OBSERVACION="Paso 5: Cargando datos desde la tabla EXTERNA hacia la tabla TEMPORAL...",
            )
            logger.info(
                "PASO 5: Cargando datos desde la tabla EXTERNA hacia la tabla TEMPORAL..."
            )
            if not self.load_data_from_external_to_tmp():
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo al cargar datos desde la tabla EXTERNA hacia la tabla TEMPORAL.",
                )
                logger.error("Fallo al insertar desde tabla EXTERNA hacia TMP.")
                return False
            # Se actualiza la bitácora con el estado de la carga
            self._bitacora_update(
                ESTADO="PASO 6",
                OBSERVACION="Paso 6: Ejecutando MERGE hacia la tabla de PRODUCCIÓN Netezza...",
            )
            logger.info(
                "PASO 6: Ejecutando MERGE hacia la tabla de PRODUCCIÓN Netezza..."
            )
            if not self.execute_merge_to_production():
                self._bitacora_update(
                    CARGADO=1,
                    ESTADO="ERROR",
                    OBSERVACION="Fallo durante la operación MERGE a la tabla de PRODUCCIÓN Netezza.",
                )
                logger.error(
                    "Fallo durante la operación MERGE a la tabla de PRODUCCIÓN Netezza."
                )
                return False
            logger.info(
                f"--- PROCESO ETL PARA TABLA {self.netezza_schema}.{self.target_table} COMPLETADO EXITOSAMENTE ---"
            )

            # Obtén los conteos:
            conteo_origen = self._conteo_base_origen()
            conteo_archivo = self._conteo_archivo()
            conteo_destino = self._conteo_base_destino()

            # Validación de conteos
            if conteo_origen != conteo_archivo or conteo_archivo != conteo_destino:
                self._bitacora_update(
                    CARGADO=2,
                    ESTADO="ERROR",
                    OBSERVACION=f"Error en la validación de los conteos. Origen: {conteo_origen}, Archivo: {conteo_archivo}, Destino: {conteo_destino}",
                    CONTEO_BASE_ORIGEN=conteo_origen,
                    CONTEO_ARCHIVO=conteo_archivo,
                    CONTEO_BASE_DESTINO=conteo_destino,
                    FIN_CARGA=datetime.now().replace(microsecond=0),
                )
                return False

            # Si todo OK:
            self._bitacora_update(
                FIN_CARGA=datetime.now().replace(microsecond=0),
                CARGADO=0,
                ESTADO="OK",
                CONTEO_BASE_ORIGEN=conteo_origen,
                CONTEO_ARCHIVO=conteo_archivo,
                CONTEO_BASE_DESTINO=conteo_destino,
                OBSERVACION="El proceso de migración finalizó correctamente.",
            )

            return True
        except Exception as e:
            logger.error(
                f"Error catastrófico en NetezzaETLLoader.run: {e}", exc_info=True
            )
            return False
        finally:
            if tmp_table_created:
                tmp_table_fqn = f'"{self.netezza_schema}"."{self.target_table}_tmp"'
                drop_tmp_sql = f"DROP TABLE {tmp_table_fqn} IF EXISTS;"
                logger.info(
                    f"Limpiando tabla temporal Netezza: Ejecutando '{drop_tmp_sql}'"
                )
                if self.netezza_db.execute_command(drop_tmp_sql):
                    logger.info(f"Tabla temporal Netezza {tmp_table_fqn} eliminada.")
                else:
                    logger.warning(
                        f"No se pudo eliminar la tabla temporal Netezza {tmp_table_fqn}. Podría requerir limpieza manual."
                    )
            if self.netezza_db and self.netezza_db.conn:
                self.netezza_db.close()
            if self.postgres_db and self.postgres_db.conn:
                self.postgres_db.close()
            if self.raw_pg_file and self.raw_pg_file.exists():
                try:
                    os.remove(self.raw_pg_file)
                    logger.info(
                        f"Archivo temporal de datos crudos de PostgreSQL '{self.raw_pg_file}' eliminado."
                    )
                except OSError as e_os:
                    logger.warning(
                        f"No se pudo eliminar el archivo temporal '{self.raw_pg_file}': {e_os}"
                    )
            # Si quieres borrar el CSV final, descomenta aquí
            # if self.final_csv_file and self.final_csv_file.exists():
            #     try:
            #         os.remove(self.final_csv_file)
            #         logger.info(f"Archivo CSV final '{self.final_csv_file}' eliminado.")
            #     except OSError as e_os:
            #         logger.warning(f"No se pudo eliminar el archivo CSV final '{self.final_csv_file}': {e_os}")
            logger.info(f"--- FIN DEL PROCESO ETL (run) PARA {self.target_table} ---")
