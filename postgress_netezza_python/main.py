import argparse
import logging
import sys

from pathlib import Path
from etl.etl_loader import NetezzaETLLoader

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


def main():
    parser = argparse.ArgumentParser(
        description="Extrae datos de PostgreSQL, los transforma y los carga/actualiza en Netezza usando MERGE.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "netezza_target_table",
        help="Nombre de la tabla destino en Netezza (debe coincidir con un nombre de hoja en el archivo Excel).",
    )
    parser.add_argument(
        "excel_config_path",
        help="Ruta al archivo Excel que contiene la configuración de las tablas y columnas.",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        default="output",
        help='Directorio para archivos CSV intermedios y finales (default: "output").',
    )
    parser.add_argument(
        "-c",
        "--config_file",
        default="config.ini",
        help='Ruta al archivo de configuración .ini para las conexiones de base de datos (default: "config.ini").',
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Modo verboso (activa logging DEBUG para la aplicación).",
    )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logging.getLogger("nzpy").setLevel(logging.DEBUG)
        logging.getLogger("psycopg2").setLevel(logging.INFO)
        logger.info("Modo verboso activado.")

    if not Path(args.config_file).exists():
        print(
            f"Error: El archivo de configuración de base de datos '{args.config_file}' no fue encontrado."
        )
        sys.exit(2)
    if not Path(args.excel_config_path).exists():
        print(
            f"Error: El archivo de configuración Excel '{args.excel_config_path}' no fue encontrado."
        )
        sys.exit(2)

    try:
        loader = NetezzaETLLoader(
            target_table=args.netezza_target_table,
            excel_config_path=args.excel_config_path,
            output_dir=args.output_dir,
            config_file=args.config_file,
        )
        success = loader.run()
        sys.exit(0 if success else 1)
    except FileNotFoundError as fnf_err:
        logger.error(f"Error de archivo de configuración: {fnf_err}")
        print(f"Error: {fnf_err}")
        sys.exit(2)
    except Exception as e_main:
        logger.critical(f"Excepción no controlada en main(): {e_main}", exc_info=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
