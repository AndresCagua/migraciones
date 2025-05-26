import csv
import logging

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


def validar_csv(file_path, expected_columns, delimiter):
    with open(file_path, encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for i, row in enumerate(reader, 1):
            if len(row) != expected_columns:
                logger.warning(
                    f"Fila {i} tiene {len(row)} columnas, se esperaban {expected_columns}: {row}"
                )
