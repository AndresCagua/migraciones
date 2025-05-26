---
# Proceso de Migración de Datos: PostgreSQL → Netezza

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) robusto para migrar datos desde una base de datos PostgreSQL hacia Netezza, utilizando Python como orquestador y un archivo Excel para la configuración dinámica de tablas y columnas. El proceso está pensado para cargas incrementales y seguras, con validaciones y bitácora de auditoría.
---

## Arquitectura General

El flujo sigue la siguiente secuencia:

1. **Extracción**: Se consulta PostgreSQL usando un query configurable, extrayendo los datos a un archivo temporal.
2. **Transformación**: El archivo temporal se convierte a un CSV con un separador seguro, evitando colisiones con los datos.
3. **Carga**: El CSV se carga en Netezza a través de una tabla externa, luego a una tabla temporal, y finalmente se hace un MERGE a la tabla de producción.
4. **Bitácora y Validación**: Cada paso se registra en una tabla de bitácora y se validan los conteos de registros para asegurar la integridad.

---

## Componentes Principales

- **`main.py`**: Punto de entrada. Recibe argumentos CLI para tabla destino, Excel de configuración, directorio de salida y archivo .ini de conexiones.
- **`etl/etl_loader.py`**: Clase principal `NetezzaETLLoader` que orquesta todo el proceso ETL.
- **`example.ini`**: Archivo de configuración para conexiones a PostgreSQL y Netezza.
- **Excel de configuración**: Define la estructura de las tablas, tipos de datos, claves de merge, columnas distribuidas, etc.
- **SQL de ejemplo**: Scripts para crear y poblar tablas de prueba en PostgreSQL.

---

## Flujo Detallado del Proceso

### 1. Inicialización y Validaciones

- Se valida la existencia de los archivos de configuración y Excel.
- Se inicializa el logger para auditoría y debugging.

### 2. Bitácora de Inicio

- Se inserta un registro en la tabla de bitácora de Netezza (`DWH_BITACORA_CARGA_MIGRACION`) con el timestamp de inicio y el estado "PASO 1".

### 3. Verificación/Actualización de la Tabla de Producción

- Se consulta el Excel para obtener la definición de la tabla destino.
- Si la tabla no existe en Netezza, se crea con la estructura definida.
- Si existe, se agregan columnas faltantes según el Excel (con advertencia si son NOT NULL).

### 4. Extracción de Datos desde PostgreSQL

- Se obtiene el query de extracción y el esquema desde una tabla de configuración en Netezza.
- Se ejecuta el query en PostgreSQL y se exporta el resultado a un archivo temporal (delimitado por tabs).

### 5. Conversión a CSV Final

- Se analiza una muestra del archivo temporal para elegir un separador seguro (de una lista de caracteres poco comunes).
- Se convierte el archivo temporal a un CSV final usando el separador elegido.

### 6. Creación de Tabla Temporal en Netezza

- Se genera un script SQL para crear una tabla temporal (`_tmp`) en Netezza, basada en la definición del Excel (sin la columna `UPLOAD_DATE`).
- Se ejecuta el script para crear la tabla temporal.

### 7. Creación de Tabla Externa en Netezza

- Se crea una tabla externa (`_ext`) apuntando al CSV generado, usando el separador detectado y `remotesource 'python'` para compatibilidad.
- Si la tabla externa ya existe, se elimina antes de crearla.

### 8. Carga de Datos a la Tabla Temporal

- Se insertan los datos desde la tabla externa hacia la tabla temporal.

### 9. MERGE a la Tabla de Producción

- Se genera y ejecuta una sentencia MERGE dinámica:
  - Las claves de merge se definen en el Excel (`MERGE_KEY`).
  - Se actualizan los registros existentes y se insertan los nuevos, agregando la columna `UPLOAD_DATE` con el timestamp de carga.
- Se ejecuta un GROOM TABLE para optimizar la tabla después del merge.

### 10. Validación de Conteos

- Se comparan los conteos de registros:
  - En el origen (PostgreSQL, usando el query de extracción).
  - En el archivo CSV final.
  - En el destino (Netezza, filtrando por el timestamp de carga).
- Si hay discrepancias, se registra un error en la bitácora.

### 11. Limpieza y Cierre

- Se eliminan tablas temporales y archivos intermedios.
- Se cierra la conexión a las bases de datos.
- Se actualiza la bitácora con el estado final y los conteos.

---

## Ejemplo de Ejecución

```bash
python main.py pedidos path/configuracion.xlsx --output_dir output --config_file example.ini --verbose
```

- `pedidos`: Nombre de la tabla destino en Netezza (debe coincidir con una hoja en el Excel).
- `path/configuracion.xlsx`: Ruta al archivo Excel de configuración.
- `--output_dir`: Carpeta para archivos intermedios.
- `--config_file`: Archivo .ini con las credenciales de conexión.
- `--verbose`: Activa logging detallado.

---

## Recomendaciones y Buenas Prácticas

- **Atomicidad**: Cada paso es validado y registrado en bitácora. Si algo falla, el proceso se detiene y se reporta el error.
- **Configurabilidad**: El Excel permite modificar la estructura de las tablas y las claves de merge sin tocar el código.
- **Separación de responsabilidades**: El código está modularizado, siguiendo principios de arquitectura limpia.
- **Auditoría**: Todos los pasos críticos quedan registrados en logs y en la tabla de bitácora.
- **Validación de integridad**: Se comparan los conteos de registros en cada etapa para evitar pérdidas o duplicados.

---

## Recursos y Archivos Relacionados

- **`POSTGRESS_NETEZZA_PYTHON/QUERYS_POSTGRES.SQL`**: Scripts para crear tablas de ejemplo en PostgreSQL.
- **`POSTGRESS_NETEZZA_PYTHON/CARGA_DATA.SQL`**: Script para poblar las tablas de ejemplo con datos aleatorios.
- **`POSTGRESS_NETEZZA_PYTHON/EXAMPLE.INI`**: Ejemplo de archivo de configuración para conexiones.
- **Excel de configuración**: Define columnas, tipos, claves de merge, distribución, etc.

---

## ¿Qué hacer si algo falla?

- Revisa los logs (`libraries.log`, `output.log`) para detalles del error.
- Consulta la tabla de bitácora en Netezza para ver en qué paso falló el proceso.
- Verifica que los archivos de configuración y Excel estén correctamente formateados y accesibles.
- Si el error es de estructura de tabla, revisa la definición en el Excel y en la base de datos destino.

---

## Analogía arquitectónica

Piensa en este proceso como una mudanza bien planificada:

- **Empaquetas** (extraes) los objetos desde la casa vieja (PostgreSQL),
- **Revisas** que todo esté en cajas seguras (CSV con separador seguro),
- **Transportas** a la nueva casa (Netezza),
- **Desempaquetas** en una habitación temporal (tabla temporal),
- **Ubicas** cada cosa en su lugar definitivo (MERGE a la tabla de producción),
- **Haces inventario** para asegurarte de que nada se perdió en el camino (validación de conteos y bitácora).

---
