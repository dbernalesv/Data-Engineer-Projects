# Proyecto ELT en Databricks

Proyecto individual de Diego Bernales para construir, por etapas, un flujo ELT con el conjunto público Brazilian E-Commerce by Olist y Databricks Free Edition.

## Avance

- El Job ejecuta cuatro tareas en orden: Raw, Bronze, Silver y Gold.
- Frecuencia prevista: cada lunes a las 8:00 a. m., hora de Lima (`America/Lima`).
- Rama de trabajo: `feature/databricks`.
- Gold se construye como un esquema estrella y consume cambios de Silver por CDF.
- Los archivos de datos se guardan en Databricks; no se versionan en Git.

## Fuente de datos

El proyecto usa ocho CSV de Olist: pedidos, artículos, clientes, productos, pagos, reseñas, vendedores y traducción de categorías. Consulta la lista de archivos en [config/raw.yml](config/raw.yml).

El conjunto está publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Al usarlo, respeta la atribución y las condiciones de su licencia CC BY-NC-SA 4.0.

Descarga los CSV y déjalos en la bandeja `inbox/` del volumen Raw. El Job copia sus bytes originales a `olist/`, la carpeta que lee Auto Loader.

## Organización del proyecto

- `config/`: parámetros comunes y configuración por capa.
- `ddl/raw/`: sentencias SQL para crear el catálogo, el esquema de control y el volumen Raw.
- `ddl/bronze/`: sentencias SQL para crear los esquemas, volúmenes y tablas Bronze.
- `ddl/silver/`, `ddl/gold/` y `ddl/control/`: tablas de Silver, el esquema estrella y los watermarks CDF.
- `notebooks/`: notebooks de entrada para cada capa.
- `src/databricks_elt/process/`: lógica de ingesta y transformación reutilizable por capa.
- `src/databricks_elt/utils/`: funciones para leer la configuración, escribir logs y ejecutar DDL.
- `resources/jobs/`: definición del Job del bundle.

## Preparación en Databricks

Necesitas un workspace Databricks Free Edition con Unity Catalog y permisos para crear los esquemas, volúmenes y tablas Delta del proyecto. Si el catálogo `olist_dev` aún no existe, también necesitas el permiso `CREATE CATALOG`. En un workspace nuevo, crea el volumen con [el DDL de Raw](ddl/raw/00_create_volume.sql) usando los nombres de `config/common.yml`. Luego carga los ocho CSV originales, con sus nombres exactos, en:

```text
/Volumes/olist_dev/control/olist_raw/inbox/
```

La bandeja es el punto de entrada para los archivos planos de Olist. La descarga y el depósito inicial son manuales. Cada ejecución del Job valida que el lote esté completo y copia los archivos faltantes a `/Volumes/olist_dev/control/olist_raw/olist/`. Si un archivo ya existe y su SHA-256 coincide, la tarea lo omite. Si falta un archivo de entrada o el contenido del destino difiere, la tarea falla antes de iniciar Bronze. Los archivos de Raw se tratan como inmutables para conservar el seguimiento de Auto Loader.

El catálogo del proyecto es `olist_dev`. El esquema `control` contiene los volúmenes `olist_raw` y `olist_state`, además de las tablas de watermarks; los esquemas `bronze`, `silver` y `gold` contienen las tablas Delta de cada capa. El DDL crea los objetos que falten y conserva los que ya existen.

Auto Loader lee los CSV desde el volumen Raw y guarda su esquema y checkpoints bajo:

```text
/Volumes/olist_dev/control/olist_state/autoloader/bronze/
```

Las tablas Bronze tienen nombres como `olist_dev.bronze.brz_orders`. Conservan los valores de origen como texto, agregan `_source_file` y `_ingested_at`, y guardan en `_rescued_data` los campos que no reconocen. El DDL habilita Change Data Feed (CDF) para que Silver pueda leer los cambios de Bronze.

Silver crea tablas como `olist_dev.silver.slv_orders`. La primera ejecución carga una versión consistente de Bronze; las siguientes leen los cambios de CDF desde la versión guardada en `olist_dev.control.cdf_watermarks`. Se procesan `insert` y `update_postimage`, se convierten fechas y campos numéricos, se normalizan textos y se aplican reglas de calidad de `config/silver.yml`. Un `MERGE` por clave inserta o actualiza filas y evita escrituras si los valores no cambiaron. Silver conserva `_ingested_at`, pero no `_source_file` ni los metadatos de commit de Bronze.

La tabla `cdf_watermarks` guarda por tabla fuente y destino la última versión y timestamp procesados entre Bronze y Silver. `gold_cdf_watermarks` guarda el avance independiente de Gold al consumir Silver. Los watermarks avanzan después de los `MERGE`; si una ejecución se interrumpe antes, la próxima puede repetir el rango sin duplicar filas. En esta etapa no se procesan eventos `delete`.

En la primera ejecución de esta versión, el proceso detecta las columnas antiguas en Silver, recrea esas tablas y las carga desde una versión actual de Bronze. Bronze no se elimina ni se modifica.

## Modelo dimensional Gold

El esquema `olist_dev.gold` publica `fct_order_items` con grano de una fila por artículo de pedido. Sus medidas son precio, flete, importe del artículo (precio más flete) y cantidad de artículos. Se relaciona por claves de negocio con `dim_customer`, `dim_product` y `dim_seller`, y por `order_date_key` con `dim_date`. `order_id` es una dimensión degenerada para contar pedidos distintos.

El proceso Gold consume CDF de pedidos, artículos, clientes, productos, vendedores y traducción de categorías en Silver. Si recibe cambios de dimensiones, actualiza las claves afectadas; si cambia un pedido o artículo, vuelve a calcular sus líneas. Los `MERGE` actualizan solo filas diferentes y son seguros ante reintentos. Las tablas de Gold quedan listas para preguntas como ventas de artículos por mes, estado y estado del cliente, o categorías con mayor importe. El importe es precio más flete del artículo, no el total efectivamente pagado del pedido.

## Desplegar desde PowerShell

Instala el [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). Desde `DATABRICKS/databricks_elt/`, inicia sesión con OAuth y luego valida, despliega y ejecuta el bundle en el workspace:

```powershell
databricks auth login --host https://<workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev elt_pipeline
```

El bundle se despliega desde la rama local en la que estés trabajando hacia el único workspace, usando el target `dev`. Este flujo no requiere una Git Folder de Databricks. La configuración de autenticación se guarda fuera del repositorio.

## Revisar la ejecución

Revisa el resultado de `raw_ingestion`: indica cuántos archivos se copiaron y cuántos ya existían. Al repetir el Job con el mismo lote, se esperan cero copias y ocho archivos existentes. Comprueba que las cuatro tareas finalicen correctamente y revisa las tablas de destino y sus watermarks.

## Abrir el informe de Power BI

Abre [Olist_Ventas_y_Categorias.pbip](powerbi/Olist_Ventas_y_Categorias/Olist_Ventas_y_Categorias.pbip) en Power BI Desktop. El informe consulta Gold en modo Import. Para actualizarlo, autentícate en el SQL Warehouse desde Desktop y selecciona **Inicio → Actualizar**. Consulta [la guía del informe](powerbi/Olist_Ventas_y_Categorias/README.md) para ver las páginas y las definiciones de los indicadores.
