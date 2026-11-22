# Proyecto ELT en Databricks

Proyecto individual de Diego Bernales para construir, por etapas, un flujo ELT con el conjunto público Brazilian E-Commerce by Olist y Databricks Free Edition.

## Avance

- Etapas implementadas: carga de Raw a Bronze, transformación de Bronze a Silver y modelo dimensional Gold.
- Rama de trabajo: `feature/databricks`.
- Gold se construye como un esquema estrella y consume cambios de Silver por CDF.
- Los archivos de datos se guardan en Databricks; no se versionan en Git.

## Fuente de datos

El proyecto usa ocho CSV de Olist: pedidos, artículos, clientes, productos, pagos, reseñas, vendedores y traducción de categorías. El archivo de geolocalización queda fuera de esta primera etapa porque no se necesita para la carga inicial.

El conjunto está publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Al usarlo, respeta la atribución y las condiciones de su licencia CC BY-NC-SA 4.0.

Descarga los CSV y súbelos a la carpeta `olist/` del volumen Raw. No agregues los datos al repositorio.

## Organización del proyecto

- `config/`: parámetros comunes y configuración por capa.
- `ddl/bronze/`: sentencias SQL para crear los esquemas, volúmenes y tablas Bronze.
- `ddl/silver/`, `ddl/gold/` y `ddl/control/`: tablas de Silver, el esquema estrella y los watermarks CDF.
- `notebooks/`: notebooks de entrada para cada capa.
- `src/databricks_elt/process/`: lógica de ingesta y transformación reutilizable por capa.
- `src/databricks_elt/utils/`: funciones para leer la configuración, escribir logs y ejecutar DDL.
- `resources/jobs/`: definición del Job del bundle.

## Preparación en Databricks

Necesitas un workspace Databricks Free Edition con Unity Catalog y permisos para crear los esquemas, volúmenes y tablas Delta del proyecto. Si el catálogo `olist_dev` aún no existe, también necesitas el permiso `CREATE CATALOG`. Antes de ejecutar el Job, carga los ocho CSV en:

```text
/Volumes/olist_dev/control/olist_raw/olist/
```

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

## Revisión de Bronze y Silver

Al terminar el Job, revisa el estado de ambas tareas, las tablas de destino y los watermarks. Bronze conserva los valores de origen; Silver presenta fechas y métricas con tipos consistentes, textos normalizados y claves sin duplicados. Auto Loader guarda sus checkpoints en el volumen `olist_state`; Silver usa la tabla `cdf_watermarks` para continuar desde la última versión procesada.
