# Proyecto ELT en Databricks

Proyecto individual de Diego Bernales para construir, por etapas, un flujo ELT con el conjunto público Brazilian E-Commerce by Olist y Databricks Free Edition.

## Avance

- El Job ejecuta cuatro tareas en orden: Raw, Bronze, Silver y Gold.
- Frecuencia prevista: cada lunes a las 8:00 a. m., hora de Lima (`America/Lima`).
- Rama de trabajo: `feature/databricks`.
- Gold se construye como un esquema estrella y consume cambios de Silver por CDF.
- Los archivos de datos se guardan en Databricks; no se versionan en Git.

## Fuente de datos

El proyecto usa ocho tipos de CSV de Olist. Los patrones de nombres y las carpetas de destino están en [config/bronze.yml](config/bronze.yml).

El conjunto está publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Al usarlo, respeta la atribución y las condiciones de su licencia CC BY-NC-SA 4.0.

Descarga los CSV y déjalos en la bandeja `inbox/` del volumen Raw. El Job conserva sus bytes y clasifica cada archivo en la subcarpeta que lee Auto Loader.

## Organización del proyecto

- `config/`: parámetros comunes y configuración por capa.
- `deploy/`: manifiestos con las rutas de los DDL que ejecuta Workflow Deployer.
- `ddl/raw/`: sentencias SQL para crear el catálogo, el esquema de control y el volumen Raw.
- `ddl/bronze/`: sentencias SQL para crear los esquemas, volúmenes y tablas Bronze.
- `ddl/silver/`, `ddl/gold/` y `ddl/control/`: tablas de Silver, el esquema estrella y los watermarks CDF.
- `notebooks/`: notebooks de entrada para cada capa.
- `src/process/`: lógica de ingesta y transformación por capa.
- `src/utils/`: funciones comunes para configuración, logs, DDL y CDF.
- `resources/jobs/`: definición del Job del bundle.

## Preparación en Databricks

Necesitas un workspace Databricks Free Edition con Unity Catalog y permisos para crear los esquemas, volúmenes y tablas Delta. Si el catálogo `olist_dev` aún no existe, también necesitas `CREATE CATALOG`. Despliega el bundle para que Workflow Deployer cree las estructuras. Luego carga los CSV originales en:

```text
/Volumes/olist_dev/control/olist_raw/inbox/
```

La descarga y el depósito inicial son manuales. Raw admite archivos individuales y los copia bajo `/Volumes/olist_dev/control/olist_raw/olist/`. Las subcarpetas son `orders`, `order_items`, `customers`, `products`, `order_payments`, `order_reviews`, `sellers` y `category_translation`. Si un archivo ya existe y su SHA-256 coincide, Raw lo omite. Si el contenido difiere, la tarea falla antes de publicar el lote de archivos pendiente.

El catálogo del proyecto es `olist_dev`. El esquema `control` contiene los volúmenes `olist_raw` y `olist_state`, además de las tablas de watermarks; los esquemas `bronze`, `silver` y `gold` contienen las tablas Delta de cada capa. El DDL crea los objetos que falten y conserva los que ya existen.

Auto Loader lee los CSV desde el volumen Raw y guarda su esquema y checkpoints bajo:

```text
/Volumes/olist_dev/control/olist_state/autoloader/bronze/v2/
```

Cada tabla Bronze usa su subcarpeta y checkpoint. Si llega solo un archivo de artículos, la carga de filas corresponde a `olist_dev.bronze.brz_order_items`. Bronze conserva los valores de origen como texto, agrega `_source_file` y `_ingested_at`, y guarda en `_rescued_data` los campos que no reconoce. Workflow Deployer habilita Change Data Feed (CDF) para Silver.

La primera ejecución de Silver carga una versión consistente de Bronze. Las siguientes leen por CDF desde la versión guardada en `olist_dev.control.cdf_watermarks`. Se procesan `insert` y `update_postimage`, se convierten fechas y campos numéricos, se normalizan textos y se aplican las reglas de `config/silver.yml`. Un `MERGE` por clave escribe solo filas nuevas o diferentes. Silver registra su estado en Delta mediante el watermark.

La tabla `cdf_watermarks` guarda por tabla fuente y destino la última versión y timestamp procesados entre Bronze y Silver. `gold_cdf_watermarks` guarda el avance independiente de Gold al consumir Silver. Los watermarks avanzan después de los `MERGE`; si una ejecución se interrumpe antes, la próxima puede repetir el rango sin duplicar filas. En esta etapa no se procesan eventos `delete`.

## Modelo dimensional Gold

El esquema `olist_dev.gold` publica `fct_order_items` con grano de una fila por artículo de pedido. Sus medidas son precio, flete, importe del artículo (precio más flete) y cantidad de artículos. Se relaciona por claves de negocio con `dim_customer`, `dim_product` y `dim_seller`, y por `order_date_key` con `dim_date`. `order_id` es una dimensión degenerada para contar pedidos distintos.

El proceso Gold consume CDF de pedidos, artículos, clientes, productos, vendedores y traducción de categorías en Silver. Si recibe cambios de dimensiones, actualiza las claves afectadas; si cambia un pedido o artículo, vuelve a calcular sus líneas. Los `MERGE` actualizan solo filas diferentes y son seguros ante reintentos. Las tablas de Gold quedan listas para preguntas como ventas de artículos por mes, estado y estado del cliente, o categorías con mayor importe. El importe es precio más flete del artículo, no el total efectivamente pagado del pedido.

## Desplegar desde PowerShell

Instala el [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). Desde `DATABRICKS/databricks_elt/`, inicia sesión con OAuth y luego valida, despliega y ejecuta el bundle en el workspace:

```powershell
databricks auth login --host https://<workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
# Con los archivos de entrada ya disponibles:
databricks bundle run -t dev elt_pipeline
```

Cada despliegue ejecuta Workflow Deployer y espera su resultado. El manifiesto predeterminado, [deploy/initial.yml](deploy/initial.yml), contiene una lista ordenada `ddl_files` con rutas relativas a la raíz del bundle, por ejemplo `ddl/bronze/00_create_schemas_and_volumes.sql`. Puedes ejecutar el Deployer con otro YAML desplegado bajo `deploy/` mediante `databricks bundle run -t dev --params manifest_path=deploy/mi_cambio.yml workflow_deployer`.

## Revisar la ejecución

Raw informa cuántos archivos copió y cuántos ya existían. Bronze comprueba los archivos pendientes por tabla. Silver informa qué tablas de Bronze están al día y Gold muestra el rango CDF o la versión de cada insumo. Una tarea sin novedades termina correctamente, y las capas siguientes revisan sus propios watermarks.

Para reconstruir un entorno, verifica y conserva primero sus CSV, pausa la programación, recrea las tablas de datos y control con Workflow Deployer e inicia Bronze con checkpoints nuevos. Carga el histórico y contrasta los conteos antes de incorporar archivos incrementales. Si se recrean las tablas Delta, también se reinician sus watermarks, porque las versiones nuevas pertenecen a otro historial. Reactiva la programación después de validar la carga.

## Abrir el informe de Power BI

Abre [Olist_Ventas_y_Categorias.pbip](powerbi/Olist_Ventas_y_Categorias/Olist_Ventas_y_Categorias.pbip) en Power BI Desktop. El informe consulta Gold en modo Import. Para actualizarlo, autentícate en el SQL Warehouse desde Desktop y selecciona **Inicio → Actualizar**. Consulta [la guía del informe](powerbi/Olist_Ventas_y_Categorias/README.md) para ver las páginas y las definiciones de los indicadores.
