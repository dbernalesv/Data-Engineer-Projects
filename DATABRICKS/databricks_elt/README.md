# Proyecto ELT en Databricks

Proyecto individual de Diego Bernales para construir, por etapas, un flujo ELT con el conjunto público Brazilian E-Commerce by Olist y Databricks Free Edition.

## Avance

- Etapas implementadas: carga de Raw a Bronze y transformación de Bronze a Silver.
- Rama de trabajo: `feature/databricks`.
- Gold se incorporará después de revisar Silver.
- Los archivos de datos se guardan en Databricks; no se versionan en Git.

## Fuente de datos

El proyecto usa ocho CSV de Olist: pedidos, artículos, clientes, productos, pagos, reseñas, vendedores y traducción de categorías. El archivo de geolocalización queda fuera de esta primera etapa porque no se necesita para la carga inicial.

El conjunto está publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Al usarlo, respeta la atribución y las condiciones de su licencia CC BY-NC-SA 4.0.

Descarga los CSV y súbelos a la carpeta `olist/` del volumen Raw. No agregues los datos al repositorio.

## Organización del proyecto

- `config/`: parámetros comunes y configuración por capa.
- `ddl/bronze/`: sentencias SQL para crear los esquemas, volúmenes y tablas Bronze.
- `ddl/silver/`: sentencias SQL para crear las tablas Silver.
- `notebooks/`: notebooks de entrada para cada capa.
- `src/databricks_elt/process/`: lógica de ingesta y transformación reutilizable.
- `src/databricks_elt/utils/`: funciones para leer la configuración, escribir logs y ejecutar DDL.
- `resources/jobs/`: definición del Job del bundle.

## Preparación en Databricks

Necesitas un workspace Databricks Free Edition con Unity Catalog y permisos para crear los esquemas, volúmenes y tablas Delta del proyecto. Si el catálogo `olist_dev` aún no existe, también necesitas el permiso `CREATE CATALOG`. Antes de ejecutar el Job, carga los ocho CSV en:

```text
/Volumes/olist_dev/control/olist_raw/olist/
```

El catálogo del proyecto es `olist_dev`. El esquema `control` contiene los volúmenes `olist_raw` y `olist_state`; los esquemas `bronze` y `silver` contienen las tablas Delta de cada capa. El DDL crea los objetos que falten y conserva los que ya existen.

Auto Loader lee los CSV desde el volumen Raw y guarda su esquema y checkpoints bajo:

```text
/Volumes/olist_dev/control/olist_state/autoloader/bronze/
```

Las tablas Bronze tienen nombres como `olist_dev.bronze.brz_orders`. Conservan los valores de origen como texto, agregan `_source_file` y `_ingested_at`, y guardan en `_rescued_data` los campos que no reconocen. El DDL habilita Change Data Feed (CDF) para que Silver pueda leer los cambios de Bronze.

Silver crea tablas como `olist_dev.silver.slv_orders`. Lee el snapshot inicial y los cambios posteriores desde Bronze mediante CDF, convierte fechas y campos numéricos, normaliza textos, elimina duplicados por clave y aplica reglas sencillas de calidad definidas en `config/silver.yml`.

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

Al terminar el Job, revisa el estado de ambas tareas y las tablas de destino. Bronze conserva los valores de origen; Silver presenta fechas y métricas con tipos consistentes, textos normalizados y claves sin duplicados. Auto Loader y CDF guardan checkpoints independientes en el volumen `olist_state` para procesar solo los archivos y cambios nuevos.
