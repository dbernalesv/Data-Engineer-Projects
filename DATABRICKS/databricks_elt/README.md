# Proyecto ELT en Databricks

Proyecto individual de Diego Bernales para construir, por etapas, un flujo ELT con el conjunto público Brazilian E-Commerce by Olist y Databricks Free Edition.

## Avance

- Etapa actual: carga de datos Raw a Bronze.
- Rama de trabajo: `feature/databricks`.
- Silver y Gold se incorporarán después de revisar Bronze.
- Los archivos de datos se guardan en Databricks; no se versionan en Git.

## Fuente de datos

El proyecto usa ocho CSV de Olist: pedidos, artículos, clientes, productos, pagos, reseñas, vendedores y traducción de categorías. El archivo de geolocalización queda fuera de esta primera etapa porque no se necesita para la carga inicial.

El conjunto está publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). Al usarlo, respeta la atribución y las condiciones de su licencia CC BY-NC-SA 4.0.

Descarga los CSV y súbelos a la carpeta `olist/` del volumen Raw. No agregues los datos al repositorio.

## Organización del proyecto

- `config/`: parámetros comunes y configuración por capa.
- `ddl/bronze/`: sentencias SQL para crear los esquemas, volúmenes y tablas Bronze.
- `notebooks/bronze/`: notebook que organiza los pasos de la carga.
- `src/databricks_elt/process/`: lógica de ingesta reutilizable.
- `src/databricks_elt/utils/`: funciones para leer la configuración, escribir logs y ejecutar DDL.
- `resources/jobs/`: definición del Job del bundle.

## Preparación en Databricks

Necesitas un workspace Databricks Free Edition con Unity Catalog y permisos para crear los esquemas, volúmenes y tablas Delta del proyecto. Si el catálogo `olist_dev` aún no existe, también necesitas el permiso `CREATE CATALOG`. Antes de ejecutar el Job, carga los ocho CSV en:

```text
/Volumes/olist_dev/control/olist_raw/olist/
```

El catálogo del proyecto es `olist_dev`. El esquema `control` contiene los volúmenes `olist_raw` y `olist_state`; el esquema `bronze` contiene las tablas Delta. El DDL crea los objetos que falten y conserva los que ya existen.

Auto Loader lee los CSV desde el volumen Raw y guarda su esquema y checkpoints bajo:

```text
/Volumes/olist_dev/control/olist_state/autoloader/bronze/
```

Las tablas de destino tienen nombres como `olist_dev.bronze.brz_orders`. Bronze conserva los valores de origen como texto, agrega `_source_file` y `_ingested_at`, y guarda en `_rescued_data` los campos que no reconoce. Las conversiones de tipos y las reglas de negocio se dejan para Silver.

## Desplegar desde PowerShell

Instala el [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install). Desde `DATABRICKS/databricks_elt/`, inicia sesión con OAuth y luego valida, despliega y ejecuta el bundle en el workspace:

```powershell
databricks auth login --host https://<workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev elt_pipeline
```

El bundle se despliega desde la rama local en la que estés trabajando hacia el único workspace, usando el target `dev`. Este flujo no requiere una Git Folder de Databricks. La configuración de autenticación se guarda fuera del repositorio.

## Revisión de Bronze

Al terminar el Job, comprueba que se hayan creado las tablas esperadas, que sus filas correspondan a los archivos de origen y que incluyan `_source_file` y `_ingested_at`. Si vuelves a ejecutar la carga, Auto Loader usa sus checkpoints para no volver a procesar los mismos archivos.
