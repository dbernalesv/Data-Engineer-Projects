# Databricks ELT project

Proyecto individual de Diego Bernales para construir por etapas un pipeline ELT sobre el conjunto publico Brazilian E-Commerce by Olist, usando Databricks Free Edition.

## Estado

- Etapa actual: ingesta Raw a Bronze.
- Rama de trabajo: `feature/databricks`.
- Las capas Silver y Gold se agregaran despues de revisar y aceptar Bronze.
- Los archivos del conjunto de datos no se guardan en Git.

## Fuente

Se usan ocho archivos CSV de Olist: pedidos, items, clientes, productos, pagos, resenas, vendedores y traduccion de categorias. Se omite geolocalizacion porque no es necesaria para el modelo inicial. El conjunto original esta publicado en [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) y su uso requiere respetar la atribucion y licencia aplicable (CC BY-NC-SA 4.0).

Descarga los CSV y cárgalos en el volumen de Unity Catalog indicado por `config/bronze.yml`. La carpeta esperada es `olist/` dentro del volumen Raw. No subas los datos al repositorio.

## Estructura

- `config/`: parametros comunes y por capa en YAML.
- `ddl/bronze/`: SQL para crear esquemas, volumenes y tablas Bronze.
- `notebooks/bronze/`: punto de entrada del Job; mantiene solo la orquestacion.
- `src/databricks_elt/process/`: logica de ingesta reutilizable.
- `src/databricks_elt/utils/`: carga de configuracion, logger y ejecucion de DDL.
- `resources/jobs/`: definicion del Job del bundle.

## Requisitos en Databricks

- Un workspace Databricks Free Edition con Unity Catalog.
- El usuario que ejecuta el Job debe poder crear esquemas, volumenes y tablas Delta en el catalogo configurado.
- El usuario que ejecute el DDL inicial debe tener `CREATE CATALOG` si `olist_dev` aun no existe.
- Los CSV deben estar cargados en el volumen Raw antes de ejecutar el Job.
- Databricks CLI instalado localmente ([instalacion oficial](https://docs.databricks.com/aws/en/dev-tools/cli/install)).

El catalogo del proyecto es `olist_dev`. El esquema `control` contiene los volumenes `olist_raw` y `olist_state`; el esquema `bronze` contiene las tablas Delta Bronze. El DDL crea el catalogo si falta, ambos esquemas y los volumenes. El Job crea o comprueba las tablas y luego procesa los CSV con Auto Loader. Bronze conserva los valores de origen como texto, agrega metadatos de ingesta y mantiene los campos no reconocidos en `_rescued_data`. Las conversiones y reglas de negocio se reservan para Silver.

Carga los ocho CSV dentro de la carpeta `olist/` del volumen Raw:

```text
/Volumes/olist_dev/control/olist_raw/olist/
```

El estado y los checkpoints de Auto Loader se guardan bajo:

```text
/Volumes/olist_dev/control/olist_state/autoloader/bronze/
```

Las tablas de destino tienen nombres completos como `olist_dev.bronze.brz_orders`.

## Despliegue desde PowerShell

Desde `DATABRICKS/databricks_elt/`, autentica el Databricks CLI una vez con OAuth y la URL de tu workspace:

```powershell
databricks auth login --host https://<workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev elt_pipeline
```

Los comandos despliegan el bundle desde la rama local actual al unico workspace, con el target `dev`. No se requiere enlazar una Git folder de Databricks para ejecutar este flujo. La configuracion de autenticacion permanece fuera del repositorio.

## Evaluacion de Bronze

Revisa en Databricks que el Job termine correctamente, que existan las tablas Delta esperadas, que los conteos correspondan con los archivos fuente, que `_source_file` y `_ingested_at` esten presentes y que una segunda ejecucion no vuelva a insertar los mismos archivos. Los logs del Job muestran el resultado de cada tabla; la salida no usa `display`, `show` ni `print`.
