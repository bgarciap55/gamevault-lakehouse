# Databricks notebook source
# MAGIC %md
# MAGIC # PrepAmb · Preparar el ambiente
# MAGIC
# MAGIC Crea, si no existen:
# MAGIC
# MAGIC 1. **Etapa 2 (Azure):** las ubicaciones externas `ext_landing`, `ext_bronze` y `ext_catalogo` sobre los
# MAGIC    contenedores del data lake, usando la storage credential de la identidad administrada (`cred_gamevault`).
# MAGIC 2. El catálogo del ambiente. En Azure sus tablas administradas quedan en `abfss://catalogo@<cuenta>/<catálogo>`.
# MAGIC 3. Sus esquemas `bronze`, `silver` y `gold`.
# MAGIC 4. **Etapa 1 (Free Edition):** los Volumes `bronze.raw` (datos crudos) y `bronze.archivos` (salida de bronze).
# MAGIC
# MAGIC Usa el SDK de Databricks en lugar de `CREATE CATALOG`, porque el proyecto no usa SQL.
# MAGIC Se puede correr cuantas veces se quiera: lo que ya existe no se toca.
# MAGIC
# MAGIC En Free Edition el catálogo hay que crearlo una vez desde la interfaz (Catalog > + > Create catalog,
# MAGIC Default Storage), porque ahí no se puede por código. El notebook lo detecta y lo explica.

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.dropdown("modo_fuente", "volume", ["volume", "externo"], "Modo de fuente")
dbutils.widgets.text("cuenta_adls", "", "Cuenta ADLS Gen2 (solo externo)")
dbutils.widgets.text("credencial", "cred_gamevault", "Storage credential (solo externo)")

catalogo = dbutils.widgets.get("catalogo")
modo_fuente = dbutils.widgets.get("modo_fuente")
cuenta_adls = dbutils.widgets.get("cuenta_adls")
credencial = dbutils.widgets.get("credencial")

if modo_fuente == "externo" and not cuenta_adls:
    raise ValueError("En modo externo hay que indicar cuenta_adls (la cuenta de ADLS Gen2)")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InvalidState, NotFound
from databricks.sdk.service.catalog import VolumeType

w = WorkspaceClient()

ESQUEMAS = {
    "bronze": "Datos tal cual llegan de las fuentes",
    "silver": "Catálogo limpio y unificado (RAWG + IGDB)",
    "gold": "Explorador de versiones, plataformas y biblioteca",
}
VOLUMES = {
    "raw": "Datos crudos de la etapa 1: igdb/<endpoint>/*.jsonl y postgres/<tabla>.csv",
    "archivos": "Salida de bronze en la etapa 1 (parquet o avro)",
}


# Un contenedor del data lake por uso
CONTENEDORES = {
    "landing": "Fuente 1: JSONL de la API de IGDB y CSV de origen",
    "bronze": "Capa bronze en parquet o avro",
    "catalogo": "Tablas Delta administradas de silver y gold",
}


def existe(leer):
    try:
        leer()
        return True
    except NotFound:
        return False


def url_contenedor(contenedor):
    return f"abfss://{contenedor}@{cuenta_adls}.dfs.core.windows.net/"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ubicaciones externas (solo etapa 2)
# MAGIC
# MAGIC Unity Catalog solo deja leer o escribir rutas del data lake que estén registradas como ubicación externa.
# MAGIC La storage credential (`cred_gamevault`) se crea una vez desde Catalog > External Data > Credentials con el
# MAGIC Access Connector `ac-gamevault` y la identidad administrada `id-gamevault`, que tiene el rol
# MAGIC *Storage Blob Data Contributor* sobre la cuenta.

# COMMAND ----------

if modo_fuente == "externo":
    for contenedor, comentario in CONTENEDORES.items():
        nombre = f"ext_{contenedor}"
        if existe(lambda: w.external_locations.get(nombre)):
            print(f"Ubicación externa {nombre}: ya existe")
        else:
            # skip_validation: la validación de "file events" falla sin roles extra que no necesitamos (ingesta por lotes)
            w.external_locations.create(
                name=nombre,
                url=url_contenedor(contenedor),
                credential_name=credencial,
                comment=comentario,
                skip_validation=True,
            )
            print(f"Ubicación externa {nombre}: creada -> {url_contenedor(contenedor)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogo y esquemas

# COMMAND ----------

if existe(lambda: w.catalogs.get(catalogo)):
    print(f"Catálogo {catalogo}: ya existe")
else:
    # En Azure las tablas administradas del catálogo viven en el contenedor "catalogo" del data lake
    ubicacion = f"{url_contenedor('catalogo')}{catalogo}" if modo_fuente == "externo" else None
    try:
        w.catalogs.create(
            name=catalogo,
            comment="GameVault: versiones, plataformas y remakes de videojuegos",
            storage_root=ubicacion,
        )
    except InvalidState as e:
        # Free Edition (Default Storage) solo permite crear catálogos desde la interfaz
        raise RuntimeError(
            f"No se pudo crear el catálogo {catalogo} por código. Créalo en Catalog > + > Create catalog "
            f"(nombre {catalogo}, tipo Standard, Default Storage) y vuelve a correr este notebook."
        ) from e
    print(f"Catálogo {catalogo}: creado" + (f" en {ubicacion}" if ubicacion else ""))

for esquema, comentario in ESQUEMAS.items():
    if existe(lambda: w.schemas.get(f"{catalogo}.{esquema}")):
        print(f"Esquema {esquema}: ya existe")
    else:
        w.schemas.create(name=esquema, catalog_name=catalogo, comment=comentario)
        print(f"Esquema {esquema}: creado")

if modo_fuente == "volume":
    for volume, comentario in VOLUMES.items():
        if existe(lambda: w.volumes.read(f"{catalogo}.bronze.{volume}")):
            print(f"Volume bronze.{volume}: ya existe")
        else:
            w.volumes.create(
                catalog_name=catalogo,
                schema_name="bronze",
                name=volume,
                volume_type=VolumeType.MANAGED,
                comment=comentario,
            )
            print(f"Volume bronze.{volume}: creado")
