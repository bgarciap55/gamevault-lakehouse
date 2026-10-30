# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparar el catálogo
# MAGIC
# MAGIC Crea, si no existen, el catálogo del ambiente, sus esquemas `bronze`, `silver` y `gold` y,
# MAGIC en la etapa 1, los Volumes `bronze.raw` (datos crudos) y `bronze.archivos` (salida de bronze).
# MAGIC
# MAGIC Usa el SDK de Databricks en lugar de `CREATE CATALOG`, porque el proyecto no usa SQL.
# MAGIC Se puede correr cuantas veces se quiera: lo que ya existe no se toca.

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.dropdown("modo_fuente", "volume", ["volume", "externo"], "Modo de fuente")

catalogo = dbutils.widgets.get("catalogo")
modo_fuente = dbutils.widgets.get("modo_fuente")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
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


def existe(leer):
    try:
        leer()
        return True
    except NotFound:
        return False


if existe(lambda: w.catalogs.get(catalogo)):
    print(f"Catálogo {catalogo}: ya existe")
else:
    w.catalogs.create(name=catalogo, comment="GameVault: versiones, plataformas y remakes de videojuegos")
    print(f"Catálogo {catalogo}: creado")

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
