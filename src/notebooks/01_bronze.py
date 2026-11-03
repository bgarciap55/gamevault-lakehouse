# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (Extract)
# MAGIC
# MAGIC Lee las dos fuentes con `gamevault.fuentes` y deja cada entidad en bronze como parquet o avro,
# MAGIC con columnas de auditoría (`_fecha_carga`, `_origen`).
# MAGIC
# MAGIC | Fuente | Etapa 1 (`volume`) | Etapa 2 (`externo`) |
# MAGIC | --- | --- | --- |
# MAGIC | JSONL de IGDB | `/Volumes/<catálogo>/bronze/raw/igdb/` | `abfss://landing@<cuenta>.dfs.core.windows.net/igdb/` |
# MAGIC | RAWG y biblioteca | CSV en `/Volumes/<catálogo>/bronze/raw/postgres/` | PostgreSQL por JDBC |

# COMMAND ----------

import os
import sys

# El paquete gamevault vive en src/, una carpeta arriba de este notebook
sys.path.insert(0, os.path.abspath(".."))

from pyspark.errors import AnalysisException

from gamevault.config import Config
from gamevault.fuentes import (
    ENDPOINTS_IGDB,
    TABLAS_PG,
    agregar_auditoria,
    escribir_bronze,
    leer_igdb,
    leer_tabla_pg,
)

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.dropdown("modo_fuente", "volume", ["volume", "externo"], "Modo de fuente")
dbutils.widgets.text("ruta_raw", "", "Ruta raw (vacío = Volume)")
dbutils.widgets.text("ruta_bronze", "", "Ruta bronze (vacío = Volume)")
dbutils.widgets.dropdown("formato_bronze", "parquet", ["parquet", "avro"], "Formato bronze")
dbutils.widgets.text("pg_url", "", "JDBC de PostgreSQL (solo externo)")

modo_fuente = dbutils.widgets.get("modo_fuente")
cfg = Config(
    catalogo=dbutils.widgets.get("catalogo"),
    modo_fuente=modo_fuente,
    ruta_raw=dbutils.widgets.get("ruta_raw"),
    ruta_bronze=dbutils.widgets.get("ruta_bronze"),
    formato_bronze=dbutils.widgets.get("formato_bronze"),
    pg_url=dbutils.widgets.get("pg_url"),
    # Las credenciales nunca van en el código: salen del secret scope "gamevault"
    pg_usuario=dbutils.secrets.get("gamevault", "pg_usuario") if modo_fuente == "externo" else "",
    pg_clave=dbutils.secrets.get("gamevault", "pg_clave") if modo_fuente == "externo" else "",
)
print(cfg)

# COMMAND ----------

def cargar(nombre, leer, origen, carpeta):
    """Lee una entidad y la escribe en bronze; si la fuente no existe todavía, la salta."""
    try:
        df = agregar_auditoria(leer(), origen)
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e):
            print(f"  {nombre:<22} omitido: no hay datos en la fuente")
            return
        raise
    ruta = escribir_bronze(df, cfg, carpeta)
    print(f"  {nombre:<22} {df.count():>9,} filas -> {ruta}")


print("IGDB")
for endpoint in ENDPOINTS_IGDB:
    cargar(endpoint, lambda: leer_igdb(spark, cfg, endpoint), "igdb", f"igdb/{endpoint}")

print("PostgreSQL")
for tabla in TABLAS_PG:
    cargar(tabla, lambda: leer_tabla_pg(spark, cfg, tabla), "postgres", f"postgres/{tabla}")
