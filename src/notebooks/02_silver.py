# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver (Transform)
# MAGIC
# MAGIC Limpia bronze y deja el catálogo unificado en `<catálogo>.silver`, todo en Delta.
# MAGIC La lógica está en `gamevault/silver.py`; aquí solo se lee bronze, se llama a cada función y se escribe.
# MAGIC
# MAGIC - `games`, `platforms` y `releases` se actualizan con **MERGE** (API Python de `DeltaTable`).
# MAGIC - El resto se reconstruye entero en cada corrida, porque se deriva por completo de bronze.
# MAGIC - Las fuentes que no se cargaron (por ejemplo, tu biblioteca) se saltan.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(".."))

from pyspark.sql import functions as F

from gamevault import silver
from gamevault.config import Config
from gamevault.tablas import leer_bronze, nombre_tabla, sobrescribir, upsert

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.text("ruta_bronze", "", "Ruta bronze (vacío = Volume)")
dbutils.widgets.dropdown("formato_bronze", "parquet", ["parquet", "avro"], "Formato bronze")

# Silver solo lee bronze, así que el modo de fuente no le importa
cfg = Config(
    catalogo=dbutils.widgets.get("catalogo"),
    ruta_bronze=dbutils.widgets.get("ruta_bronze"),
    formato_bronze=dbutils.widgets.get("formato_bronze"),
)
S = lambda tabla: nombre_tabla(cfg, "silver", tabla)
bronze = lambda carpeta: leer_bronze(spark, cfg, carpeta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## IGDB: tipos, plataformas, juegos y relaciones

# COMMAND ----------

tipos = silver.game_types(bronze("igdb/game_types"))
sobrescribir(tipos, S("game_types"))

upsert(silver.platforms(bronze("igdb/platforms")), S("platforms"), "platform_id")

b_games = bronze("igdb/games")
games = silver.games(b_games, spark.read.table(S("game_types")))
descartados = b_games.select("id").distinct().count() - games.count()
print(f"  regla de calidad de games: {descartados} juegos descartados")
upsert(games, S("games"), "game_id")

sobrescribir(silver.game_relations(b_games), S("game_relations"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## IGDB: lanzamientos y tiempo para terminarlo

# COMMAND ----------

b_regiones = bronze("igdb/release_date_regions")
upsert(
    silver.releases(bronze("igdb/release_dates"), silver.regiones(b_regiones, spark)),
    S("releases"),
    "release_id",
)

b_ttb = bronze("igdb/game_time_to_beats")
if b_ttb is not None:
    sobrescribir(silver.time_to_beat(b_ttb), S("time_to_beat"))
else:
    print("  time_to_beat: no se descargó, se omite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAWG y emparejamiento con IGDB

# COMMAND ----------

sobrescribir(silver.rawg_games(bronze("postgres/rawg_games")), S("rawg_games"))
rawg = spark.read.table(S("rawg_games"))
sobrescribir(silver.rawg_game_platforms(rawg), S("rawg_game_platforms"))
sobrescribir(silver.source_ids(spark.read.table(S("games")), rawg), S("source_ids"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tu biblioteca

# COMMAND ----------

b_mis_plataformas = bronze("postgres/mis_plataformas")
b_mi_biblioteca = bronze("postgres/mi_biblioteca")

if b_mis_plataformas is not None:
    sobrescribir(silver.my_platforms(b_mis_plataformas, spark.read.table(S("platforms"))), S("my_platforms"))
else:
    print("  my_platforms: no hay mis_plataformas.csv, se omite")

if b_mi_biblioteca is not None and spark.catalog.tableExists(S("my_platforms")):
    sobrescribir(
        silver.my_library(b_mi_biblioteca, spark.read.table(S("games")), spark.read.table(S("my_platforms"))),
        S("my_library"),
    )
else:
    print("  my_library: falta mi_biblioteca.csv o mis_plataformas.csv, se omite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobaciones rápidas

# COMMAND ----------

display(spark.read.table(S("games")).groupBy("game_type").count().orderBy(F.desc("count")))
display(spark.read.table(S("game_relations")).groupBy("relation_type").count().orderBy(F.desc("count")))

g = spark.read.table(S("games"))
emparejados = spark.read.table(S("source_ids")).count()
print(f"Juegos IGDB con pareja en RAWG: {emparejados:,} de {g.count():,}")
