# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (Load)
# MAGIC
# MAGIC Construye el explorador en `<catálogo>.gold`, en Delta, a partir de silver:
# MAGIC
# MAGIC | Tabla | Para qué |
# MAGIC | --- | --- |
# MAGIC | `game_catalog` | Ficha de cada juego: notas, plataformas, horas, número de versiones y si lo tienes |
# MAGIC | `game_versions` | Remakes, remasters, ports y ediciones de cada juego, y si puedes jugarlos en tus plataformas |
# MAGIC | `game_releases` | Cuándo salió cada juego en cada plataforma y región |
# MAGIC | `my_library` | Tu biblioteca con nombres limpios |
# MAGIC
# MAGIC Se reconstruyen enteras en cada corrida. En la etapa 2 estas tablas se copian a PostgreSQL para Power BI.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from pyspark.sql import functions as F

from gamevault import gold
from gamevault.config import Config
from gamevault.tablas import nombre_tabla, sobrescribir

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
cfg = Config(catalogo=dbutils.widgets.get("catalogo"))

S = lambda tabla: nombre_tabla(cfg, "silver", tabla)
G = lambda tabla: nombre_tabla(cfg, "gold", tabla)
silver = lambda tabla: spark.read.table(S(tabla)) if spark.catalog.tableExists(S(tabla)) else None

games, platforms, relations = silver("games"), silver("platforms"), silver("game_relations")
my_platforms, my_library = silver("my_platforms"), silver("my_library")

# COMMAND ----------

sobrescribir(
    gold.game_catalog(
        games, platforms, relations, silver("rawg_games"), silver("source_ids"),
        silver("time_to_beat"), my_library,
    ),
    G("game_catalog"),
)
sobrescribir(gold.game_versions(games, relations, platforms, my_platforms), G("game_versions"))
sobrescribir(gold.game_releases(silver("releases"), games, platforms), G("game_releases"))

if my_library is not None:
    sobrescribir(gold.my_library(my_library, games, platforms), G("my_library"))
else:
    print("  gold.my_library: no hay biblioteca en silver, se omite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejemplo: todas las versiones de Ocarina of Time

# COMMAND ----------

display(
    spark.read.table(G("game_versions"))
    .filter(F.lower(F.col("original_name")).contains("ocarina of time"))
    .orderBy("version_release_date")
)
