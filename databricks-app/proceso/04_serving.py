# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Serving (gold -> PostgreSQL)
# MAGIC
# MAGIC Copia las tablas de gold a la base transaccional de servicio (`gamevault_serving` en PostgreSQL),
# MAGIC que es de donde lee el tablero de Power BI. Escribe con el conector nativo `postgresql` de PySpark (funciona en serverless), sin SQL.
# MAGIC
# MAGIC Solo corre en la etapa 2 (`modo_fuente=externo`) y si el bundle define `pg_url_serving`.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath("."))

from gamevault.config import Config
from gamevault.serving import TABLAS_GOLD, publicar
from gamevault.tablas import nombre_tabla

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.dropdown("modo_fuente", "volume", ["volume", "externo"], "Modo de fuente")
dbutils.widgets.text("pg_url_serving", "", "JDBC de la base de servicio")

cfg = Config(catalogo=dbutils.widgets.get("catalogo"))
url = dbutils.widgets.get("pg_url_serving")

if dbutils.widgets.get("modo_fuente") != "externo" or not url:
    dbutils.notebook.exit("Serving omitido: solo aplica en la etapa 2 con pg_url_serving definido.")

# Las credenciales salen del secret scope "gamevault" (Azure Key Vault)
usuario = dbutils.secrets.get("gamevault", "pg-usuario")
clave = dbutils.secrets.get("gamevault", "pg-clave")

# COMMAND ----------

for tabla in TABLAS_GOLD:
    origen = nombre_tabla(cfg, "gold", tabla)
    if not spark.catalog.tableExists(origen):
        print(f"  {origen:<40} no existe, se omite")
        continue
    filas = publicar(spark.read.table(origen), url, tabla, usuario, clave)
    print(f"  {origen:<40} {filas:>9,} filas -> public.{tabla}")
