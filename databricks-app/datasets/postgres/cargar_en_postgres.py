# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Datasets · Cargar la base de origen en PostgreSQL
# MAGIC
# MAGIC Se corre **una sola vez** para simular el sistema transaccional de origen: toma los CSV que subiste a
# MAGIC `landing/postgres/` en el data lake y los escribe como tablas en la base `gamevault_source`.
# MAGIC Después el ETL (`proceso/01_bronze.py`) los lee de PostgreSQL por JDBC, como de cualquier sistema real.
# MAGIC
# MAGIC | CSV en `landing/postgres/` | Tabla en PostgreSQL |
# MAGIC | --- | --- |
# MAGIC | `rawg_games.csv` (Kaggle) | `public.rawg_games` |
# MAGIC | `mis_plataformas.csv` | `public.mis_plataformas` |
# MAGIC | `mi_biblioteca.csv` | `public.mi_biblioteca` |
# MAGIC
# MAGIC Solo PySpark: lee con `spark.read.csv` y escribe con `df.write.format("jdbc")`.

# COMMAND ----------

dbutils.widgets.text("ruta_csv", "abfss://landing@stgamevaultbg64.dfs.core.windows.net/postgres", "Carpeta de los CSV")
dbutils.widgets.text(
    "pg_url",
    "jdbc:postgresql://pg-gamevault-bg64.postgres.database.azure.com:5432/gamevault_source?sslmode=require",
    "JDBC de la base de origen",
)

ruta_csv = dbutils.widgets.get("ruta_csv")
pg_url = dbutils.widgets.get("pg_url")
usuario = dbutils.secrets.get("gamevault", "pg-usuario")
clave = dbutils.secrets.get("gamevault", "pg-clave")

TABLAS = ("rawg_games", "mis_plataformas", "mi_biblioteca")

# COMMAND ----------

for tabla in TABLAS:
    df = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(f"{ruta_csv}/{tabla}.csv")
    )
    (
        df.write.format("postgresql")
        .mode("overwrite")
        .option("host", "pg-gamevault-bg64.postgres.database.azure.com")
        .option("port", "5432")
        .option("database", "gamevault_source")
        .option("dbtable", f"public.{tabla}")
        .option("user", usuario)
        .option("password", clave)
        .save()
    )
    print(f"  {tabla:<18} {df.count():>9,} filas -> gamevault_source.public.{tabla}")