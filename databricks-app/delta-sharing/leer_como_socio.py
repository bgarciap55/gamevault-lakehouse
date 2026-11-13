# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing · Cómo lee el socio
# MAGIC
# MAGIC Esto lo corre el **socio**, no nosotros: solo necesita Python, la librería abierta `delta-sharing` y el archivo
# MAGIC de credenciales `config.share` que descargó con el enlace de activación. No necesita cuenta de Databricks ni de Azure.
# MAGIC
# MAGIC Para probarlo nosotros mismos: descarga `config.share` con el enlace, súbelo a tu carpeta del workspace (o a un
# MAGIC Volume) y pon su ruta en el widget. **No lo subas al repo**: es una credencial (el `.gitignore` lo excluye).
# MAGIC
# MAGIC La URL de cada tabla es `<archivo de perfil>#<share>.<esquema>.<tabla>`.

# COMMAND ----------

# MAGIC %pip install delta-sharing

# COMMAND ----------

dbutils.widgets.text("perfil", "/Workspace/Users/<tu-usuario>/config.share", "Ruta de config.share")
dbutils.widgets.text("share", "gamevault_gold", "Share")

perfil = dbutils.widgets.get("perfil")
share = dbutils.widgets.get("share")

# COMMAND ----------

import delta_sharing
from pyspark.sql import functions as F

cliente = delta_sharing.SharingClient(perfil)
print("Tablas que el socio puede ver:")
for tabla in cliente.list_all_tables():
    print(f"  {tabla.share}.{tabla.schema}.{tabla.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC El cliente abierto devuelve pandas; se pasa a un DataFrame de PySpark para trabajar igual que en el ETL.

# COMMAND ----------

COLUMNAS = ["original_name", "relation_type", "version_name", "version_type", "version_release_date"]
crudo = delta_sharing.load_as_pandas(f"{perfil}#{share}.gold.game_versions")
versiones = spark.createDataFrame(crudo[COLUMNAS])

(
    versiones.filter(F.col("original_name") == "The Legend of Zelda: Ocarina of Time")
    .select(*COLUMNAS[1:])
    .orderBy("version_release_date")
    .show(truncate=False)
)
print(f"game_versions compartida: {versiones.count():,} filas")
