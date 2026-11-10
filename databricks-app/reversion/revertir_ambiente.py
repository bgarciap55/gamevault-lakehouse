# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión · Borrar el ambiente
# MAGIC
# MAGIC Deshace lo que crean `PrepAmb/preparar_ambiente` y el ETL, en este orden:
# MAGIC
# MAGIC 1. **Tablas lógicas**: todas las tablas de `gold`, `silver` y `bronze`.
# MAGIC 2. **Rutas físicas**: los archivos parquet o avro de bronze (`ruta_bronze`).
# MAGIC 3. **Volumes** de la etapa 1 (`bronze.raw` y `bronze.archivos`), solo si se pide.
# MAGIC 4. **Esquemas** `gold`, `silver` y `bronze`.
# MAGIC
# MAGIC El catálogo no se borra: en Free Edition se crea desde la interfaz, así que se borra desde ahí.
# MAGIC No usa SQL: todo va por el SDK de Databricks y `dbutils.fs`.
# MAGIC
# MAGIC **No tiene vuelta atrás.** Para que corra hay que escribir `BORRAR` en el widget `confirmar`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.text("ruta_bronze", "", "Ruta bronze (vacío = Volume)")
dbutils.widgets.dropdown("borrar_volumes", "no", ["no", "si"], "Borrar también los Volumes (datos crudos)")
dbutils.widgets.text("confirmar", "", "Escribe BORRAR para confirmar")

catalogo = dbutils.widgets.get("catalogo")
ruta_bronze = dbutils.widgets.get("ruta_bronze") or f"/Volumes/{catalogo}/bronze/archivos"
borrar_volumes = dbutils.widgets.get("borrar_volumes") == "si"

if dbutils.widgets.get("confirmar") != "BORRAR":
    dbutils.notebook.exit("No se borró nada: escribe BORRAR en el widget 'confirmar' para continuar.")

if catalogo.endswith("_prod"):
    print(f"ATENCIÓN: vas a borrar el ambiente de producción ({catalogo}).")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

w = WorkspaceClient()
ESQUEMAS = ["gold", "silver", "bronze"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tablas lógicas

# COMMAND ----------

for esquema in ESQUEMAS:
    try:
        tablas = list(w.tables.list(catalog_name=catalogo, schema_name=esquema))
    except NotFound:
        print(f"Esquema {esquema}: no existe")
        continue
    for tabla in tablas:
        w.tables.delete(tabla.full_name)
        print(f"Tabla {tabla.full_name}: borrada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Rutas físicas de bronze

# COMMAND ----------

try:
    for carpeta in dbutils.fs.ls(ruta_bronze):
        dbutils.fs.rm(carpeta.path, recurse=True)
        print(f"Ruta {carpeta.path}: borrada")
except Exception as e:
    if "FileNotFoundException" in str(e) or "NOT_FOUND" in str(e):
        print(f"Ruta {ruta_bronze}: no existe")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volumes y 4. esquemas

# COMMAND ----------

if borrar_volumes:
    for volume in ["raw", "archivos"]:
        try:
            w.volumes.delete(f"{catalogo}.bronze.{volume}")
            print(f"Volume bronze.{volume}: borrado")
        except NotFound:
            print(f"Volume bronze.{volume}: no existe")

for esquema in ESQUEMAS:
    try:
        # Un esquema con Volumes no se puede borrar; si los conservamos, bronze se queda
        if esquema == "bronze" and not borrar_volumes:
            print("Esquema bronze: se conserva porque guarda los Volumes con los datos crudos")
            continue
        w.schemas.delete(f"{catalogo}.{esquema}")
        print(f"Esquema {esquema}: borrado")
    except NotFound:
        print(f"Esquema {esquema}: no existe")
