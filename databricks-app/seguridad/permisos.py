# Databricks notebook source
# MAGIC %md
# MAGIC # Seguridad · Grupos y permisos
# MAGIC
# MAGIC Da permisos de Unity Catalog y del Job a dos grupos, con el principio de mínimo privilegio:
# MAGIC
# MAGIC | Grupo | Catálogo | bronze / silver | gold | Ubicaciones externas | Job |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | `gamevault_ingenieros` | USE CATALOG | leer y escribir | leer y escribir | leer landing, leer y escribir bronze | ejecutar |
# MAGIC | `gamevault_analistas` | USE CATALOG | sin acceso | solo leer | sin acceso | ver |
# MAGIC
# MAGIC Los grupos son de la **cuenta** de Databricks (Unity Catalog no acepta grupos locales del workspace).
# MAGIC Se crean una vez en Settings > Identity and access > Groups > Add group, y ahí se les agregan usuarios.
# MAGIC Si un grupo todavía no existe, el notebook lo avisa y sigue con el otro, para no frenar el ETL.
# MAGIC
# MAGIC No usa `GRANT` en SQL: los permisos van por la API de Unity Catalog a través del SDK de Databricks.
# MAGIC Se puede correr cuantas veces se quiera: dar un permiso que ya existe no cambia nada.

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.dropdown("modo_fuente", "volume", ["volume", "externo"], "Modo de fuente")
dbutils.widgets.text("grupo_ingenieros", "gamevault_ingenieros", "Grupo de ingenieros")
dbutils.widgets.text("grupo_analistas", "gamevault_analistas", "Grupo de analistas")
dbutils.widgets.text("job_id", "", "Id. del Job (lo llena el Job solo)")

catalogo = dbutils.widgets.get("catalogo")
modo_fuente = dbutils.widgets.get("modo_fuente")
ingenieros = dbutils.widgets.get("grupo_ingenieros")
analistas = dbutils.widgets.get("grupo_analistas")
job_id = dbutils.widgets.get("job_id")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Permisos de Unity Catalog por grupo: (tipo de objeto, nombre completo, privilegios)
PERMISOS = {
    ingenieros: [
        ("catalog", catalogo, ["USE_CATALOG"]),
        *[
            ("schema", f"{catalogo}.{esquema}", ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"])
            for esquema in ("bronze", "silver", "gold")
        ],
    ],
    analistas: [
        ("catalog", catalogo, ["USE_CATALOG"]),
        ("schema", f"{catalogo}.gold", ["USE_SCHEMA", "SELECT"]),
    ],
}
if modo_fuente == "externo":
    # Los ingenieros leen la fuente (landing) y escriben los archivos de bronze
    PERMISOS[ingenieros] += [
        ("external_location", "ext_landing", ["READ_FILES"]),
        ("external_location", "ext_bronze", ["READ_FILES", "WRITE_FILES"]),
    ]

# Permisos sobre el Job
PERMISOS_JOB = {ingenieros: "CAN_MANAGE_RUN", analistas: "CAN_VIEW"}


def grupo_existe(nombre):
    return any(True for _ in w.groups.list(filter=f'displayName eq "{nombre}"', attributes="id"))


def dar_permisos(tipo, nombre, grupo, privilegios):
    # PATCH de la API de permisos de Unity Catalog: agrega privilegios sin quitar los que ya hay
    w.api_client.do(
        "PATCH",
        f"/api/2.1/unity-catalog/permissions/{tipo}/{nombre}",
        body={"changes": [{"principal": grupo, "add": privilegios}]},
    )
    print(f"  {tipo:<18} {nombre:<28} {', '.join(privilegios)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog y Job

# COMMAND ----------

faltantes = []
for grupo, permisos in PERMISOS.items():
    if not grupo_existe(grupo):
        faltantes.append(grupo)
        print(f"Grupo {grupo}: no existe todavía, se omite (créalo en Settings > Identity and access > Groups)")
        continue
    print(f"Grupo {grupo}:")
    for tipo, nombre, privilegios in permisos:
        dar_permisos(tipo, nombre, grupo, privilegios)
    if job_id:
        w.api_client.do(
            "PATCH",
            f"/api/2.0/permissions/jobs/{job_id}",
            body={"access_control_list": [{"group_name": grupo, "permission_level": PERMISOS_JOB[grupo]}]},
        )
        print(f"  {'job':<18} {job_id:<28} {PERMISOS_JOB[grupo]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia: permisos efectivos

# COMMAND ----------

for tipo, nombre in [("catalog", catalogo), ("schema", f"{catalogo}.gold"), ("schema", f"{catalogo}.silver")]:
    respuesta = w.api_client.do("GET", f"/api/2.1/unity-catalog/permissions/{tipo}/{nombre}")
    print(f"{tipo} {nombre}")
    for asignacion in respuesta.get("privilege_assignments", []):
        print(f"  {asignacion['principal']:<28} {', '.join(sorted(asignacion.get('privileges', [])))}")

if faltantes:
    print(f"\nFaltan grupos: {', '.join(faltantes)}. Créalos y vuelve a correr este notebook o el Job.")
