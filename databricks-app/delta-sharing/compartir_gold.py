# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing · Compartir gold con un socio
# MAGIC
# MAGIC Un "socio" (por ejemplo, una tienda de videojuegos) quiere consultar el catálogo de versiones sin tener
# MAGIC cuenta en nuestro Databricks. Delta Sharing se lo permite sin copiar datos: el socio lee las mismas tablas
# MAGIC Delta de gold, solo en lectura y solo las que están en el share.
# MAGIC
# MAGIC Este notebook, con el SDK de Databricks y sin SQL:
# MAGIC
# MAGIC 1. crea el share `gamevault_gold` y le agrega las tablas de gold;
# MAGIC 2. crea el destinatario `socio_gamevault`, de tipo *open sharing* (token), para alguien fuera de Databricks;
# MAGIC 3. le da `SELECT` sobre el share.
# MAGIC
# MAGIC Se corre a mano, una vez por ambiente, después de que el Job haya creado gold. Se puede repetir sin problema.
# MAGIC
# MAGIC **Antes, una sola vez:** en la consola de cuenta (accounts.azuredatabricks.net) > Catalog > `metastore-gamevault`
# MAGIC hay que activar *Delta Sharing to external parties*, porque viene apagado.
# MAGIC
# MAGIC **El enlace de activación** del destinatario no se imprime aquí, porque da acceso a los datos: se copia desde
# MAGIC Catalog > Delta Sharing > Shared by me > Recipients > `socio_gamevault` y se le envía al socio.
# MAGIC El socio descarga con él su archivo de credenciales (`config.share`) y lee las tablas como en `leer_como_socio.py`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "gamevault_dev", "Catálogo")
dbutils.widgets.text("share", "gamevault_gold", "Share")
dbutils.widgets.text("destinatario", "socio_gamevault", "Destinatario")

catalogo = dbutils.widgets.get("catalogo")
share = dbutils.widgets.get("share")
destinatario = dbutils.widgets.get("destinatario")

TABLAS_GOLD = ["game_catalog", "game_versions", "game_releases"]
# my_library no se comparte: es la biblioteca personal

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.sharing import AuthenticationType

w = WorkspaceClient()


def existe(leer):
    try:
        leer()
        return True
    except NotFound:
        return False


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Share con las tablas de gold

# COMMAND ----------

if existe(lambda: w.shares.get(share)):
    print(f"Share {share}: ya existe")
else:
    w.shares.create(name=share, comment="Catálogo de versiones, remakes y lanzamientos de GameVault (gold)")
    print(f"Share {share}: creado")

info = w.api_client.do("GET", f"/api/2.1/unity-catalog/shares/{share}", query={"include_shared_data": "true"})
ya_compartidas = {objeto["name"] for objeto in info.get("objects", [])}

nuevas = [f"{catalogo}.gold.{tabla}" for tabla in TABLAS_GOLD if f"{catalogo}.gold.{tabla}" not in ya_compartidas]
if nuevas:
    w.api_client.do(
        "PATCH",
        f"/api/2.1/unity-catalog/shares/{share}",
        body={
            "updates": [
                # history_data_sharing_status: el socio puede leer versiones anteriores (time travel)
                {
                    "action": "ADD",
                    "data_object": {
                        "name": tabla,
                        "data_object_type": "TABLE",
                        "history_data_sharing_status": "ENABLED",
                    },
                }
                for tabla in nuevas
            ]
        },
    )
for tabla in TABLAS_GOLD:
    nombre = f"{catalogo}.gold.{tabla}"
    print(f"  {nombre:<40} {'agregada' if nombre in nuevas else 'ya estaba'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Destinatario y permiso

# COMMAND ----------

if existe(lambda: w.recipients.get(destinatario)):
    print(f"Destinatario {destinatario}: ya existe")
else:
    w.recipients.create(
        name=destinatario,
        authentication_type=AuthenticationType.TOKEN,
        comment="Socio externo que consulta el catálogo de versiones (open sharing)",
    )
    print(f"Destinatario {destinatario}: creado. Copia su enlace de activación desde Catalog > Delta Sharing.")

w.api_client.do(
    "PATCH",
    f"/api/2.1/unity-catalog/shares/{share}/permissions",
    body={"changes": [{"principal": destinatario, "add": ["SELECT"]}]},
)
print(f"Permiso SELECT sobre {share} para {destinatario}: dado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidencia

# COMMAND ----------

info = w.api_client.do("GET", f"/api/2.1/unity-catalog/shares/{share}", query={"include_shared_data": "true"})
print(f"Share {share}:")
for objeto in info.get("objects", []):
    print(f"  {objeto['data_object_type']:<6} {objeto['name']}")

permisos = w.api_client.do("GET", f"/api/2.1/unity-catalog/shares/{share}/permissions")
for asignacion in permisos.get("privilege_assignments", []):
    print(f"Destinatario {asignacion['principal']}: {', '.join(asignacion.get('privileges', []))}")
