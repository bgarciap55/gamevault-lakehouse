# Seguridad

`permisos.py` da permisos de Unity Catalog y del Job a dos grupos, con el SDK de Databricks y sin SQL (no hay `GRANT`).
Corre como tarea `permisos` del Job `gamevault_etl`, después de `preparar_catalogo`, así que cada ambiente (dev y prod)
queda con los mismos permisos.

| Grupo | Catálogo | bronze / silver | gold | Ubicaciones externas | Job |
| --- | --- | --- | --- | --- | --- |
| `gamevault_ingenieros` | `USE_CATALOG` | `USE_SCHEMA`, `SELECT`, `MODIFY`, `CREATE_TABLE` | igual | `READ_FILES` en `ext_landing`; `READ_FILES` y `WRITE_FILES` en `ext_bronze` | `CAN_MANAGE_RUN` |
| `gamevault_analistas` | `USE_CATALOG` | sin acceso | `USE_SCHEMA`, `SELECT` | sin acceso | `CAN_VIEW` |

## Grupos y usuarios

Unity Catalog solo acepta grupos de la cuenta, no grupos locales del workspace. Se crean una vez desde el workspace
(Settings > Identity and access > Groups > Add group) y ahí mismo se les agregan usuarios. Si un grupo no existe,
el notebook lo avisa y sigue, para no frenar el ETL.

## Otros controles del proyecto

- **Credenciales:** el usuario y la clave de PostgreSQL están en Azure Key Vault (`kv-gamevault-bg64`) y se leen con
  el secret scope `gamevault`. Nunca van en el repo.
- **Data lake:** Databricks entra con la identidad administrada `id-gamevault` (rol *Storage Blob Data Contributor*),
  a través del Access Connector `ac-gamevault`. No hay claves de la cuenta de almacenamiento en el código.
- **Data Factory:** lanza el Job con su propia identidad administrada (rol *Contributor* sobre el workspace), sin tokens.
- **GitHub Actions:** el token de Databricks está en los secretos del repo y el despliegue a prod pasa por el
  environment `produccion`.
