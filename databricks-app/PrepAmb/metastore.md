# Metastore de Unity Catalog

El proyecto usa un metastore propio en lugar del que Azure crea por defecto, para tener control total
sobre su almacenamiento raíz y sus administradores.

| Propiedad | Valor |
| --- | --- |
| Nombre | `metastore-gamevault` |
| Región | `eastus2` (la misma del workspace) |
| Almacenamiento raíz | `abfss://metastore@stgamevaultbg64.dfs.core.windows.net/` |
| Access Connector | `ac-gamevault` |
| Workspace asignado | `dbw-gamevault` |

## Cómo se creó

1. **Contenedor raíz:** se creó el contenedor `metastore` en la cuenta ADLS Gen2 `stgamevaultbg64`.
2. **Consola de cuenta:** en https://accounts.azuredatabricks.net (requiere un usuario de organización de Entra ID
   con rol de administrador global; las cuentas personales de Microsoft no pueden entrar).
3. **Metastore por defecto:** se borró `metastore_azure_eastus2`, porque Azure permite un solo metastore por región.
4. **Metastore nuevo:** Catalog → Create metastore, con el almacenamiento raíz, el Access Connector y la identidad de la tabla.
5. **Asignación:** se asignó al workspace `dbw-gamevault` y se dejó como administrador al usuario del proyecto.
6. **Credencial y ubicaciones externas:** `preparar_ambiente.py` crea la storage credential `cred_gamevault`
   y las ubicaciones externas `ext_landing`, `ext_bronze` y `ext_catalogo` sobre ella.

Evidencia: `evidencias/metastore_gamevault.png`.
