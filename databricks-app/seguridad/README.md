# Seguridad

Pendiente (etapa 2). Un notebook `.py` que, con el SDK de Databricks y sin SQL:

- crea los grupos (por ejemplo `gamevault_ingenieros` y `gamevault_analistas`) y les asigna usuarios;
- da permisos de Unity Catalog: los ingenieros escriben en bronze, silver y gold; los analistas solo leen gold;
- da permisos sobre el share de Delta Sharing y sobre el Job.

Las credenciales de PostgreSQL nunca van en el repo: se leen del secret scope `gamevault` (respaldado por Azure Key Vault).
