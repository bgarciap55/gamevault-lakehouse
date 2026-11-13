# Evidencias

Capturas `.png` que muestran que el proyecto funciona:

| Archivo | Qué muestra |
| --- | --- |
| `workflow_ok.png` | Corrida exitosa del Job `gamevault_etl` (preparar_catalogo -> bronze -> silver -> gold) |
| `actions_ok.png` | GitHub Actions en verde (CI y CD a producción) |
| `servicios_azure.png` | Grupo de recursos en Azure: Databricks, ADLS, PostgreSQL, Key Vault, Data Factory |
| `arquitectura.png` | Diagrama de la arquitectura |
| `metastore_gamevault.png` | Metastore propio de Unity Catalog asignado al workspace |
| `credencial_identidad_administrada.png` | Storage credential con la identidad administrada id-gamevault |
| `ubicacion_externa_landing.png` | Ubicación externa del contenedor landing |
| `test_ubicacion_externa.png` | Prueba de conexión de la ubicación externa |
| `job_etl_dev_ok.png` | Job gamevault_etl_dev completo en Azure (serverless) |
| `catalogo_gold_dev.png` | Tablas gold de gamevault_dev guardadas en el data lake |
