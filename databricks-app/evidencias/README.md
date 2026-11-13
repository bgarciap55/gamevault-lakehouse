# Evidencias

Capturas `.png` que muestran que el proyecto funciona:

| Archivo | Qué muestra |
| --- | --- |
| `workflow_ok.png` | Corrida exitosa del Job `gamevault_etl` (preparar_catalogo -> bronze -> silver -> gold) |
| `actions_ok.png` | GitHub Actions en verde (CI y CD a producción) |
| `servicios_azure.png` | Grupo de recursos en Azure: Databricks, ADLS, PostgreSQL, Key Vault, Data Factory |
| `arquitectura.png` | Diagrama de la arquitectura |
| 2026-11-13 | Azure, \gamevault_dev` (serverless) | Job completo en 3m37s leyendo de ADLS y PostgreSQL. Mismos conteos que Free Edition; gold publicado en `gamevault_serving` (game_releases 72.956 filas). Evidencia: `evidencias/job_etl_dev_ok.png`. |`
