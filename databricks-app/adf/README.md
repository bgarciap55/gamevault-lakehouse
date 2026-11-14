# Azure Data Factory

Orquestación del ETL. Exportado desde ADF Studio (Administrar > Plantilla de ARM > Exportar plantilla de ARM).

| Objeto | Nombre | Qué hace |
| --- | --- | --- |
| Data Factory | `adf-gamevault-bg64` | Servicio de orquestación en `rg-gamevault` |
| Servicio vinculado | `ls_databricks` | Conexión a `dbw-gamevault` con la identidad administrada de ADF (rol *Contributor* sobre el workspace), sin tokens |
| Canalización | `pl_gamevault_etl` | Una actividad *Trabajo* (Databricks Job) que lanza `gamevault_etl_prod` |
| Desencadenador | `tr_semanal` | Programación semanal: lunes 6:00 (hora de Bogotá) |

ADF solo orquesta: la ingesta y las transformaciones las hace Databricks (bronze, silver, gold y serving).

## Archivos

- `ARMTemplateForFactory.json`: definición del linked service, la canalización y el desencadenador.
- `ARMTemplateParametersForFactory.json`: parámetros (nombre del factory).

Para recrearlo: portal de Azure > Implementar una plantilla personalizada > cargar los dos archivos.
