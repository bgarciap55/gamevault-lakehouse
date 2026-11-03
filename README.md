# gamevault-lakehouse

Lakehouse de videojuegos en Azure Databricks: para cada juego, sus versiones, plataformas, remakes, remasters y ports.

- **Fuentes:** PostgreSQL (catálogo RAWG y biblioteca personal) y ADLS Gen2 (JSONL de la API de IGDB).
- **Capas:** bronze (parquet o avro), silver y gold (Delta), bajo Unity Catalog.
- **Regla:** solo PySpark, sin Spark SQL ni SQL. `tests/test_solo_pyspark.py` lo verifica en cada pull request.
- **Despliegue:** Databricks Asset Bundle con targets `dev` y `prod`.

## Estructura

```
databricks.yml                 bundle: variables y targets dev / prod
resources/gamevault_etl.job.yml  Job: preparar_catalogo -> bronze -> silver -> gold
src/gamevault/config.py        configuración de una corrida
src/gamevault/fuentes.py       lectura de las fuentes (único lugar que cambia entre etapas)
src/gamevault/silver.py        transformaciones bronze -> silver (funciones puras)
src/gamevault/gold.py          explorador de versiones y plataformas (funciones puras)
src/gamevault/tablas.py        lectura de bronze y escritura Delta (overwrite y MERGE)
src/notebooks/                 notebooks que ejecuta el Job
datos_ejemplo/postgres/        plantillas de tu biblioteca (mis_plataformas, mi_biblioteca)
tests/                         pruebas con Spark local (pytest)
.github/workflows/ci.yml       pruebas y validación del bundle
```

## Tablas

| Capa | Tablas |
| --- | --- |
| bronze (parquet/avro) | una carpeta por endpoint de IGDB y por tabla de PostgreSQL |
| silver (Delta) | `game_types`, `platforms`, `games`, `game_relations`, `releases`, `time_to_beat`, `rawg_games`, `rawg_game_platforms`, `source_ids`, `my_platforms`, `my_library` |
| gold (Delta) | `game_catalog`, `game_versions`, `game_releases`, `my_library` |

## Etapas

| | Etapa 1: `modo_fuente=volume` | Etapa 2: `modo_fuente=externo` |
| --- | --- | --- |
| JSONL de IGDB | `/Volumes/<catálogo>/bronze/raw/igdb/<endpoint>/` | `abfss://landing@<cuenta>.dfs.core.windows.net/igdb/<endpoint>/` |
| RAWG y biblioteca | `/Volumes/<catálogo>/bronze/raw/postgres/<tabla>.csv` | PostgreSQL por JDBC |
| Salida de bronze | `/Volumes/<catálogo>/bronze/archivos/` | `abfss://bronze@<cuenta>.dfs.core.windows.net/` |

Para pasar a la etapa 2 solo cambian las variables del bundle; el código de bronze, silver y gold es el mismo.

## Cómo desplegar

1. Instala la [CLI de Databricks](https://docs.databricks.com/aws/en/dev-tools/cli/install) e inicia sesión:
   `databricks auth login --host https://<tu-workspace>`
2. Despliega en dev: `databricks bundle deploy -t dev`
3. En Free Edition, crea primero el catálogo `gamevault_dev` desde Catalog > + > Create catalog (Default Storage); por código no se puede. Después crea los esquemas y los Volumes corriendo solo la primera tarea: `databricks bundle run gamevault_etl -t dev --only preparar_catalogo`
4. Sube los datos al Volume `bronze.raw` (Catalog Explorer > Upload) con esta forma:
   - `igdb/games/*.jsonl`, `igdb/platforms/*.jsonl`, ... (lo que genera `igdb_descarga.py`)
   - `postgres/rawg_games.csv` (el CSV de RAWG de Kaggle, renombrado)
   - `postgres/mis_plataformas.csv` y `postgres/mi_biblioteca.csv` (copia las de `datos_ejemplo/` y edítalas)
5. Corre el Job completo: `databricks bundle run gamevault_etl -t dev`

## Pruebas locales

Requiere Python 3.11 y Java 17.

```
pip install -r requirements-dev.txt
pytest
```
