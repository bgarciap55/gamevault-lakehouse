"""Lectura de las dos fuentes de GameVault y escritura de bronze.

Es el único módulo que sabe de dónde vienen los datos:

- modo "volume" (etapa 1): todo está en un Volume de Unity Catalog. Las tablas
  de PostgreSQL llegan exportadas como CSV.
- modo "externo" (etapa 2): los JSONL de IGDB están en ADLS Gen2 (abfss://) y
  las tablas se leen de PostgreSQL por JDBC.

Bronze, silver y gold no cambian al pasar de una etapa a la otra.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gamevault.config import Config

# Endpoints que descarga scripts/igdb_descarga.py (una carpeta por endpoint)
ENDPOINTS_IGDB = (
    "games",
    "platforms",
    "game_types",
    "release_date_regions",
    "genres",
    "themes",
    "game_modes",
    "release_dates",
    "game_time_to_beats",
    "franchises",
    "collections",
)

# Tablas de la base transaccional de origen
TABLAS_PG = ("rawg_games", "mis_plataformas", "mi_biblioteca")


def leer_igdb(spark: SparkSession, cfg: Config, endpoint: str) -> DataFrame:
    """JSONL de un endpoint de IGDB (un objeto por línea), con el archivo de origen."""
    return (
        spark.read.json(f"{cfg.ruta_raw}/igdb/{endpoint}")
        .withColumn("_archivo", F.col("_metadata.file_path"))
    )


def leer_tabla_pg(spark: SparkSession, cfg: Config, tabla: str) -> DataFrame:
    """Una tabla de la base de origen: CSV en el Volume (etapa 1) o PostgreSQL (etapa 2).

    En modo volume todas las columnas llegan como texto y en modo externo con
    los tipos de PostgreSQL; silver castea explícitamente, así que da igual.
    """
    if cfg.modo_fuente == "volume":
        return (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(f"{cfg.ruta_raw}/postgres/{tabla}.csv")
        )
    return (
        spark.read.format("jdbc")
        .option("url", cfg.pg_url)
        .option("dbtable", f"{cfg.pg_esquema}.{tabla}")
        .option("user", cfg.pg_usuario)
        .option("password", cfg.pg_clave)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def agregar_auditoria(df: DataFrame, origen: str) -> DataFrame:
    """Columnas de auditoría que lleva toda tabla bronze."""
    return df.withColumn("_fecha_carga", F.current_timestamp()).withColumn("_origen", F.lit(origen))


def escribir_bronze(df: DataFrame, cfg: Config, carpeta: str) -> str:
    """Escribe una entidad de bronze como parquet o avro y devuelve su ruta.

    Se sobrescribe completa en cada corrida: bronze es una foto fiel de la fuente.
    """
    ruta = f"{cfg.ruta_bronze}/{carpeta}"
    df.write.mode("overwrite").format(cfg.formato_bronze).save(ruta)
    return ruta
