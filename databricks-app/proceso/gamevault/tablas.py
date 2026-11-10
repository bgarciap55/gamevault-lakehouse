"""Lectura de bronze y escritura de tablas Delta, sin SQL.

- leer_bronze: los archivos parquet o avro que dejó 01_bronze.
- sobrescribir: reconstruye una tabla entera (tablas derivadas por completo de bronze).
- upsert: MERGE con la API Python de DeltaTable; actualiza lo que cambió y añade lo nuevo.
"""

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gamevault.config import Config


def nombre_tabla(cfg: Config, esquema: str, tabla: str) -> str:
    return f"{cfg.catalogo}.{esquema}.{tabla}"


def leer_bronze(spark: SparkSession, cfg: Config, carpeta: str):
    """DataFrame de una entidad de bronze, o None si esa fuente no se cargó."""
    try:
        return spark.read.format(cfg.formato_bronze).load(f"{cfg.ruta_bronze}/{carpeta}")
    except AnalysisException as e:
        if "PATH_NOT_FOUND" in str(e):
            return None
        raise


def sobrescribir(df: DataFrame, tabla: str) -> int:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabla)
    filas = df.sparkSession.read.table(tabla).count()
    print(f"  {tabla:<40} {filas:>9,} filas (overwrite)")
    return filas


def upsert(df: DataFrame, tabla: str, clave: str) -> None:
    """Crea la tabla la primera vez; después hace MERGE por la clave.

    Una fila existente solo se actualiza si su updated_at es más reciente.
    """
    from delta.tables import DeltaTable  # disponible en Databricks, no en las pruebas locales

    spark = df.sparkSession
    if not spark.catalog.tableExists(tabla):
        df.write.format("delta").saveAsTable(tabla)
        print(f"  {tabla:<40} {df.count():>9,} filas (creada)")
        return

    (
        DeltaTable.forName(spark, tabla)
        .alias("t")
        .merge(df.alias("s"), F.col(f"t.{clave}") == F.col(f"s.{clave}"))
        .whenMatchedUpdateAll(condition=F.col("s.updated_at") > F.col("t.updated_at"))
        .whenNotMatchedInsertAll()
        .execute()
    )
    ultima = DeltaTable.forName(spark, tabla).history(1).select("operationMetrics").first()[0]
    print(
        f"  {tabla:<40} {int(ultima.get('numTargetRowsUpdated', 0)):>9,} actualizadas, "
        f"{int(ultima.get('numTargetRowsInserted', 0)):,} nuevas (merge)"
    )
