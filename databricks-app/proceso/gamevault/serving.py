"""Publicación de gold en la base de servicio (PostgreSQL) para Power BI.

PostgreSQL y Power BI trabajan mejor con columnas simples, así que las listas
se convierten en texto separado por comas y las estructuras en JSON.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

# Tablas de gold que se publican
TABLAS_GOLD = ("game_catalog", "game_versions", "game_releases", "my_library")


def columnas_simples(df: DataFrame) -> DataFrame:
    """Listas -> 'a, b, c'; estructuras y mapas -> JSON; el resto igual."""
    columnas = []
    for campo in df.schema.fields:
        c = F.col(campo.name)
        if isinstance(campo.dataType, ArrayType) and not isinstance(
            campo.dataType.elementType, (ArrayType, StructType, MapType)
        ):
            c = F.concat_ws(", ", c.cast("array<string>"))
        elif isinstance(campo.dataType, (ArrayType, StructType, MapType)):
            c = F.to_json(c)
        columnas.append(c.alias(campo.name))
    return df.select(*columnas)


def publicar(df: DataFrame, url: str, tabla: str, usuario: str, clave: str) -> int:
    """Reemplaza la tabla en PostgreSQL con el contenido de df (por JDBC, sin SQL)."""
    df = columnas_simples(df)
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", url)
        .option("dbtable", f"public.{tabla}")
        .option("user", usuario)
        .option("password", clave)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10000)
        .save()
    )
    return df.count()
