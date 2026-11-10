"""Tablas gold (Load): el explorador de versiones y plataformas.

| Tabla gold        | Una fila por                         | Responde                                       |
| ----------------- | ------------------------------------ | ---------------------------------------------- |
| game_catalog      | juego                                | qué es, notas, horas, cuántas versiones tiene  |
| game_versions     | juego original y versión relacionada | qué remakes, remasters, ports y ediciones hay  |
| game_releases     | juego, plataforma y región           | dónde y cuándo salió                           |
| my_library        | juego de tu biblioteca               | qué tienes, en qué plataforma y cómo vas       |

Como silver, son funciones puras: reciben DataFrames y devuelven DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _nombres_plataformas(s_games: DataFrame, s_platforms: DataFrame) -> DataFrame:
    """game_id -> lista ordenada de nombres de plataforma."""
    return (
        s_games.select("game_id", F.explode("platform_ids").alias("platform_id"))
        .join(s_platforms.select("platform_id", "platform_name"), "platform_id")
        .groupBy("game_id")
        .agg(F.array_sort(F.collect_set("platform_name")).alias("platforms"))
    )


def game_versions(
    s_games: DataFrame,
    s_relations: DataFrame,
    s_platforms: DataFrame,
    s_my_platforms: DataFrame = None,
) -> DataFrame:
    """Cada versión de un juego con su tipo, fecha y plataformas.

    Si se pasan tus plataformas, marca en cuáles de ellas puedes jugar esa versión.
    """
    plataformas = _nombres_plataformas(s_games, s_platforms)
    original = s_games.select(F.col("game_id"), F.col("name").alias("original_name"))
    version = (
        s_games.select(
            F.col("game_id").alias("version_id"),
            F.col("name").alias("version_name"),
            F.col("game_type").alias("version_type"),
            F.col("first_release_date").alias("version_release_date"),
            F.col("critic_rating").alias("version_critic_rating"),
            F.col("platform_ids").alias("_platform_ids"),
        )
        .join(plataformas.withColumnRenamed("game_id", "version_id"), "version_id", "left")
    )

    versiones = (
        s_relations.join(original, "game_id")
        .join(version, F.col("related_game_id") == F.col("version_id"))
        .select(
            F.col("game_id").alias("original_id"),
            "original_name",
            "relation_type",
            "version_id",
            "version_name",
            "version_type",
            "version_release_date",
            "version_critic_rating",
            "platforms",
            "_platform_ids",
        )
    )

    if s_my_platforms is not None:
        mias = s_my_platforms.filter(F.col("platform_id").isNotNull()).agg(
            F.collect_set("platform_id").alias("_mias")
        )
        versiones = versiones.crossJoin(mias).withColumn(
            "playable_on_my_platforms",
            F.coalesce(F.arrays_overlap(F.col("_platform_ids"), F.col("_mias")), F.lit(False)),
        ).drop("_mias")

    return versiones.drop("_platform_ids")


def game_releases(s_releases: DataFrame, s_games: DataFrame, s_platforms: DataFrame) -> DataFrame:
    """Primer lanzamiento de cada juego en cada plataforma y región."""
    return (
        s_releases.join(s_games.select("game_id", F.col("name").alias("game_name")), "game_id")
        .join(s_platforms.select("platform_id", "platform_name"), "platform_id", "left")
        .groupBy("game_id", "game_name", "platform_id", "platform_name", "region")
        .agg(F.min("release_date").alias("release_date"))
    )


def game_catalog(
    s_games: DataFrame,
    s_platforms: DataFrame,
    s_relations: DataFrame,
    s_rawg: DataFrame,
    s_source_ids: DataFrame,
    s_time_to_beat: DataFrame = None,
    s_my_library: DataFrame = None,
) -> DataFrame:
    """Ficha de cada juego: IGDB, completado con RAWG, horas y si lo tienes."""
    n_versiones = s_relations.groupBy("game_id").agg(F.countDistinct("related_game_id").alias("n_versions"))
    rawg = s_source_ids.join(
        s_rawg.select("rawg_id", "metacritic", "genres", "developers", "publishers", "esrb_rating"),
        "rawg_id",
    )

    catalogo = (
        s_games.select(
            "game_id", "name", "game_type", "first_release_date", "summary",
            "user_rating", "critic_rating", "version_parent_id", "parent_game_id",
        )
        .join(_nombres_plataformas(s_games, s_platforms), "game_id", "left")
        .join(n_versiones, "game_id", "left")
        .join(rawg, "game_id", "left")
        .withColumn("n_versions", F.coalesce(F.col("n_versions"), F.lit(0)))
        .withColumn("n_platforms", F.coalesce(F.size("platforms"), F.lit(0)))
    )

    if s_time_to_beat is not None:
        catalogo = catalogo.join(
            s_time_to_beat.select("game_id", "hours_rushed", "hours_normal", "hours_completionist"), "game_id", "left"
        )

    tengo = F.lit(False)
    if s_my_library is not None:
        mios = s_my_library.filter(F.col("game_id").isNotNull()).select("game_id").distinct().withColumn("_mio", F.lit(True))
        catalogo = catalogo.join(mios, "game_id", "left")
        tengo = F.coalesce(F.col("_mio"), F.lit(False))
    return catalogo.withColumn("in_my_library", tengo).drop("_mio")


def my_library(s_my_library: DataFrame, s_games: DataFrame, s_platforms: DataFrame) -> DataFrame:
    """Tu biblioteca con nombres limpios; los juegos que no se emparejaron quedan con game_id nulo."""
    return (
        s_my_library.join(s_games.select("game_id", "game_type", "first_release_date"), "game_id", "left")
        .join(s_platforms.select("platform_id", "platform_name"), "platform_id", "left")
        .select(
            "game_id",
            F.coalesce(F.col("game_name"), F.col("game_name_input")).alias("game_name"),
            "game_type",
            "first_release_date",
            F.coalesce(F.col("platform_name"), F.col("platform_name_input")).alias("platform_name"),
            "formato",
            "estado",
            F.col("game_id").isNotNull().alias("matched_igdb"),
        )
    )
