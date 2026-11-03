"""Transformaciones de bronze a silver (Transform).

Cada función recibe DataFrames de bronze y devuelve una tabla silver: una fila
por cosa, tipos correctos, listas como arrays y sin duplicados. No leen ni
escriben nada, así se prueban con Spark local.

| Tabla silver          | Una fila por                                   | Fuente      |
| --------------------- | ---------------------------------------------- | ----------- |
| game_types            | tipo de juego                                  | IGDB        |
| platforms             | plataforma                                     | IGDB        |
| games                 | juego (incluidos remakes, ports, ediciones)    | IGDB        |
| game_relations        | par original -> versión relacionada            | IGDB        |
| releases              | lanzamiento en una plataforma y región         | IGDB        |
| time_to_beat          | juego, con horas para terminarlo               | IGDB        |
| rawg_games            | juego del catálogo RAWG                        | PostgreSQL  |
| rawg_game_platforms   | juego RAWG y plataforma                        | PostgreSQL  |
| source_ids            | pareja juego IGDB <-> juego RAWG               | las dos     |
| my_platforms          | plataforma que tienes                          | PostgreSQL  |
| my_library            | juego de tu biblioteca, ya emparejado con IGDB | las dos     |
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Regiones antiguas de IGDB, por si no se descargó release_date_regions
REGIONES_IGDB = [
    (1, "europe"), (2, "north_america"), (3, "australia"), (4, "new_zealand"), (5, "japan"),
    (6, "china"), (7, "asia"), (8, "worldwide"), (9, "korea"), (10, "brazil"),
]


# Si un mismo par aparece con varias relaciones, gana la primera de esta lista
PRIORIDAD_RELACIONES = ["remake", "remaster", "port", "expanded_game", "edition", "addon"]


def columna(df: DataFrame, nombre: str, tipo: str = "string"):
    """La columna si existe; si no, nulos. IGDB agrega y renombra campos con el tiempo."""
    return F.col(nombre).cast(tipo) if nombre in df.columns else F.lit(None).cast(tipo)


def fecha_unix(df: DataFrame, nombre: str):
    """Fecha a partir de segundos desde 1970, como las da IGDB."""
    return F.to_date(F.timestamp_seconds(columna(df, nombre, "long")))


def normalizar(c):
    """Nombre para emparejar: minúsculas, solo letras y números."""
    return F.regexp_replace(F.lower(c), r"[^a-z0-9]", "")


def ultima_version(df: DataFrame, clave: str = "id") -> DataFrame:
    """Si bronze trae el mismo registro varias veces, deja el más reciente."""
    orden = [F.col("_fecha_carga").desc()] if "_fecha_carga" in df.columns else []
    if "updated_at" in df.columns:
        orden.insert(0, F.col("updated_at").desc_nulls_last())
    if not orden:
        return df.dropDuplicates([clave])
    w = Window.partitionBy(clave).orderBy(*orden)
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def game_types(b_game_types: DataFrame) -> DataFrame:
    tipos = b_game_types.select(
        F.col("id").cast("long").alias("game_type_id"),
        F.col("type").alias("game_type"),
    )
    # En IGDB "Main Game" tiene id 0 y a veces no viene en la descarga
    main_game = tipos.sparkSession.createDataFrame([(0, "Main Game")], "game_type_id long, game_type string")
    return tipos.unionByName(main_game).dropDuplicates(["game_type_id"])


def platforms(b_platforms: DataFrame) -> DataFrame:
    bp = b_platforms
    return ultima_version(bp).select(
        F.col("id").cast("long").alias("platform_id"),
        F.col("name").alias("platform_name"),
        columna(bp, "abbreviation").alias("abbreviation"),
        columna(bp, "alternative_name").alias("alternative_name"),
        columna(bp, "generation", "int").alias("generation"),
        columna(bp, "slug").alias("slug"),
        F.timestamp_seconds(columna(bp, "updated_at", "long")).alias("updated_at"),
    )


def games(b_games: DataFrame, s_game_types: DataFrame) -> DataFrame:
    bg = b_games
    limpio = (
        ultima_version(bg)
        .select(
            F.col("id").cast("long").alias("game_id"),
            F.col("name"),
            columna(bg, "slug").alias("slug"),
            columna(bg, "game_type", "long").alias("game_type_id"),
            fecha_unix(bg, "first_release_date").alias("first_release_date"),
            columna(bg, "summary").alias("summary"),
            columna(bg, "platforms", "array<long>").alias("platform_ids"),
            columna(bg, "genres", "array<long>").alias("genre_ids"),
            columna(bg, "franchises", "array<long>").alias("franchise_ids"),
            columna(bg, "version_parent", "long").alias("version_parent_id"),
            columna(bg, "version_title").alias("version_title"),
            columna(bg, "parent_game", "long").alias("parent_game_id"),
            F.round(columna(bg, "rating", "double"), 1).alias("user_rating"),
            columna(bg, "rating_count", "long").alias("user_rating_count"),
            F.round(columna(bg, "aggregated_rating", "double"), 1).alias("critic_rating"),
            columna(bg, "aggregated_rating_count", "long").alias("critic_rating_count"),
            columna(bg, "url").alias("igdb_url"),
            F.timestamp_seconds(columna(bg, "updated_at", "long")).alias("updated_at"),
        )
        .join(s_game_types, "game_type_id", "left")
    )
    return limpio.filter(calidad_games())


def calidad_games():
    """Regla de calidad de silver.games: id válido, nombre y notas entre 0 y 100."""
    nota_ok = lambda c: F.col(c).isNull() | F.col(c).between(0, 100)
    return (
        F.col("game_id").isNotNull()
        & (F.col("game_id") > 0)
        & F.col("name").isNotNull()
        & nota_ok("user_rating")
        & nota_ok("critic_rating")
    )


def game_relations(b_games: DataFrame) -> DataFrame:
    """Relaciones con la misma forma: original -> relacionado, tipo de relación.

    IGDB las da de dos maneras: listas en el original (remakes, remasters, ports,
    expanded_games) y un puntero en el derivado (version_parent, parent_game).
    """
    bg = b_games
    vacio = bg.sparkSession.createDataFrame([], "game_id long, related_game_id long, relation_type string")

    def desde_lista(campo, relacion):
        if campo not in bg.columns:
            return vacio
        return (
            bg.select(
                F.col("id").cast("long").alias("game_id"),
                F.explode(columna(bg, campo, "array<long>")).alias("related_game_id"),
            )
            .withColumn("relation_type", F.lit(relacion))
        )

    def desde_puntero(campo, relacion):
        if campo not in bg.columns:
            return vacio
        return (
            bg.select(F.col(campo).cast("long").alias("game_id"), F.col("id").cast("long").alias("related_game_id"))
            .filter(F.col("game_id").isNotNull())
            .withColumn("relation_type", F.lit(relacion))
        )

    relaciones = (
        desde_lista("remakes", "remake")
        .unionByName(desde_lista("remasters", "remaster"))
        .unionByName(desde_lista("ports", "port"))
        .unionByName(desde_lista("expanded_games", "expanded_game"))
        .unionByName(desde_puntero("version_parent", "edition"))
        .unionByName(desde_puntero("parent_game", "addon"))
        .filter(F.col("related_game_id").isNotNull())
    )
    # IGDB a veces marca el mismo par de dos formas (un remake que además tiene
    # parent_game). Nos quedamos con la relación más específica.
    prioridad = F.lit(len(PRIORIDAD_RELACIONES))
    for i, relacion in reversed(list(enumerate(PRIORIDAD_RELACIONES))):
        prioridad = F.when(F.col("relation_type") == relacion, i).otherwise(prioridad)
    w = Window.partitionBy("game_id", "related_game_id").orderBy(prioridad)
    return (
        relaciones.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def regiones(b_regions: DataFrame = None, spark=None) -> DataFrame:
    if b_regions is not None:
        return b_regions.select(F.col("id").cast("long").alias("region_id"), F.col("region"))
    return spark.createDataFrame(REGIONES_IGDB, "region_id long, region string")


def releases(b_release_dates: DataFrame, s_regiones: DataFrame) -> DataFrame:
    br = b_release_dates
    return (
        ultima_version(br)
        .select(
            F.col("id").cast("long").alias("release_id"),
            F.col("game").cast("long").alias("game_id"),
            columna(br, "platform", "long").alias("platform_id"),
            fecha_unix(br, "date").alias("release_date"),
            columna(br, "human").alias("release_date_text"),
            F.coalesce(columna(br, "release_region", "long"), columna(br, "region", "long")).alias("region_id"),
            F.timestamp_seconds(columna(br, "updated_at", "long")).alias("updated_at"),
        )
        .join(s_regiones, "region_id", "left")
    )


def time_to_beat(b_time_to_beats: DataFrame) -> DataFrame:
    """IGDB da segundos: hastily (rápido), normally y completely (100 %). Los pasamos a horas."""
    bt = b_time_to_beats
    horas = lambda c: F.round(columna(bt, c, "double") / 3600, 1)
    return (
        ultima_version(bt)
        .select(
            F.coalesce(columna(bt, "game_id", "long"), columna(bt, "game", "long")).alias("game_id"),
            horas("hastily").alias("hours_rushed"),
            horas("normally").alias("hours_normal"),
            horas("completely").alias("hours_completionist"),
            columna(bt, "count", "long").alias("submissions"),
        )
        .filter(F.col("game_id").isNotNull())
        .dropDuplicates(["game_id"])
    )


def _lista(df: DataFrame, c: str):
    """RAWG guarda listas como texto 'PC||macOS'. En PostgreSQL pueden llegar igual."""
    texto = columna(df, c)
    return F.when(texto.isNull() | (texto == ""), F.lit(None)).otherwise(F.split(texto, r"\|\|"))


def rawg_games(b_rawg: DataFrame) -> DataFrame:
    br = b_rawg
    return br.select(
        F.col("id").cast("long").alias("rawg_id"),
        F.col("slug"),
        F.col("name"),
        F.to_date(columna(br, "released")).alias("released"),
        columna(br, "tba", "boolean").alias("tba"),
        columna(br, "metacritic", "int").alias("metacritic"),
        columna(br, "rating", "double").alias("user_rating_5"),
        columna(br, "ratings_count", "long").alias("ratings_count"),
        columna(br, "playtime", "int").alias("avg_playtime_hours"),
        _lista(br, "platforms").alias("platforms"),
        _lista(br, "genres").alias("genres"),
        _lista(br, "developers").alias("developers"),
        _lista(br, "publishers").alias("publishers"),
        columna(br, "esrb_rating").alias("esrb_rating"),
        columna(br, "updated", "timestamp").alias("updated_at"),
    ).filter(F.col("rawg_id").isNotNull()).dropDuplicates(["rawg_id"])


def rawg_game_platforms(s_rawg: DataFrame) -> DataFrame:
    return s_rawg.select("rawg_id", F.explode("platforms").alias("platform_name"))


def source_ids(s_games: DataFrame, s_rawg: DataFrame) -> DataFrame:
    """Empareja IGDB con RAWG: primero por slug, luego por nombre normalizado.

    En los dos casos los años de lanzamiento pueden diferir como mucho en 1.
    Si hay varios candidatos gana el de año más cercano y más valoraciones.
    """
    igdb = s_games.select(
        "game_id",
        F.col("slug").alias("i_slug"),
        normalizar(F.col("name")).alias("i_norm"),
        F.year("first_release_date").alias("i_year"),
    )
    rw = s_rawg.select(
        "rawg_id",
        F.col("slug").alias("r_slug"),
        normalizar(F.col("name")).alias("r_norm"),
        F.year("released").alias("r_year"),
        "ratings_count",
    )
    anio_ok = F.col("i_year").isNull() | F.col("r_year").isNull() | (F.abs(F.col("i_year") - F.col("r_year")) <= 1)

    por_slug = igdb.join(rw, F.col("i_slug") == F.col("r_slug")).filter(anio_ok).withColumn("match_method", F.lit("slug"))
    sueltos = igdb.join(por_slug.select("game_id"), "game_id", "left_anti")
    por_nombre = (
        sueltos.join(rw, F.col("i_norm") == F.col("r_norm")).filter(anio_ok).withColumn("match_method", F.lit("nombre"))
    )

    w = Window.partitionBy("game_id").orderBy(
        F.abs(F.coalesce(F.col("i_year") - F.col("r_year"), F.lit(0))),
        F.col("ratings_count").desc_nulls_last(),
    )
    return (
        por_slug.unionByName(por_nombre)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("game_id", "rawg_id", "match_method")
    )


def my_platforms(b_mis_plataformas: DataFrame, s_platforms: DataFrame) -> DataFrame:
    """Tus plataformas, con el id de IGDB (se empareja por nombre o abreviatura)."""
    mias = b_mis_plataformas.select(F.trim(F.col("platform_name")).alias("platform_name_input")).dropDuplicates()
    p = s_platforms.select("platform_id", "platform_name", "abbreviation")
    clave = normalizar(F.col("platform_name_input"))
    coincide = (clave == normalizar(F.col("platform_name"))) | (clave == normalizar(F.col("abbreviation")))
    w = Window.partitionBy("platform_name_input").orderBy(F.col("platform_id"))
    return (
        mias.join(p, coincide, "left")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("platform_name_input", "platform_id", "platform_name")
    )


def my_library(b_mi_biblioteca: DataFrame, s_games: DataFrame, s_my_platforms: DataFrame) -> DataFrame:
    """Tu biblioteca: qué juego tienes, en qué plataforma, en qué formato y cómo vas.

    El juego se empareja con IGDB por nombre normalizado; si hay varios
    (original y remake con el mismo nombre) gana el más antiguo.
    """
    lib = b_mi_biblioteca.select(
        F.trim(F.col("game_name")).alias("game_name_input"),
        F.trim(F.col("platform_name")).alias("platform_name_input"),
        F.lower(F.trim(columna(b_mi_biblioteca, "formato"))).alias("formato"),
        F.lower(F.trim(columna(b_mi_biblioteca, "estado"))).alias("estado"),
    )
    g = s_games.select("game_id", F.col("name").alias("game_name"), "first_release_date")
    w = Window.partitionBy("game_name_input", "platform_name_input").orderBy(F.col("first_release_date").asc_nulls_last())
    return (
        lib.join(g, normalizar(F.col("game_name_input")) == normalizar(F.col("game_name")), "left")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .join(s_my_platforms.select("platform_name_input", "platform_id"), "platform_name_input", "left")
        .select("game_id", "game_name", "game_name_input", "platform_id", "platform_name_input", "formato", "estado")
    )
