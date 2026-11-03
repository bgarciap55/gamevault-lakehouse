"""Silver y gold con un mini catálogo: Ocarina of Time, su remake 3D y Master Quest."""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from gamevault import gold, silver

UNIX_1998 = 911_606_400   # 1998-11-21
UNIX_2011 = 1_308_787_200  # 2011-06-23


@pytest.fixture
def bronze(spark):
    b_games = spark.createDataFrame(
        [
            (1029, "The Legend of Zelda: Ocarina of Time", "the-legend-of-zelda-ocarina-of-time", 0, UNIX_1998,
             [4], [1039], None, 91.6, 100, 1),
            (1039, "The Legend of Zelda: Ocarina of Time 3D", "the-legend-of-zelda-ocarina-of-time-3d", 8, UNIX_2011,
             [37], None, None, 88.0, 50, 1),
            (1040, "The Legend of Zelda: Ocarina of Time - Master Quest", "master-quest", 4, None,
             [21], None, 1029, None, None, 1),
            # Fila repetida más vieja: ultima_version debe descartarla
            (1029, "Ocarina (vieja)", "x", 0, UNIX_1998, [4], None, None, 50.0, 1, 0),
            # Nota fuera de rango: la regla de calidad la descarta
            (9999, "Juego roto", "roto", 0, None, None, None, None, 150.0, 1, 1),
        ],
        "id long, name string, slug string, game_type long, first_release_date long, platforms array<long>, "
        "remakes array<long>, parent_game long, aggregated_rating double, aggregated_rating_count long, updated_at long",
    )
    return {
        "games": b_games,
        "game_types": spark.createDataFrame([(4, "Expansion"), (8, "Remake")], "id long, type string"),
        "platforms": spark.createDataFrame(
            [(4, "Nintendo 64", "N64", 1), (37, "Nintendo 3DS", "3DS", 1), (21, "Nintendo GameCube", "NGC", 1),
             (130, "Nintendo Switch", "Switch", 1)],
            "id long, name string, abbreviation string, updated_at long",
        ),
        "release_dates": spark.createDataFrame(
            [(1, 1029, 4, UNIX_1998, None, 2, 1), (2, 1039, 37, UNIX_2011, None, 1, 1)],
            "id long, game long, platform long, date long, human string, release_region long, updated_at long",
        ),
        "rawg": spark.createDataFrame(
            [("100", "the-legend-of-zelda-ocarina-of-time", "The Legend of Zelda: Ocarina of Time", "1998-11-23",
              "99", "4.5", "5000", "Nintendo 64||Nintendo 3DS", "Action||Adventure"),
             ("200", "otro-slug", "The Legend of Zelda: Ocarina of Time 3D", "2011-06-16", "94", "4.4", "800",
              "Nintendo 3DS", "Action")],
            "id string, slug string, name string, released string, metacritic string, rating string, "
            "ratings_count string, platforms string, genres string",
        ),
        "mis_plataformas": spark.createDataFrame([("3DS",), ("Nintendo Switch",)], "platform_name string"),
        "mi_biblioteca": spark.createDataFrame(
            [("the legend of zelda: ocarina of time 3d", "Nintendo 3DS", "Físico", "Terminado"),
             ("Juego que no existe", "Nintendo Switch", "digital", "pendiente")],
            "game_name string, platform_name string, formato string, estado string",
        ),
    }


@pytest.fixture
def s(spark, bronze):
    tipos = silver.game_types(bronze["game_types"])
    games = silver.games(bronze["games"], tipos)
    platforms = silver.platforms(bronze["platforms"])
    rawg = silver.rawg_games(bronze["rawg"])
    my_platforms = silver.my_platforms(bronze["mis_plataformas"], platforms)
    return {
        "games": games,
        "platforms": platforms,
        "relations": silver.game_relations(bronze["games"]),
        "releases": silver.releases(bronze["release_dates"], silver.regiones(None, spark)),
        "rawg": rawg,
        "source_ids": silver.source_ids(games, rawg),
        "my_platforms": my_platforms,
        "my_library": silver.my_library(bronze["mi_biblioteca"], games, my_platforms),
    }


def test_main_game_y_tipos(spark, bronze):
    tipos = {r["game_type_id"]: r["game_type"] for r in silver.game_types(bronze["game_types"]).collect()}
    assert tipos[0] == "Main Game"
    assert tipos[8] == "Remake"


def test_games_deduplica_y_aplica_calidad(s):
    filas = {r["game_id"]: r for r in s["games"].collect()}
    assert set(filas) == {1029, 1039, 1040}
    assert filas[1029]["name"] == "The Legend of Zelda: Ocarina of Time"
    assert filas[1029]["first_release_date"] == dt.date(1998, 11, 21)
    assert filas[1039]["game_type"] == "Remake"


def test_relaciones_desde_lista_y_puntero(s):
    rel = {(r["game_id"], r["related_game_id"], r["relation_type"]) for r in s["relations"].collect()}
    assert rel == {(1029, 1039, "remake"), (1029, 1040, "addon")}


def test_releases_con_region(s):
    regiones = {r["release_id"]: r["region"] for r in s["releases"].collect()}
    assert regiones == {1: "north_america", 2: "europe"}


def test_rawg_listas_y_tipos(s):
    fila = s["rawg"].filter(F.col("rawg_id") == 100).first()
    assert fila["platforms"] == ["Nintendo 64", "Nintendo 3DS"]
    assert fila["metacritic"] == 99
    assert fila["released"] == dt.date(1998, 11, 23)


def test_emparejamiento_por_slug_y_por_nombre(s):
    pares = {r["game_id"]: (r["rawg_id"], r["match_method"]) for r in s["source_ids"].collect()}
    assert pares[1029] == (100, "slug")
    assert pares[1039] == (200, "nombre")
    assert 1040 not in pares


def test_biblioteca_empareja_plataforma_por_abreviatura_y_juego_por_nombre(s):
    mias = {r["platform_name_input"]: r["platform_id"] for r in s["my_platforms"].collect()}
    assert mias == {"3DS": 37, "Nintendo Switch": 130}

    lib = {r["game_name_input"]: r for r in s["my_library"].collect()}
    assert lib["the legend of zelda: ocarina of time 3d"]["game_id"] == 1039
    assert lib["the legend of zelda: ocarina of time 3d"]["formato"] == "físico"
    assert lib["Juego que no existe"]["game_id"] is None


def test_gold_versiones_marca_si_puedo_jugarla(s):
    versiones = gold.game_versions(s["games"], s["relations"], s["platforms"], s["my_platforms"])
    filas = {r["version_id"]: r for r in versiones.collect()}
    assert filas[1039]["original_name"] == "The Legend of Zelda: Ocarina of Time"
    assert filas[1039]["relation_type"] == "remake"
    assert filas[1039]["platforms"] == ["Nintendo 3DS"]
    assert filas[1039]["playable_on_my_platforms"] is True
    assert filas[1040]["playable_on_my_platforms"] is False


def test_gold_catalogo(s):
    cat = gold.game_catalog(
        s["games"], s["platforms"], s["relations"], s["rawg"], s["source_ids"], None, s["my_library"]
    )
    filas = {r["game_id"]: r for r in cat.collect()}
    assert filas[1029]["n_versions"] == 2
    assert filas[1029]["metacritic"] == 99
    assert filas[1029]["genres"] == ["Action", "Adventure"]
    assert filas[1029]["in_my_library"] is False
    assert filas[1039]["in_my_library"] is True
    assert filas[1040]["n_platforms"] == 1


def test_gold_lanzamientos_y_biblioteca(s):
    rel = {(r["game_id"], r["platform_name"], r["region"]): r["release_date"]
           for r in gold.game_releases(s["releases"], s["games"], s["platforms"]).collect()}
    assert rel[(1029, "Nintendo 64", "north_america")] == dt.date(1998, 11, 21)

    lib = {r["game_name"]: r for r in gold.my_library(s["my_library"], s["games"], s["platforms"]).collect()}
    assert lib["The Legend of Zelda: Ocarina of Time 3D"]["platform_name"] == "Nintendo 3DS"
    assert lib["Juego que no existe"]["matched_igdb"] is False
