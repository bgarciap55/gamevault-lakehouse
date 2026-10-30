import json

import pytest

from gamevault.config import Config
from gamevault.fuentes import agregar_auditoria, escribir_bronze, leer_igdb, leer_tabla_pg


@pytest.fixture
def raw(tmp_path):
    """Imita el Volume bronze.raw de la etapa 1."""
    igdb = tmp_path / "raw" / "igdb" / "games"
    igdb.mkdir(parents=True)
    juegos = [
        {"id": 1029, "name": "The Legend of Zelda: Ocarina of Time", "game_type": 0, "remakes": [1039]},
        {"id": 1039, "name": "The Legend of Zelda: Ocarina of Time 3D", "game_type": 8},
    ]
    (igdb / "games_20261030.jsonl").write_text("\n".join(json.dumps(j) for j in juegos), encoding="utf-8")

    postgres = tmp_path / "raw" / "postgres"
    postgres.mkdir()
    # Igual que el CSV de RAWG: listas separadas por '||' y texto con comas y saltos de línea
    (postgres / "rawg_games.csv").write_text(
        'id,name,platforms,description\n'
        '1,"Ocarina, of Time",Nintendo 64||Nintendo 3DS,"Línea 1\nLínea 2"\n',
        encoding="utf-8",
    )
    return tmp_path


def config(raw, **cambios):
    valores = dict(catalogo="gamevault_test", ruta_raw=str(raw / "raw"), ruta_bronze=str(raw / "bronze"))
    valores.update(cambios)
    return Config(**valores)


def test_volume_usa_rutas_del_catalogo_por_defecto():
    cfg = Config(catalogo="gamevault_dev")
    assert cfg.ruta_raw == "/Volumes/gamevault_dev/bronze/raw"
    assert cfg.ruta_bronze == "/Volumes/gamevault_dev/bronze/archivos"


@pytest.mark.parametrize(
    "cambios",
    [
        dict(modo_fuente="s3"),
        dict(formato_bronze="csv"),
        dict(modo_fuente="externo", ruta_raw="abfss://landing@x.dfs.core.windows.net", ruta_bronze="abfss://bronze@x.dfs.core.windows.net"),
        dict(modo_fuente="externo", pg_url="jdbc:postgresql://x:5432/gamevault"),
    ],
)
def test_config_invalida(cambios):
    with pytest.raises(ValueError):
        Config(catalogo="gamevault_dev", **cambios)


def test_clave_no_aparece_al_imprimir_config():
    cfg = Config(
        catalogo="gamevault_dev",
        modo_fuente="externo",
        ruta_raw="abfss://landing@x.dfs.core.windows.net",
        ruta_bronze="abfss://bronze@x.dfs.core.windows.net",
        pg_url="jdbc:postgresql://x:5432/gamevault",
        pg_clave="secreta",
    )
    assert "secreta" not in repr(cfg)


def test_leer_igdb_desde_volume(spark, raw):
    df = leer_igdb(spark, config(raw), "games")
    filas = {r["id"]: r for r in df.collect()}
    assert set(filas) == {1029, 1039}
    assert filas[1029]["remakes"] == [1039]
    assert filas[1029]["_archivo"].endswith("games_20261030.jsonl")


def test_leer_tabla_pg_desde_csv_respeta_comillas_y_saltos(spark, raw):
    fila = leer_tabla_pg(spark, config(raw), "rawg_games").collect()[0]
    assert fila["name"] == "Ocarina, of Time"
    assert fila["platforms"] == "Nintendo 64||Nintendo 3DS"
    assert fila["description"] == "Línea 1\nLínea 2"


def test_escribir_bronze_con_auditoria(spark, raw):
    cfg = config(raw)
    df = agregar_auditoria(leer_igdb(spark, cfg, "games"), "igdb")
    ruta = escribir_bronze(df, cfg, "igdb/games")

    leido = spark.read.parquet(ruta)
    assert leido.count() == 2
    assert {"_fecha_carga", "_origen", "_archivo"} <= set(leido.columns)
    assert {r["_origen"] for r in leido.collect()} == {"igdb"}
