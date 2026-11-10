# Datasets

Insumos del ETL. Los archivos grandes no se suben al repo; aquí está de dónde salen y cómo obtenerlos.

| Fuente | Origen | En el lakehouse |
| --- | --- | --- |
| IGDB (JSONL por endpoint: games, platforms, release_dates, game_time_to_beats, ...) | API de IGDB (Twitch). Se descarga con [`igdb/igdb_descarga.py`](igdb/igdb_descarga.py), que lee `IGDB_CLIENT_ID` e `IGDB_CLIENT_SECRET` del entorno | Etapa 1: Volume `bronze/raw/igdb/`. Etapa 2: ADLS `landing/igdb/` |
| RAWG (`rawg_games`) | Kaggle: `jummyegg/rawg-game-dataset` (`game_info.csv`) | Etapa 1: `bronze/raw/postgres/rawg_games.csv`. Etapa 2: tabla en PostgreSQL |
| Biblioteca personal (`mis_plataformas`, `mi_biblioteca`) | Plantillas en [`postgres/`](postgres/) | Etapa 1: `bronze/raw/postgres/`. Etapa 2: tablas en PostgreSQL |

Columnas de la biblioteca:
- `mis_plataformas.csv`: `platform_name`
- `mi_biblioteca.csv`: `game_name`, `platform_name`, `formato`, `estado`
