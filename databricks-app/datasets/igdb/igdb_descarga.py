"""
GameVault · Descarga de IGDB desde tu PC (camino 2 del plan).

Free Edition no puede salir a Twitch ni a IGDB, así que este script descarga
los datos en tu PC y los deja en archivos JSON Lines listos para subir al
Volume /Volumes/gamevault/bronze/raw/igdb/.

Solo usa la librería estándar de Python 3.9+: no hay que instalar nada.

Uso:
    python igdb_descarga.py revisar      # revisa tus juegos favoritos y muestra sus versiones
    python igdb_descarga.py descargar    # descarga los datos para bronze

Las claves se piden al ejecutar (el secreto no se ve al escribirlo) o se leen
de las variables de entorno IGDB_CLIENT_ID e IGDB_CLIENT_SECRET.
No se guardan en ningún archivo.
"""

import datetime as dt
import getpass
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# --- Configuración que puedes cambiar ------------------------------------------

# Juegos para el modo "revisar": pon 10 que conozcas bien.
MIS_JUEGOS = [
    "The Legend of Zelda: Ocarina of Time",
    "Resident Evil 4",
    "Final Fantasy VII",
    "Metroid Prime",
    "Super Mario 64",
]

# Qué juegos descargar. IGDB tiene cientos de miles; empezamos por los que
# tienen al menos N valoraciones de usuarios o crítica (unos pocos miles a
# decenas de miles). Bájalo para tener más juegos, súbelo para tener menos.
MIN_VALORACIONES = 10

CARPETA_SALIDA = Path("igdb_export")

# ------------------------------------------------------------------------------

API = "https://api.igdb.com/v4"
TAM_PAGINA = 500        # máximo que permite IGDB por petición
PAUSA = 0.3             # IGDB permite unas 4 peticiones por segundo
TAM_LOTE_IDS = 400      # ids por consulta "where x = (…)" para no pasarnos de largo


def pedir_claves():
    client_id = os.environ.get("IGDB_CLIENT_ID") or input("Client ID de Twitch: ").strip()
    secreto = os.environ.get("IGDB_CLIENT_SECRET") or getpass.getpass("Client secret (no se verá al escribir): ").strip()
    return client_id, secreto


def obtener_token(client_id, secreto):
    params = urllib.parse.urlencode({
        "client_id": client_id,
        "client_secret": secreto,
        "grant_type": "client_credentials",
    })
    req = urllib.request.Request(f"https://id.twitch.tv/oauth2/token?{params}", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)["access_token"]
    except urllib.error.HTTPError as e:
        sys.exit(f"Twitch rechazó las claves (HTTP {e.code}). Revisa el client ID y el secreto.")


class IGDB:
    def __init__(self, client_id, token):
        self.headers = {
            "Client-ID": client_id,
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def consulta(self, endpoint, cuerpo, reintentos=5):
        """Lanza una consulta Apicalypse y devuelve la lista de resultados."""
        req = urllib.request.Request(
            f"{API}/{endpoint}", data=cuerpo.encode("utf-8"), headers=self.headers, method="POST"
        )
        for intento in range(reintentos):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    time.sleep(PAUSA)
                    return json.load(r)
            except urllib.error.HTTPError as e:
                if e.code == 429 and intento < reintentos - 1:   # demasiadas peticiones: esperamos
                    time.sleep(2 ** intento)
                    continue
                detalle = e.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"IGDB {endpoint} devolvió HTTP {e.code}: {detalle}") from None

    def todo(self, endpoint, filtro=None, campos="*"):
        """Recorre un endpoint completo paginando por id (más fiable que offset)."""
        ultimo_id = -1   # algunas tablas (game_types) tienen un registro con id 0
        while True:
            condicion = f"id > {ultimo_id}" + (f" & {filtro}" if filtro else "")
            pagina = self.consulta(
                endpoint, f"fields {campos}; where {condicion}; sort id asc; limit {TAM_PAGINA};"
            )
            if not pagina:
                return
            yield from pagina
            ultimo_id = pagina[-1]["id"]

    def por_ids(self, endpoint, campo, ids, campos="*"):
        """Descarga los registros cuyo `campo` está en la lista de ids, por lotes."""
        ids = sorted(set(ids))
        for i in range(0, len(ids), TAM_LOTE_IDS):
            lote = ",".join(map(str, ids[i:i + TAM_LOTE_IDS]))
            # Un lote puede tener más de 500 resultados (p. ej. fechas de lanzamiento): paginamos dentro del lote
            yield from self.todo(endpoint, filtro=f"{campo} = ({lote})", campos=campos)


def guardar(nombre, registros, sello):
    """Escribe JSON Lines (un objeto por línea): el formato más cómodo para Auto Loader."""
    carpeta = CARPETA_SALIDA / nombre
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}_{sello}.jsonl"
    n = 0
    try:
        with open(ruta, "w", encoding="utf-8") as f:
            for r in registros:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                n += 1
    except Exception:
        ruta.unlink(missing_ok=True)   # no dejamos archivos a medias
        raise
    if n == 0:
        ruta.unlink()
        print(f"  {nombre:<22} sin registros")
        return 0
    print(f"  {nombre:<22} {n:>8} registros  ->  {ruta}")
    return n


def ids_de(registros, campo):
    salida = set()
    for r in registros:
        valor = r.get(campo)
        if isinstance(valor, list):
            salida.update(valor)
        elif isinstance(valor, int):
            salida.add(valor)
    return salida


def modo_revisar(igdb):
    for titulo in MIS_JUEGOS:
        titulo_seguro = titulo.replace('"', '\\"')
        resultados = igdb.consulta("games", f'''
            search "{titulo_seguro}";
            fields name, first_release_date, game_type.type, platforms.name,
                   remakes.name, remasters.name, ports.name, version_parent.name, version_title;
            limit 3;
        ''')
        print(f"\n=== {titulo} ===")
        for g in resultados:
            nombres = lambda clave: ", ".join(x["name"] for x in g.get(clave, [])) or "-"
            fecha = g.get("first_release_date")
            anio = dt.datetime.fromtimestamp(fecha, dt.timezone.utc).year if fecha else "?"
            print(f"- {g.get('name')} ({anio}) · tipo: {(g.get('game_type') or {}).get('type', '?')}")
            if g.get("version_parent"):
                print(f"    versión de: {g['version_parent']['name']} · {g.get('version_title') or ''}")
            print(f"    plataformas: {nombres('platforms')}")
            print(f"    remakes: {nombres('remakes')}")
            print(f"    remasters: {nombres('remasters')}")
            print(f"    ports: {nombres('ports')}")


def modo_descargar(igdb):
    sello = dt.datetime.now().strftime("%Y%m%d_%H%M")
    print(f"Descargando en {CARPETA_SALIDA.resolve()}\n")

    # 1. Tablas pequeñas de referencia (completas)
    for endpoint in ["platforms", "game_types", "release_date_regions", "genres", "themes", "game_modes"]:
        try:
            guardar(endpoint, list(igdb.todo(endpoint)), sello)
        except RuntimeError as e:
            print(f"  {endpoint:<22} omitido: {e}")

    # 2. Juegos con un mínimo de valoraciones
    filtro = f"(total_rating_count >= {MIN_VALORACIONES} | rating_count >= {MIN_VALORACIONES})"
    juegos = list(igdb.todo("games", filtro=filtro))

    # Añadimos las versiones relacionadas aunque tengan pocas valoraciones,
    # para que el árbol de versiones quede completo
    relacionados = ids_de(juegos, "remakes") | ids_de(juegos, "remasters") | ids_de(juegos, "ports") \
        | ids_de(juegos, "version_parent") | ids_de(juegos, "parent_game") | ids_de(juegos, "expanded_games")
    faltan = relacionados - {g["id"] for g in juegos}
    if faltan:
        juegos += list(igdb.por_ids("games", "id", faltan))
    guardar("games", juegos, sello)
    ids_juegos = [g["id"] for g in juegos]

    # 3. Datos que cuelgan de cada juego
    for endpoint, campo in [("release_dates", "game"), ("game_time_to_beats", "game_id")]:
        try:
            guardar(endpoint, igdb.por_ids(endpoint, campo, ids_juegos), sello)
        except RuntimeError as e:
            print(f"  {endpoint:<22} omitido: {e}")

    # 4. Franquicias, colecciones y compañías que aparecen en esos juegos
    for endpoint, campo in [("franchises", "franchises"), ("collections", "collections")]:
        ids = ids_de(juegos, campo) | (ids_de(juegos, "franchise") if endpoint == "franchises" else set())
        try:
            guardar(endpoint, igdb.por_ids(endpoint, "id", ids, campos="id, name, slug, updated_at"), sello)
        except RuntimeError as e:
            print(f"  {endpoint:<22} omitido: {e}")

    try:
        ic = list(igdb.por_ids("involved_companies", "id", ids_de(juegos, "involved_companies")))
        guardar("involved_companies", ic, sello)
        guardar("companies", igdb.por_ids("companies", "id", ids_de(ic, "company"),
                                          campos="id, name, country, updated_at"), sello)
    except RuntimeError as e:
        print(f"  companies              omitido: {e}")

    print("\nListo. Sube la carpeta igdb_export al Volume (instrucciones en el hilo).")


def main():
    modo = sys.argv[1] if len(sys.argv) > 1 else ""
    if modo not in ("revisar", "descargar"):
        print(__doc__)
        sys.exit(1)
    client_id, secreto = pedir_claves()
    igdb = IGDB(client_id, obtener_token(client_id, secreto))
    modo_revisar(igdb) if modo == "revisar" else modo_descargar(igdb)


if __name__ == "__main__":
    main()
