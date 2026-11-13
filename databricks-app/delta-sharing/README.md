# Delta Sharing

Comparte el catálogo de versiones (gold) con un socio externo, sin copiar datos y solo en lectura.

| Archivo | Quién lo corre | Qué hace |
| --- | --- | --- |
| `compartir_gold.py` | nosotros, una vez por ambiente | crea el share `gamevault_gold` con `game_catalog`, `game_versions` y `game_releases`, el destinatario `socio_gamevault` (open sharing) y le da `SELECT` |
| `leer_como_socio.py` | el socio | lee las tablas con la librería abierta `delta-sharing` y su archivo `config.share`, y las pasa a PySpark |

`my_library` no se comparte porque es la biblioteca personal.

## Pasos

1. Una vez: en la consola de cuenta (accounts.azuredatabricks.net) > Catalog > `metastore-gamevault`, activar
   *Delta Sharing to external parties*.
2. Correr `compartir_gold.py` con el catálogo del ambiente (`gamevault_dev` o `gamevault_prod`).
3. Copiar el enlace de activación desde Catalog > Delta Sharing > Shared by me > Recipients > `socio_gamevault`
   y enviárselo al socio. Con él descarga `config.share`, que es una credencial y nunca va al repo.
4. El socio corre `leer_como_socio.py`.
