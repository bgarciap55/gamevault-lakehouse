# Dashboard

Tablero de Power BI sobre la base de servicio en PostgreSQL (`gamevault_serving`), que la tarea `serving` del Job carga desde gold.

| Archivo | Qué es |
| --- | --- |
| `gamevault.pbix` | Tablero de Power BI Desktop (modo importación) |
| `dashboard_resumen.png` | Página Resumen: juegos, versiones, lanzamientos, versiones por tipo, lanzamientos por año y top 10 plataformas |
| `dashboard_explorador.png` | Página Explorador: versiones de un juego y si se pueden jugar en mis plataformas (ej. Ocarina of Time) |
| gamevault.pdf | Exportación del tablero para verlo sin Power BI |

## Conexión

Obtener datos > PostgreSQL: servidor `pg-gamevault-bg64.postgres.database.azure.com`, base `gamevault_serving`,
tablas `public.game_catalog`, `public.game_versions`, `public.game_releases` y `public.my_library`.
Las credenciales no se guardan en el archivo; el firewall del servidor debe permitir la IP de quien lo abre.

No se publicó en el servicio de Power BI: la cuenta personal no tiene licencia de Power BI Service.
