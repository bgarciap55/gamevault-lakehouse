"""Configuración de una corrida del ETL.

Los notebooks la arman con los parámetros del Job, que a su vez vienen de las
variables del bundle (databricks.yml). Así el mismo código corre en dev y prod,
y en la etapa 1 (Volumes) o en la etapa 2 (ADLS + PostgreSQL).
"""

from dataclasses import dataclass, field

MODOS_FUENTE = ("volume", "externo")
FORMATOS_BRONZE = ("parquet", "avro")


@dataclass(frozen=True)
class Config:
    catalogo: str
    modo_fuente: str = "volume"
    # Carpeta con los datos crudos: igdb/<endpoint>/*.jsonl y, en modo volume, postgres/<tabla>.csv
    ruta_raw: str = ""
    # Carpeta donde bronze deja sus archivos parquet o avro
    ruta_bronze: str = ""
    formato_bronze: str = "parquet"
    # Solo modo externo: conexión JDBC a PostgreSQL
    pg_url: str = ""
    pg_esquema: str = "public"
    pg_usuario: str = ""
    pg_clave: str = field(default="", repr=False)

    def __post_init__(self):
        if not self.catalogo:
            raise ValueError("Falta el catálogo")
        if self.modo_fuente not in MODOS_FUENTE:
            raise ValueError(f"modo_fuente debe ser uno de {MODOS_FUENTE}, no {self.modo_fuente!r}")
        if self.formato_bronze not in FORMATOS_BRONZE:
            raise ValueError(f"formato_bronze debe ser uno de {FORMATOS_BRONZE}, no {self.formato_bronze!r}")
        if self.modo_fuente == "externo":
            if not self.ruta_raw or not self.ruta_bronze:
                raise ValueError("En modo externo hay que indicar ruta_raw y ruta_bronze (abfss://...)")
            if not self.pg_url:
                raise ValueError("En modo externo hay que indicar pg_url")

        # En modo volume las rutas tienen un valor por defecto dentro del catálogo
        volumes = f"/Volumes/{self.catalogo}/bronze"
        if not self.ruta_raw:
            object.__setattr__(self, "ruta_raw", f"{volumes}/raw")
        if not self.ruta_bronze:
            object.__setattr__(self, "ruta_bronze", f"{volumes}/archivos")
