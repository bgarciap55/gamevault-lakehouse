import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    sesion = (
        SparkSession.builder.master("local[1]")
        .appName("gamevault-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sesion
    sesion.stop()
