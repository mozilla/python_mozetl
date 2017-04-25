import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_context(request):
    """Initialize a spark context"""
    spark = (SparkSession
             .builder
             .appName("python_mozetl_test")
             .getOrCreate())

    sc = spark.sparkContext

    # teardown
    request.addfinalizer(lambda: spark.stop())

    return sc


@pytest.fixture(scope="session")
def row_to_dict():
    """Convert pyspark.Row to dict for easier unordered comparison"""
    def func(row):
        return {key: row[key] for key in row.__fields__}
    return func
