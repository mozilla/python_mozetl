import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession
            .builder
            .appName("python_mozetl_test")
            .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def spark_context(spark):
    return spark.sparkContext


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """ Disable stopping the shared spark session during tests """
    def nop(*args, **kwargs):
        print("Disabled spark.stop for testing")
    monkeypatch.setattr("pyspark.sql.SparkSession.stop", nop)


@pytest.fixture(scope="session")
def row_to_dict():
    """Convert pyspark.Row to dict for easier unordered comparison"""
    def func(row):
        return {key: row[key] for key in row.__fields__}
    return func


class DataFrameFactory:
    """Create a dataframe given a base dictionary and schema."""

    def __init__(self, spark_session):
        self.spark = spark_session

    def create_dataframe(self, snippets, base, schema=None):
        """Generate a dataframe in the shape of the base dictionary where every
        row has column values overwritten by the snippets.

        :snippets list[dict]: a list of fields to overwrite in the base dictionary
        :base dict: a base instantiation of a row in the dataset
        :schema pyspark.sql.types.StructType: schema for the dataset
        """
        # the dataframe should have at least one item
        if not snippets:
            snippets = [dict()]

        samples = []
        for snippet in snippets:
            sample = base.copy()
            sample.update(snippet)
            samples.append(sample)

        # if no schema is provided, the schema will be inferred
        return self.spark.createDataFrame(samples, schema)


@pytest.fixture()
def dataframe_factory(spark):
    return DataFrameFactory(spark)
