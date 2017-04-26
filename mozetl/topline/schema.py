import json
import os
from pyspark.sql.types import StructType


def schema_from_json(path, relative=True):
    """ Create a pyspark schema from the json representation.

    The json representation must be from a StructType. This can be
    generated from any StructType using the `.json()` method. The
    schema for a dataframe can be obtained using the `.schema`
    accessor. For example, to generate the json from the
    `topline_summary`, run the following in the pyspark repl:

    >>> path = 's3a://telemetry-parquet/topline_summary/v1/mode=weekly'
    >>> json_data = spark.read.parquet(path).schema.json()

    :path str: Path the the json data
    :relative bool: Use the relative path to the current file.
    """

    if relative:
        path = os.path.join(os.path.dirname(__file__), path)

    with open(path) as json_data:
        data = json.load(json_data)
    return StructType.fromJson(data)


# Generate module level schemas
historical_schema = schema_from_json('historical_schema.json')
topline_schema = schema_from_json('topline_schema.json')
