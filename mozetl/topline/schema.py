import json
from pyspark.sql.types import StructType

import pkg_resources
import mozetl.topline


def schema_from_json(path):
    """ Create a pyspark schema from the json representation.

    The json representation must be from a StructType. This can be
    generated from any StructType using the `.json()` method. The
    schema for a dataframe can be obtained using the `.schema`
    accessor. For example, to generate the json from the
    `topline_summary`, run the following in the pyspark repl:

    >>> path = 's3a://telemetry-parquet/topline_summary/v1/mode=weekly'
    >>> json_data = spark.read.parquet(path).schema.json()

    :path str: Path the the json data
    """
    with pkg_resources.resource_stream(mozetl.topline.__name__, path) as f:
        data = json.load(f)
        return StructType.fromJson(data)


# Generate module level schemas
historical_schema = schema_from_json("historical_schema.json")
topline_schema = schema_from_json("topline_schema.json")
