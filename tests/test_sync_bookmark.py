import pytest
from pyspark.sql.types import (
    StructType, LongType, StructField, StringType, ArrayType
)
from mozetl.sync import bookmark_validation as sbv

# Create a schema that works for this particular derived dataset

failure_type = StructType([
    StructField("name", StringType(), False),
])

status_type = StructType([
    StructField("sync", StringType(), True),
])

validation_type = StructType([
    StructField("version", LongType(), False),
    StructField("checked", LongType(), False),
    StructField("took", LongType(), False),
    StructField("problems", LongType(), False),
])

engine_type = StructType([
    StructField("name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("failure_reason", failure_type, True),
    StructField("validation", validation_type, True),
])

sync_summary_schema = StructType([
    StructField("app_build_id", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("app_display_version", StringType(), True),
    StructField("app_name", StringType(), True),
    StructField("app_channel", StringType(), True),
    StructField("uid", StringType(), False),
    StructField("device_id", StringType(), True),
    StructField("when", LongType(), False),
    StructField("status", status_type, False),
    StructField("engines", ArrayType(engine_type, False), False),
    StructField("submission_date_s3", StringType(), False),
])


sync_summary_sample = {
    "app_build_id": "id",
    "app_version": "version",
    "app_display_version": "display_version",
    "app_name": "app_name",
    "app_channel": "channel",
    "uid": "uid",
    "device_id": "device_id",
    "when": 1222,
    "status": {
        "sync": "sync",
    },
    "engines": [
        {
            "name": "name",
            "status": "status",
            "failure_reason": {"name": "name"},
            "validation": {
                "version": 1,
                "checked": 1,
                "took": 1,
                "problems": 1,
            }
        }
    ]
}


def build_sync_summary_snippet(snippet, sample=sync_summary_sample):
    """Build a sync_summary row by providing small dictionary snippets to overwrite.

    :snippet dict:  A dictionary representing the root level in the schema
    :returns: A single dictionary that is used to overwrite the sample
    """

    snippet = {}
    snippet.update({k: v for k, v in snippet.iteritems() if k != 'engines'})

    if 'engines' not in snippet or snippet['engines'] is None:
        return snippet

    # NOTE: The entire engine sub-tree must be completely materialized before
    # passing it to the dataframe factory. Replacing nested values in a
    # dictionary has edge-cases that make a generalized builder more difficult
    # to build.

    engines = []
    base_engine = sample["engines"][0]

    # basic type-checking on the engine that's been passed in
    assert isinstance(snippet["engine"], list)

    for engine_snippet in snippet["engines"]:
        engine = base_engine.copy()

        engine.update({
            k: v for k, v in engine_snippet.iteritems()
            if k not in ["failure_reason", "validation"]
        })
        engine["failure_reason"].update(engine_snippet.get("failure_reason", {}))
        engine["validation"].update(engine_snippet.get("validation", {}))

        engines.append(engine)

    snippet.update({"engines": engines})
    return snippet


@pytest.fixture()
def generate_data(dataframe_factory):
    def _generate_data(snippets):
        return (
            dataframe_factory.create_dataframe(
                map(build_sync_summary_snippet, snippets),
                sync_summary_sample,
                sync_summary_schema
            )
        )
    return _generate_data


@pytest.fixture()
def test_transform(spark, generate_data, monkeypatch):
    def _test_transform(snippets):
        def mock_parquet(path):
            return generate_data(snippets)
        monkeypatch.setattr("pyspark.sql.SparkSession.read.parquet", mock_parquet)

        # TODO: Add proper dates to sample data
        sbv.extract(spark, None, "20171001")
        sbv.transform(spark)
        return spark.table("bmk_validation_problems"), spark.table("bmk_total_per_day")
    return _test_transform


# Test cases

# Check that non-null failure reasons are filtered
# Check that engines with problems are filtered in bmk_validation_problems
# Check that bookmarks are filtered
# Null validation checked
# multiple sync_days
# total_bookmark_validations
# total_validated_users
# sum of engines checked
