import copy

import arrow

import pytest
from mozetl.sync import bookmark_validation as sbv
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


@pytest.fixture()
def sync_summary_schema():
    """"Generate a schema for sync_summary. This subset contains enough
    structure for testing bookmark validation. The schema is derived from
    [`telemetry-batch-view`][1].

    [1]: https://git.io/vdQ5A
    """
    failure_type = StructType([StructField("name", StringType(), False)])

    status_type = StructType([StructField("sync", StringType(), True)])

    validation_problems = StructType(
        [
            StructField("name", StringType(), False),
            StructField("count", LongType(), False),
        ]
    )

    validation_type = StructType(
        [
            StructField("version", LongType(), False),
            StructField("checked", LongType(), False),
            StructField("took", LongType(), False),
            StructField("problems", ArrayType(validation_problems, False), True),
        ]
    )

    engine_type = StructType(
        [
            StructField("name", StringType(), False),
            StructField("status", StringType(), False),
            StructField("failure_reason", failure_type, True),
            StructField("validation", validation_type, True),
        ]
    )

    return StructType(
        [
            StructField("app_build_id", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("app_display_version", StringType(), True),
            StructField("app_name", StringType(), True),
            StructField("app_channel", StringType(), True),
            StructField("uid", StringType(), False),
            StructField("device_id", StringType(), True),
            StructField("when", LongType(), False),
            StructField("failure_reason", failure_type, True),
            StructField("status", status_type, False),
            StructField("engines", ArrayType(engine_type, False), True),
            StructField("submission_date_s3", StringType(), False),
        ]
    )


def to_when(adate):
    return adate.timestamp * 1000


def to_submission_date(adate):
    return adate.format("YYYYMMDD")


SYNC_ACTIVITY_DATE = arrow.get("2017-10-01")

sync_summary_sample = {
    "app_build_id": "id",
    "app_version": "version",
    "app_display_version": "display_version",
    "app_name": "app_name",
    "app_channel": "channel",
    "uid": "uid",
    "device_id": "device_id",
    "when": to_when(SYNC_ACTIVITY_DATE),
    "status": {"sync": "sync"},
    "failure_type": None,
    "engines": [
        {
            "name": "bookmarks",
            "status": "status",
            "failure_reason": {"name": "name"},
            "validation": {
                "version": 1,
                "checked": 1,
                "took": 1,
                "problems": [{"name": "name", "count": 1}],
            },
        }
    ],
    "submission_date_s3": to_submission_date(SYNC_ACTIVITY_DATE),
}


def build_sync_summary_snippet(snippet, sample=sync_summary_sample):
    """Build a sync_summary row by providing small dictionary snippets to overwrite.

    :snippet dict:  A dictionary representing the root level in the schema
    :returns: A single dictionary that is used to overwrite the sample
    """

    result = {}
    result.update({k: v for k, v in list(snippet.items()) if k != "engines"})

    if "engines" not in snippet or snippet["engines"] is None:
        return result

    # NOTE: The entire engine sub-tree must be completely materialized before
    # passing it to the dataframe factory. Replacing nested values in a
    # dictionary has edge-cases that make a generalized builder more difficult
    # to build.

    engines = []
    base_engine = sample["engines"][0]

    # basic type-checking on the engine that's been passed in
    assert isinstance(snippet["engines"], list)

    for engine_snippet in snippet["engines"]:
        engine = copy.deepcopy(base_engine)

        engine.update(
            {
                k: v
                for k, v in list(engine_snippet.items())
                if k not in ["failure_reason", "validation"]
            }
        )

        failure_reason = engine_snippet.get("failure_reason", {})
        if failure_reason is None:
            engine["failure_reason"] = None
        else:
            engine["failure_reason"].update(failure_reason)

        validation = engine_snippet.get("validation", {})
        if validation is None:
            engine["validation"] = None
        else:
            engine["validation"].update(validation)

        engines.append(engine)

    result.update({"engines": engines})
    return result


@pytest.fixture()
def generate_data(dataframe_factory, sync_summary_schema):
    def _generate_data(snippets):
        return dataframe_factory.create_dataframe(
            list(map(build_sync_summary_snippet, snippets)),
            sync_summary_sample,
            sync_summary_schema,
        )

    return _generate_data


@pytest.fixture()
def test_transform(spark, generate_data, monkeypatch):
    def _test_transform(snippets):
        def mock_parquet(cls, path):
            return generate_data(snippets)

        monkeypatch.setattr("pyspark.sql.DataFrameReader.parquet", mock_parquet)
        sbv.extract(spark, None, to_submission_date(SYNC_ACTIVITY_DATE))
        sbv.transform(spark)
        return spark.table("bmk_validation_problems"), spark.table("bmk_total_per_day")

    return _test_transform


def test_failures_filtered(test_transform):
    df, _ = test_transform(
        [
            {"failure_reason": None},
            {"failure_reason": {"name": "failure!"}},
            {"failure_reason": {"name": "failure2!"}},
        ]
    )

    assert df.count() == 1


def test_validation_problems(test_transform):
    df, _ = test_transform(
        [
            # failed, also not a bookmark
            {
                "failure_reason": {"name": "some failure"},
                "engines": [
                    {"name": "not bookmarks", "validation": {"problems": None}}
                ],
            },
            # not a bookmark
            {
                "failure_reason": None,
                "engines": [
                    {"name": "not bookmarks", "validation": {"problems": None}}
                ],
            },
            # does not contain a problem
            {
                "failure_reason": None,
                "engines": [{"name": "bookmarks", "validation": {"problems": None}}],
            },
            # single engine, single problem, no longer explicitly setting values
            {"engines": [{"validation": {"problems": [{"name": "1", "count": 1}]}}]},
            # two engines, with one and two problems each
            {
                "engines": [
                    # no problems
                    {"name": "not bookmarks"},
                    # a single problem
                    {"validation": {"problems": [{"name": "2", "count": 10}]}},
                    {
                        "validation": {
                            "problems": [
                                {"name": "3", "count": 100},
                                {"name": "4", "count": 1000},
                            ]
                        }
                    },
                ]
            },
            # new bookmarks engine with problems
            {
                "failure_reason": None,
                "engines": [
                    {
                        "name": "bookmarks-buffered",
                        "validation": {
                            "problems": [
                                {"name": "new problem", "count": 50},
                                {"name": "another problem", "count": 4},
                            ]
                        },
                    }
                ],
            },
            # new bookmarks engine without problems
            {
                "failure_reason": None,
                "engines": [
                    {"name": "bookmarks-buffered", "validation": {"problems": None}}
                ],
            },
        ]
    )

    assert df.count() == 6
    assert (
        df.select(F.sum("engine_validation_problem_count").alias("pcount"))
        .first()
        .pcount
        == 1165
    )

    assert (
        df.where(F.col("engine_name") == "bookmarks")
        .select(F.sum("engine_validation_problem_count").alias("pcount"))
        .first()
        .pcount
        == 1111
    )
    assert (
        df.where(F.col("engine_name") == "bookmarks-buffered")
        .select(F.sum("engine_validation_problem_count").alias("pcount"))
        .first()
        .pcount
        == 54
    )


def test_total_bookmarks_checked(test_transform):
    _, df = test_transform(
        [
            {
                "engines": [
                    {"validation": {"checked": 1}},
                    {"validation": {"checked": 10}},
                ]
            },
            {"engines": [{"validation": {"checked": 100}}, {"validation": None}]},
        ]
    )

    assert df.count() == 1
    assert df.select("total_bookmarks_checked").first()[0] == 111


def test_total_bookmark_validations(test_transform):
    _, df = test_transform(
        [
            {"uid": "0", "device_id": "0"},
            {"uid": "0", "device_id": "1"},
            {"uid": "1", "device_id": "0"},
            {"uid": "1", "device_id": "1"},
            # duplicates
            {"uid": "1", "device_id": "1"},
            {"uid": "1", "device_id": "1"},
        ]
    )

    assert df.count() == 1
    assert df.first().total_bookmark_validations == 4


def test_total_users_per_day(test_transform, row_to_dict):
    day_1 = SYNC_ACTIVITY_DATE
    day_2 = SYNC_ACTIVITY_DATE.replace(days=-1)

    _, df = test_transform(
        [
            {
                "uid": "date not included",
                "submission_date_s3": to_submission_date(day_2),
            },
            {"uid": "0", "when": to_when(day_1)},
            {"uid": "1", "when": to_when(day_1)},
            {"uid": "1", "when": to_when(day_2)},
            {"uid": "1", "when": to_when(day_2)},
            {"uid": "2", "when": to_when(day_2)},
        ]
    )

    assert df.count() == 1
    rows = df.select("submission_day", "total_validated_users").collect()
    assert list(map(row_to_dict, rows)) == [
        {"submission_day": to_submission_date(day_1), "total_validated_users": 3}
    ]
