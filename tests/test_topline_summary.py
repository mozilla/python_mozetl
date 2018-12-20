import functools
import os

import arrow
import pytest
from click.testing import CliRunner
from pyspark.sql import functions as F, Row
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    LongType,
)

from mozetl.topline import topline_summary as topline


def generate_dates(submission_date_s3, ts_offset=0, creation_offset=0):
    # convert submission_date into profile_creation_date and timestamps
    submission = arrow.get(submission_date_s3, "YYYYMMDD")
    creation = submission.replace(days=+creation_offset)
    timestamp = submission.replace(days=+ts_offset)

    # variables for conversion
    epoch = arrow.get(0)
    seconds_per_day = topline.seconds_per_day
    nanoseconds_per_second = 10 ** 9

    date_snippet = {
        "submission_date_s3": submission_date_s3,
        "profile_creation_date": (
            int((creation - epoch).total_seconds() / seconds_per_day)
        ),
        "timestamp": (
            int((timestamp - epoch).total_seconds() * nanoseconds_per_second)
        ),
    }

    return date_snippet


def search_row(engine="hooli", count=1, source="searchbar"):
    return Row(engine=str(engine), source=str(source), count=count)


schema = StructType(
    [
        StructField("document_id", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("is_default_browser", BooleanType(), True),
        StructField(
            "search_counts",
            ArrayType(
                StructType(
                    [
                        StructField("engine", StringType(), True),
                        StructField("source", StringType(), True),
                        StructField("count", LongType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("country", StringType(), True),
        StructField("profile_creation_date", LongType(), True),
        StructField("normalized_channel", StringType(), True),
        StructField("os", StringType(), True),
        StructField("subsession_length", LongType(), True),
        StructField("submission_date_s3", StringType(), True),
    ]
)


start_ds = "20170601"
end_ds = "20170608"

default_sample = {
    "document_id": "document-id",
    "client_id": "client-id",
    "timestamp": int(1.4962752e18),
    "is_default_browser": True,
    "search_counts": [search_row()],
    "country": "US",
    "profile_creation_date": 17318,
    "normalized_channel": "release",
    "os": "Linux",
    "subsession_length": 3600,
    "submission_date_s3": "20170601",
}
default_sample.update(generate_dates(start_ds))


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe_with_key,
        base=default_sample,
        key="document_id",
        schema=schema,
    )


@pytest.fixture()
def cool_df(dataframe_factory):
    rows = [
        # exact
        "ice",
        "lice",
        # prefixed
        "glacier water",
        "melting glaciers",
        # suffixed
        "gentoo penguin",
        "penguin suit",
        # anywhere
        "dinosaurs are always cool",
        "chickens are dinosaurs, somewhat",
    ]
    snippets = [{"items": row} for row in rows]
    base = {"items": "foo"}
    return dataframe_factory.create_dataframe(snippets, base)


def test_column_like(cool_df):
    pattern = {"cool": ["ice", "glacier%", "%penguin", "%dinosaurs%"]}
    res = cool_df.select(
        topline.column_like("items", pattern, "not cool").alias("items")
    )

    # lice, melting glaciers, and penguin suits are not cool
    assert res.where("items='not cool'").count() == 3


@pytest.mark.skip(reason="Bug 1377730 - Weekly Topline Summary aborts on save")
def test_deduplicate_documents(dataframe_factory):
    snippets = [{"document_id": "1"}, {"document_id": "2"}, {"document_id": "2"}]
    df = dataframe_factory.create_dataframe(snippets, default_sample, schema=schema)

    res = topline.clean_input(df, start_ds, end_ds)
    assert res.count() == 2


def test_clean_input_date_range(generate_data):
    snippets = [
        generate_dates("20170601"),
        generate_dates("20170602"),
        generate_dates("20170501"),
        generate_dates("20170608"),
    ]

    df = generate_data(snippets)
    res = topline.clean_input(df, start_ds, end_ds)
    assert res.count() == 2


def test_clean_input_country(generate_data):
    snippets = [{"country": "INVALID"}, {"country": "US"}]

    df = generate_data(snippets)
    res = topline.clean_input(df, start_ds, end_ds)

    assert res.where(F.col("country") == "Other").count() == 1


def test_clean_input_profile_creation(generate_data):
    snippets = [{"profile_creation_date": 1}, {"profile_creation_date": -1}]

    df = generate_data(snippets)
    res = topline.clean_input(df, start_ds, end_ds)

    total = res.select("profile_creation_date").groupBy().sum().first()[0]
    assert total == topline.seconds_per_day


def test_clean_input_os(generate_data):
    oses = [
        "Windows_NT",
        "WINNT",
        "Darwin",
        "Linux",  # 4
        "xWindows",
        "Mac",
        "SUN",
        "BaDsTrInG",  # 4
    ]
    snippets = [{"os": os} for os in oses]
    df = generate_data(snippets)
    res = topline.clean_input(df, start_ds, end_ds)

    assert res.where(F.col("os") == "Other").count() == 4
    assert res.select("os").distinct().count() == 4


def test_clean_input_hours(generate_data):
    snippets = [
        {"subsession_length": topline.seconds_per_hour},
        {"subsession_length": 181 * topline.seconds_per_day},
        {"subsession_length": -1 * topline.seconds_per_day},
    ]

    df = generate_data(snippets)
    res = topline.clean_input(df, start_ds, end_ds)

    total = res.select("hours").groupBy().sum().first()[0]
    assert total == 1.0


def test_transform_searches(generate_data):
    snippets = [
        {"search_counts": None},
        {"search_counts": [search_row("google")]},
        {"country": "CA", "search_counts": [search_row("hooli"), search_row("google")]},
    ]

    df = generate_data(snippets)
    res = topline.transform(df, start_ds, "weekly")

    assert res.count() == 2
    assert res.groupBy().sum().first()["sum(google)"] == 2
    assert (
        res.groupBy("geo").sum().where(F.col("geo") == "CA").first()["sum(other)"]
    ) == 1


def test_transform_searches_filters_incontent(generate_data):
    snippets = [
        {"search_counts": [search_row("google", source="in-content")]},  # no
        {"search_counts": [search_row("google", source="contextmenu")]},  # yes
        {"search_counts": [search_row("google", source="abouthome")]},  # yes
    ]

    df = generate_data(snippets)
    res = topline.transform(df, start_ds, "weekly")

    assert res.groupBy().sum().first()["sum(google)"] == 2


def test_transform_hours(generate_data):
    snippets = [
        {"country": "US", "subsession_length": topline.seconds_per_hour},
        {"country": "CA", "subsession_length": topline.seconds_per_hour},
        {"subsession_length": 181 * topline.seconds_per_day},
        {"subsession_length": -1 * topline.seconds_per_day},
    ]

    df = generate_data(snippets)
    res = topline.transform(df, start_ds, "weekly")

    assert res.count() == 2
    assert res.groupBy().sum().first()["sum(hours)"] == 2.0
    assert (
        res.groupBy("geo").sum().where(F.col("geo") == "CA").first()["sum(hours)"]
    ) == 1.0


def test_transform_clients(generate_data):
    submission_dates = generate_dates(start_ds)
    snippets = [
        # not new, default
        {"client_id": "0", "profile_creation_date": 0},
        # new, but duplicate
        {"client_id": "1"},
        {"client_id": "1", "timestamp": submission_dates["timestamp"] + 1},
        # new, not default
        {"client_id": "2", "is_default_browser": False},
    ]

    df = generate_data(snippets)
    res = topline.transform(df, start_ds, "weekly")

    assert res.count() == 1
    row = res.first()

    assert row.actives == 3
    assert row.new_records == 2
    assert row.default == 2


def test_job_weekly(spark, generate_data, monkeypatch, tmpdir):
    test_bucket = str(tmpdir)
    test_prefix = "prefix"

    snippets = [{"client_id": "1"}, {"client_id": "2"}, {"client_id": "3"}]

    def mock_extract(spark_session, source_path):
        return generate_data(snippets)

    monkeypatch.setattr(topline, "extract", mock_extract)

    def mock_format_spark_path(bucket, prefix):
        return "file://{}/{}".format(bucket, prefix)

    monkeypatch.setattr(topline, "format_spark_path", mock_format_spark_path)

    runner = CliRunner()
    args = [start_ds, "weekly", test_bucket, test_prefix]

    result = runner.invoke(topline.main, args)
    assert result.exit_code == 0

    # assert path was written
    assert os.path.isdir(
        "{}/{}/v1/mode=weekly/report_start={}".format(
            test_bucket, test_prefix, start_ds
        )
    )

    # assert data can be read
    path = mock_format_spark_path(test_bucket, test_prefix)
    df = spark.read.parquet(path)
    assert df.count() == 1

    row = df.first()
    assert row.actives == 3
