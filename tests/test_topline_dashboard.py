import functools

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_s3

from mozetl.topline import topline_dashboard as topline
from mozetl.topline.schema import historical_schema, topline_schema

default_sample = {
    "geo": "US",
    "channel": "nightly",
    "os": "Windows",
    "hours": 1.0,
    "crashes": 1,
    "google": 1,
    "bing": 1,
    "yahoo": 1,
    "other": 1,
    "actives": 1,
    "new_records": 1,
    "default": 1,
    "report_start": "20160101",
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe, base=default_sample, schema=topline_schema
    )


@pytest.fixture()
def simple_df(generate_data):
    return generate_data(None)


@pytest.fixture()
def multi_df(generate_data):
    snippets = [{"geo": "CA"}, {"channel": "release"}, {"os": "Linux"}]
    return generate_data(snippets)


# reformatted data filters out ROW into Other
def test_reformat_filters_ROW(generate_data):
    # Maldives is not a target region
    input_df = generate_data([{"geo": "MV"}])
    df = topline.reformat_data(input_df)

    assert df.where("geo='MV'").count() == 0
    assert df.where("geo='Other'").count() > 0


def test_reformat_generates_rows_with_all(simple_df):
    """ The output of the dataset should contain 2^3 values. The
    cardinality of each dimension is 2 because of the additional `all`
    label."""
    df = topline.reformat_data(simple_df)

    assert df.count() == 8


def test_reformat_prunes_empty_rows_with_all(multi_df):
    """ This test should generate 16 results where any of the rows
    contains `all` in any of the attribute fields. The cardinality of
    the cross product is 27. Don't include any rows that do not
    contain 'all'. Dont include rows that contain values of 0. This
    removes 2^3 results imediately, leaving 19 rows. We get rid of the
    extra three from the tuples containing only a single `all`.

    ('CA', 'release', 'all'),
    ('CA', 'all', 'Linux'),
    ('all', 'release','Linux')

    should not exist and contain empty rows. This leaves 16 results."""
    df = topline.reformat_data(multi_df)

    # This row should be pruned
    assert df.where("geo='CA' AND channel='release'").count() == 0

    # This should be the accurate count at the end
    assert df.where("geo='all' OR channel='all' OR os='all'").count() == 16


# reformatted data correctly aggregates all values
def test_reformat_aggregates(multi_df):
    df = topline.reformat_data(multi_df)

    rows = df.where("geo='all' AND channel='all' AND os='all'").head()
    assert rows.hours == 3.0


def test_reformat_conforms_to_historical_schema(simple_df):
    df = topline.reformat_data(simple_df)

    assert df.columns == historical_schema.names


@mock_s3
def test_cli_monthly(simple_df, tmpdir, monkeypatch):
    # set up moto with a fake bucket
    bucket = "test-bucket"
    prefix = "test-prefix"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    # change s3_path to use file:// protocol
    def mock_format_spark_path(bucket, prefix):
        return "file://{}/{}".format(bucket, prefix)

    monkeypatch.setattr(topline, "format_spark_path", mock_format_spark_path)

    # write test data to local path
    input_bucket = str(tmpdir)
    test_path = topline.format_spark_path(
        input_bucket, "topline_summary/v1/mode=monthly"
    )
    simple_df.write.partitionBy("report_start").parquet(test_path)

    # Run the application via the cli
    runner = CliRunner()
    args = ["monthly", bucket, prefix, "--input_bucket", input_bucket]
    result = runner.invoke(topline.main, args)
    assert result.exit_code == 0

    # read results using boto
    body = (
        conn.Object(bucket, prefix + "/topline-monthly.csv")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )

    # header + 8x rows = 9
    assert len(body.rstrip().split("\n")) == 9
