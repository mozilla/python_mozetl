import functools
import os

import pytest
from click.testing import CliRunner

from mozetl.topline import historical_backfill as backfill
from mozetl.topline.schema import historical_schema


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """ Disable stopping the shared spark session during tests """

    def nop(*args, **kwargs):
        print("Disabled spark.stop for testing")

    monkeypatch.setattr("pyspark.sql.SparkSession.stop", nop)


default_sample = {
    "geo": "US",
    "channel": "nightly",
    "os": "Windows",
    "date": "2016-01-01",
    "actives": "1",
    "hours": "1",
    "inactives": "1",
    "new_records": "1",
    "five_of_seven": "1",
    "total_records": "1",
    "crashes": "1",
    "default": "1",
    "google": "1",
    "bing": "1",
    "yahoo": "1",
    "other": "1",
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=historical_schema,
    )


# does not include rows containing `all` from original data
def test_excludes_rows_containing_all(spark, generate_data, tmpdir):
    snippets = [
        {"geo": "all"},
        {"os": "all"},
        {"channel": "all"},
        {},  # There must be a single data point
    ]
    input_df = generate_data(snippets)

    path = str(tmpdir.join("test/mode=weekly/"))
    backfill.backfill_topline_summary(input_df, path, overwrite=True)

    df = spark.read.parquet(path)
    assert df.where("geo = 'all'").count() == 0
    assert df.where("os = 'all'").count() == 0
    assert df.where("channel = 'all'").count() == 0


def test_multiple_dates_fails(generate_data, tmpdir):
    snippets = [{"date": "2016-01-01"}, {"date": "2016-01-08"}]
    input_df = generate_data(snippets)

    path = str(tmpdir)
    with pytest.raises(RuntimeError):
        backfill.backfill_topline_summary(input_df, path, overwrite=True)


# writes out partitions by report_date
def test_multiple_dates_batch(generate_data, tmpdir):
    snippets = [{"date": "2016-01-01"}, {"date": "2016-01-08"}]
    input_df = generate_data(snippets)

    path = str(tmpdir)
    backfill.backfill_topline_summary(input_df, path, batch=True, overwrite=True)

    parts = [name for name in os.listdir(path) if name.startswith("report_start")]
    assert len(parts) == 2


# data is correctly written to the correct location given default prefix
def test_cli_monthly(generate_data, tmpdir, monkeypatch):
    # generate test data
    snippets = [{"date": "2016-01-01"}]
    input_df = generate_data(snippets)

    # add a csv file to the test folder
    toplevel = tmpdir
    input_csv = toplevel.join("test.csv")

    csv_data = ",".join(input_df.columns) + "\n"
    for row in input_df.collect():
        csv_data += ",".join([unicode(x) for x in row]) + "\n"

    input_csv.write(csv_data)

    # create the output directory
    testdir = toplevel.join("test")
    output_path = str(testdir)

    # change s3_path to use file:// protocol
    def mock_format_output_path(bucket, prefix):
        return "file://{}/{}".format(bucket, prefix)

    monkeypatch.setattr(backfill, "format_output_path", mock_format_output_path)

    # Run the application via the cli
    runner = CliRunner()
    args = ["file://" + str(input_csv), "monthly", output_path]
    result = runner.invoke(backfill.main, args)
    assert result.exit_code == 0

    # test that the results can be read via spark
    path = str(testdir.join("topline_summary/v1/" "mode=monthly/report_start=20160101"))
    assert os.path.isdir(path)
