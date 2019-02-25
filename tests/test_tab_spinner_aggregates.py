import pytest
import json
import pandas as pd
import boto3
from click.testing import CliRunner
from moto import mock_s3

from mozetl.tab_spinner.utils import get_short_and_long_spinners
from mozetl.tab_spinner import tab_spinner


def create_row():
    with open("tests/tab_spinner_ping.json") as infile:
        return json.load(infile)


@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize([create_row()])


def test_simple_transform(simple_rdd, spark_context):
    results = get_short_and_long_spinners(simple_rdd)

    # get rid of pd index
    result = {
        k: {build_id: series.values for build_id, series in v}
        for k, v in results.items()
    }

    expected = {
        "short": {"20170101": pd.Series([0, 1.0, 0, 0, 0, 0, 0, 0])},
        "long": {"20170101": pd.Series([0, 0.0, 1.0, 0, 0, 0, 0, 0])},
    }

    for k, v in expected.items():
        for build_id, series in v.items():
            assert all(result[k][build_id] == series)


@mock_s3
def test_tab_spinner_main(monkeypatch, simple_rdd):
    bucket = "telemetry-public-analysis-2"
    prefix = "spinner-severity-generator/data/severities_by_build_id_nightly.json"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    class Dataset:
        @staticmethod
        def from_source(*args, **kwargs):
            return Dataset()

        def where(self, *args, **kwargs):
            return self

        def records(self, *args, **kwargs):
            return simple_rdd

    monkeypatch.setattr("mozetl.tab_spinner.generate_counts.Dataset", Dataset)

    runner = CliRunner()
    result = runner.invoke(tab_spinner.main, [])
    assert result.exit_code == 0

    # should not raise a botocore exception
    conn.Object(bucket, prefix).load()
