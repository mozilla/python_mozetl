# -*- coding: utf-8 -*-
import datetime as DT
import boto3
import pytest
from moto import mock_s3

from mozetl import utils

default_sample = {"test": "data"}


@pytest.fixture()
def generate_data(dataframe_factory):
    """Return a function that generates a dataframe from a list of strings."""

    def create_dataframe_from_list(row=None):
        snippets = None if row is None else [{"test": x} for x in row]
        return dataframe_factory.create_dataframe(snippets, default_sample)

    return create_dataframe_from_list


def test_write_csv_ascii(generate_data, tmpdir):
    test_data = ["hello", "world"]
    df = generate_data(test_data)

    path = str(tmpdir.join("test_data.csv"))
    utils.write_csv(df, path)

    with open(path, "rb") as f:
        data = f.read()

    assert data.rstrip().split("\r\n")[1:] == test_data


def test_generate_filter_parameters():
    """
    Check the two meaningful cases: DAU (0 days) and MAU(28 days).
    """
    expected0 = {
        "min_activity_iso": "2017-01-31",
        "max_activity_iso": "2017-02-01",
        "min_submission_string": "20170131",
        "max_submission_string": "20170210",
    }
    actual0 = utils.generate_filter_parameters(DT.date(2017, 1, 31), 0)
    assert expected0 == actual0, str(actual0)

    expected28 = {
        "min_activity_iso": "2017-01-03",
        "max_activity_iso": "2017-02-01",
        "min_submission_string": "20170103",
        "max_submission_string": "20170210",
    }
    actual28 = utils.generate_filter_parameters(DT.date(2017, 1, 31), 28)
    assert expected28 == actual28


def test_write_csv_valid_unicode(generate_data, tmpdir):
    test_data = ["∆", "∫", "∬"]
    df = generate_data(test_data)

    path = str(tmpdir.join("test_data.csv"))
    utils.write_csv(df, path)

    with open(path, "rb") as f:
        data = f.read().decode("utf-8")

    assert data.rstrip().split("\r\n")[1:] == test_data


@mock_s3
def test_write_csv_to_s3(generate_data):
    bucket = "test-bucket"
    key = "test.csv"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(["foo"]), bucket, key)

    body = conn.Object(bucket, key).get()["Body"].read().decode("utf-8")

    # header + 1x row = 2
    assert len(body.rstrip().split("\n")) == 2


@mock_s3
def test_write_csv_to_s3_no_header(generate_data):
    bucket = "test-bucket"
    key = "test.csv"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(), bucket, key, header=False)

    body = conn.Object(bucket, key).get()["Body"].read().decode("utf-8")

    assert len(body.rstrip().split("\n")) == 1


@mock_s3
def test_write_csv_to_s3_existing(generate_data):
    bucket = "test-bucket"
    key = "test.csv"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(["foo"]), bucket, key)
    utils.write_csv_to_s3(generate_data(["foo", "bar"]), bucket, key)

    body = conn.Object(bucket, key).get()["Body"].read().decode("utf-8")

    # header + 2x row = 3
    assert len(body.rstrip().split("\n")) == 3
