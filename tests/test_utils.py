# -*- coding: utf-8 -*-
import datetime as DT
import boto3
import os
import pytest
from moto import mock_s3

from mozetl import utils
from mozetl.utils import read_main_summary

default_sample = {
    "test": "data",
}


@pytest.fixture()
def generate_data(dataframe_factory):
    """Return a function that generates a dataframe from a list of strings."""

    def create_dataframe_from_list(row=None):
        snippets = None if row is None else [{'test': x} for x in row]
        return dataframe_factory.create_dataframe(snippets, default_sample)

    return create_dataframe_from_list


def test_write_csv_ascii(generate_data, tmpdir):
    test_data = ['hello', 'world']
    df = generate_data(test_data)

    path = str(tmpdir.join('test_data.csv'))
    utils.write_csv(df, path)

    with open(path, 'rb') as f:
        data = f.read()

    assert data.rstrip().split('\r\n')[1:] == test_data


def test_generate_filter_parameters():
    """
    Check the two meaningful cases: DAU (0 days) and MAU(28 days).
    """
    expected0 = {
        'min_activity_iso': '2017-01-31',
        'max_activity_iso': '2017-02-01',
        'min_submission_string': '20170131',
        'max_submission_string': '20170210'
    }
    actual0 = utils.generate_filter_parameters(DT.date(2017, 1, 31), 0)
    assert expected0 == actual0, str(actual0)

    expected28 = {
        'min_activity_iso': '2017-01-03',
        'max_activity_iso': '2017-02-01',
        'min_submission_string': '20170103',
        'max_submission_string': '20170210'
    }
    actual28 = utils.generate_filter_parameters(DT.date(2017, 1, 31), 28)
    assert expected28 == actual28


def test_write_csv_valid_unicode(generate_data, tmpdir):
    test_data = [u'∆', u'∫', u'∬']
    df = generate_data(test_data)

    path = str(tmpdir.join('test_data.csv'))
    utils.write_csv(df, path)

    with open(path, 'rb') as f:
        data = f.read().decode('utf-8')

    assert data.rstrip().split('\r\n')[1:] == test_data


@mock_s3
def test_write_csv_to_s3(generate_data):
    bucket = 'test-bucket'
    key = 'test.csv'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(["foo"]), bucket, key)

    body = (
        conn
        .Object(bucket, key)
        .get()['Body']
        .read().decode('utf-8')
    )

    # header + 1x row = 2
    assert len(body.rstrip().split('\n')) == 2


@mock_s3
def test_write_csv_to_s3_no_header(generate_data):
    bucket = 'test-bucket'
    key = 'test.csv'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(), bucket, key, header=False)

    body = (
        conn
        .Object(bucket, key)
        .get()['Body']
        .read().decode('utf-8')
    )

    assert len(body.rstrip().split('\n')) == 1


@mock_s3
def test_write_csv_to_s3_existing(generate_data):
    bucket = 'test-bucket'
    key = 'test.csv'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    utils.write_csv_to_s3(generate_data(["foo"]), bucket, key)
    utils.write_csv_to_s3(generate_data(["foo", "bar"]), bucket, key)

    body = (
        conn
        .Object(bucket, key)
        .get()['Body']
        .read().decode('utf-8')
    )

    # header + 2x row = 3
    assert len(body.rstrip().split('\n')) == 3


@pytest.fixture
def sample_data_path():
    root = os.path.dirname(__file__)
    return os.path.join(root, 'resources', 'sample_main_summary', 'v1')


@pytest.fixture
def sample_data(spark, sample_data_path):
    data = spark.read.parquet(sample_data_path)
    return data


@pytest.fixture
def sample_data_merged(spark, sample_data_path):
    reader = spark.read.option("mergeSchema", "true")
    data = reader.parquet(sample_data_path)
    return data


def test_parquet_read(sample_data):
    count = sample_data.count()
    assert count == 90


def test_schema_evolution(sample_data, sample_data_merged):
    # submission_date_s3, sample_id, document_id, a
    assert len(sample_data.columns) == 4

    # submission_date_s3, sample_id, document_id, a, b, c
    assert len(sample_data_merged.columns) == 6


def test_read_main_summary(spark, sample_data_path,
                           sample_data, sample_data_merged):
    data = read_main_summary(spark, path=sample_data_path)
    assert data.count() == sample_data.count()
    assert data.count() == sample_data_merged.count()
    assert len(data.columns) == len(sample_data_merged.columns)

    data = read_main_summary(spark, path=sample_data_path, mergeSchema=False)
    assert len(data.columns) == len(sample_data.columns)


def test_base_path(spark, sample_data_path):
    parts = "submission_date_s3=20170101/sample_id=2"
    path = "{}/{}".format(sample_data_path, parts)
    data = spark.read.option("basePath", sample_data_path) \
                     .option("mergeSchema", True) \
                     .parquet(path)
    assert data.count() == 10
    assert len(data.columns) == 4

    parts = ["submission_date_s3=20170101/sample_id=2",
             "submission_date_s3=20170102/sample_id=2"]
    paths = ["{}/{}".format(sample_data_path, p) for p in parts]
    data = spark.read.option("basePath", sample_data_path) \
                     .option("mergeSchema", True) \
                     .parquet(*paths)
    assert data.count() == 20
    assert len(data.columns) == 5


def test_part_read_main_summary(spark, sample_data_path):
    data = read_main_summary(spark, path=sample_data_path,
                             submission_date_s3=['20170102'],
                             sample_id=[2, 3])
    assert data.count() == 20
    assert len(data.columns) == 5

    data = read_main_summary(spark, path=sample_data_path,
                             submission_date_s3=['20170102', '20170103'],
                             sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 6

    data = read_main_summary(spark, path=sample_data_path, mergeSchema=False,
                             submission_date_s3=['20170102', '20170103'],
                             sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 5

    data = read_main_summary(spark, path=sample_data_path,
                             submission_date_s3=['20170101', '20170103'],
                             sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 6

    data = read_main_summary(spark, path=sample_data_path, mergeSchema=False,
                             submission_date_s3=['20170101', '20170103'],
                             sample_id=[2])
    assert data.count() == 20
    assert len(data.columns) == 4

    data = read_main_summary(spark, path=sample_data_path,
                             submission_date_s3=['20170101', '20170103'])
    assert data.count() == 60
    assert len(data.columns) == 6

    data = read_main_summary(spark, path=sample_data_path, sample_id=[1])
    assert data.count() == 30


def get_min_max_id(df):
    ids = df.select("document_id").orderBy("document_id").collect()
    return [ids[0], ids[-1]]


# A trailing slash shouldn't make a difference.
def test_trailing_slash(spark, sample_data_path):
    with_slash = sample_data_path + '/'
    without_slash = sample_data_path

    assert with_slash[-1] == '/'
    assert without_slash[-1] != '/'

    dw = read_main_summary(spark, path=with_slash, submission_date_s3=['20170102'])
    dwo = read_main_summary(spark, path=without_slash, submission_date_s3=['20170102'])

    dw_min, dw_max = get_min_max_id(dw)
    dwo_min, dwo_max = get_min_max_id(dwo)

    assert dw_min == dwo_min
    assert dw_max == dwo_max
