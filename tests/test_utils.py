# -*- coding: utf-8 -*-

import boto3
import pytest
from moto import mock_s3

from mozetl import utils

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
        conn.Object(bucket, key)
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
        conn.Object(bucket, key)
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
        conn.Object(bucket, key)
            .get()['Body']
            .read().decode('utf-8')
    )

    # header + 2x row = 3
    assert len(body.rstrip().split('\n')) == 3
