# -*- coding: utf-8 -*-

import pytest
from pyspark.sql import SparkSession

from mozetl import utils


@pytest.fixture(scope="session")
def spark(request):
    spark = (SparkSession
             .builder
             .appName("test_utils")
             .getOrCreate())

    yield spark

    spark.stop()


def test_write_csv_ascii(spark, tmpdir):
    test_data = ['hello', 'world']
    input_data = [{'a': x} for x in test_data]
    df = spark.createDataFrame(input_data)

    path = str(tmpdir.join('test_data.csv'))
    utils.write_csv(df, path)

    with open(path, 'rb') as f:
        data = f.read()

    assert data.rstrip().split('\r\n')[1:] == test_data


def test_write_csv_valid_unicode(spark, tmpdir):
    test_data = [u'∆', u'∫', u'∬']
    input_data = [{'a': x} for x in test_data]
    df = spark.createDataFrame(input_data)

    path = str(tmpdir.join('test_data.csv'))
    utils.write_csv(df, path)

    with open(path, 'rb') as f:
        data = f.read().decode('utf-8')

    assert data.rstrip().split('\r\n')[1:] == test_data
