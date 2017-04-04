import json

import pytest

from pyspark.sql import SparkSession
from python_etl import churn


# Initialize a spark context
@pytest.fixture(scope="session")
def spark(request):
    spark = (SparkSession
             .builder
             .appName("churn_test")
             .getOrCreate())

    # teardown
    request.addfinalizer(lambda: spark.stop())

    return spark


"""
Calendar for reference

    January 2017
Su Mo Tu We Th Fr Sa
 1  2  3  4  5  6  7
 8  9 10 11 12 13 14
15 16 17 18 19 20 21
22 23 24 25 26 27 28
29 30 31
"""


def create_row():
    sample = {
        "app_version":           "50.0.1",
        "attribution":           None,
        "channel":               "release",
        "client_id":             "client-id",
        "country":               "US",
        "default_search_engine": "wikipedia",
        "distribution_id":       None,
        "locale":                "en-US",
        "normalized_channel":    "release",
        "profile_creation_date": 17000,
        "submission_date_s3":    "20170118",
        "subsession_length":     1000,
        "subsession_start_date": "01/15/2017 00:00",
        "sync_configured":       False,
        "sync_count_desktop":    None,
        "sync_count_mobile":     None,
        "timestamp":             1491244610603260700,
        "total_uri_count":       20,
        "unique_domains_count":  3
    }
    return json.dumps(sample)


@pytest.fixture
def main_df(spark):
    jsonRDD = spark.sparkContext.parallelize([create_row()])
    return spark.read.json(jsonRDD)


def test_something(main_df):
    expected = 1000
    actual = main_df.select("subsession_length").collect()[0].subsession_length

    assert actual == expected
