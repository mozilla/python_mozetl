import functools

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_s3
from pyspark.sql import Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, \
    LongType

from mozetl.search import search_rollups

schema = StructType([
    StructField("country", StringType(), True),
    StructField("default_search_engine", StringType(), True),
    StructField("distribution_id", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("search_counts",
                ArrayType(
                    StructType([
                        StructField("engine", StringType(), True),
                        StructField("source", StringType(), True),
                        StructField("count", LongType(), True)
                    ]), True),
                True)
])

default_sample = {
    'country': u'US',
    'default_search_engine': u'mozilla',
    'distribution_id': u'distribution_id',
    'locale': u'en-US',
    'client_id': u'profile_id',
    'search_counts': [
        Row(engine=u'hooli', source=u'searchbar', count=1)
    ]
}


def search_row(engine='hooli', source='searchbar', count=1):
    return Row(
        engine=unicode(engine),
        source=unicode(source),
        count=count
    )


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=schema,
    )


# a client switches countries mid-day, affects shares
def test_single_client_shares_multiple_countries(generate_data):
    snippets = [
        {'country': 'US'},
        {'country': 'CA'},
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    assert result.count() == 2
    assert result.select(F.sum("profile_share")).first()[0] == 1.0


# a profile has two different search counts
def test_single_client_multiple_search_engines(generate_data):
    snippets = [
        {
            'search_counts': [
                search_row(engine="hooli", count=2),
                search_row(engine="altavista", count=4),
            ]
        },
        {'search_counts': [search_row("altavista")]},
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    assert result.where("search_provider='hooli'").count() == 1
    assert result.select(F.sum("search_count")).first()[0] == 7


def test_filter_incontent_searches(generate_data):
    snippets = [
        {'search_counts': [search_row(source="in-content")]},   # no
        {'search_counts': [search_row(source="contextmenu")]},  # yes
        {'search_counts': [search_row(source="abouthome")]},    # yes
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    # in-content search should be filtered
    assert result.select(F.sum("search_count")).first()[0] == 2


# 2 profiles have different search counts
def test_multiple_clients_multiple_search_engines(generate_data):
    snippets = [
        {
            'client_id': 'profile_0',
            'search_counts': [
                search_row(engine="hooli", count=18),
                search_row(engine="altavista", count=3),
            ]
        },
        {
            'client_id': 'profile_1',
            'search_counts': [
                search_row(engine="hooli", count=3),
                search_row(engine="altavista", count=18),
            ]
        },
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    assert result.count() == 2
    assert result.select(F.sum("search_count")).first()[0] == 42
    assert (
               result
               .where("search_provider='hooli'")
               .select(F.sum("search_count"))
               .first()[0]
           ) == 21


# searches are broken down by country
def test_searches_by_country(generate_data):
    snippets = [
        {
            'client_id': 'profile_0',
            'country': 'US',
            'search_counts': [search_row(engine="hooli", count=2)]
        },
        {
            'client_id': 'profile_1',
            'country': 'US',
            'search_counts': [search_row(engine="altavista", count=2)]
        },
        {
            'client_id': 'profile_2',
            'country': 'CA',
            'search_counts': [search_row(engine="altavista", count=2)]
        },
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    def search_by_country(df, geo):
        return (
            df
            .where(F.col("country") == geo)
            .select(F.sum("search_count"))
            .first()[0]
        )

    assert result.count() == 3
    assert search_by_country(result, "US") == 4
    assert search_by_country(result, "CA") == 2


def test_null_row(generate_data):
    # everything except client_id is null
    snippets = [
        {
            'country': None,
            'default_search_engine': None,
            'distribution_id': None,
            'locale': None,
            'search_counts': None,
        }
    ]
    df = generate_data(snippets)
    result = search_rollups.transform(df, "daily")

    row = result.where("country<>'US'").first()
    assert row.country == "XX"
    assert row.search_provider == "NO_SEARCHES"
    assert row.default_provider == "NO_DEFAULT"
    assert row.locale == "xx"
    assert row.distribution_id == "MOZILLA"
    assert row.search_count == 0


def test_transform_excludes_profile_shares_for_monthly(generate_data):
    df = generate_data(None)
    result = search_rollups.transform(df, mode="monthly")

    assert "profile_share" not in result.columns


@mock_s3
def test_cli_daily(generate_data, monkeypatch):
    bucket = 'test-bucket'
    prefix = 'test-prefix'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    def mock_extract(spark_session, source_path, start_date, mode):
        return generate_data(None)
    monkeypatch.setattr(search_rollups, 'extract', mock_extract)

    runner = CliRunner()
    args = [
        "--start_date", '20170501',
        "--mode", 'daily',
        "--bucket", bucket,
        "--prefix", prefix,
    ]
    result = runner.invoke(search_rollups.main, args)
    assert result.exit_code == 0

    csv_key = "/daily/processed-2017-05-01.csv"

    body = (
        conn
        .Object(bucket, prefix + csv_key)
        .get()['Body']
        .read().decode('utf-8')
    )

    row = body.rstrip().split(',')
    assert row[0] == '2017-05-01'


@mock_s3
def test_cli_monthly(generate_data, monkeypatch):
    bucket = 'test-bucket'
    prefix = 'test-prefix'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    def mock_extract(spark_session, source_path, start_date, mode):
        return generate_data(None)
    monkeypatch.setattr(search_rollups, 'extract', mock_extract)

    runner = CliRunner()
    args = [
        "--start_date", '20170501',
        "--mode", 'monthly',
        "--bucket", bucket,
        "--prefix", prefix,
    ]
    result = runner.invoke(search_rollups.main, args)
    assert result.exit_code == 0

    csv_key = "/monthly/processed-2017-05-01.csv"

    body = (
        conn
        .Object(bucket, prefix + csv_key)
        .get()['Body']
        .read().decode('utf-8')
    )

    row = body.rstrip().split(',')
    assert row[0] == '2017-05'


@mock_s3
def test_cli_monthly_offset(generate_data, monkeypatch):
    bucket = 'test-bucket'
    prefix = 'test-prefix'

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=bucket)

    def mock_extract(spark_session, source_path, start_date, mode):
        return generate_data(None)
    monkeypatch.setattr(search_rollups, 'extract', mock_extract)

    runner = CliRunner()
    args = [
        "--start_date", '20170502',
        "--mode", 'monthly',
        "--bucket", bucket,
        "--prefix", prefix,
    ]
    result = runner.invoke(search_rollups.main, args)
    assert result.exit_code == 0

    body = (
        conn
        .Object(bucket, prefix + '/monthly/processed-2017-05-01.csv')
        .get()['Body']
        .read().decode('utf-8')
    )

    row = body.rstrip().split(',')
    assert row[0] == '2017-05'
