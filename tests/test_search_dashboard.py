import functools
import pytest
from pyspark.sql.types import (
    StructField, ArrayType, StringType, LongType, StructType
)
from mozetl.search.dashboard import search_dashboard_etl


# Boilerplate for main_summary data
main_schema = StructType([
    StructField('submission_date_s3', StringType(), False),
    StructField('submission_date',    StringType(), False),
    StructField('country',            StringType(), True),
    StructField('app_version',        StringType(), True),
    StructField('distribution_id',    StringType(), True),
    StructField('search_counts',      ArrayType(StructType([
        StructField('engine', StringType(), False),
        StructField('source', StringType(), False),
        StructField('count',  LongType(),   False),
    ]))),
])


def generate_search_count(engine='google', source='urlbar', count=4):
    return {
        'engine': engine,
        'source': source,
        'count':  count,
    }


google = generate_search_count()
yahoo = generate_search_count(engine='yahoo')
bing = generate_search_count(engine='bing')


main_default_sample = {
    'submission_date_s3': '20170101',
    'submission_date':    '20170101',
    'country':            'DE',
    'app_version':        '54.0.1',
    'distribution_id':    None,
    'search_counts':      [google],
}


@pytest.fixture()
def generate_main_summary_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=main_default_sample,
        schema=main_schema
    )


@pytest.fixture
def main_summary(generate_main_summary_data):
    return generate_main_summary_data(
        [
            {'country': 'US'},
            {'app_version': '52.0.3'},
            {'distribution_id': 'totally not null'},
            {'search_counts': [bing, yahoo]},
        ] + 
        [{}] * 5 # Some duplicate default rows to test aggregation
    )


# Boilerplate for search_dashboard data
search_dashboard_schema = StructType([
    StructField('submission_date',    StringType(), False),
    StructField('country',            StringType(), True),
    StructField('app_version',        StringType(), True),
    StructField('distribution_id',    StringType(), True),
    StructField('engine',             StringType(), False),
    StructField('source',             StringType(), False),
    StructField('count',              LongType(),   False),
])


search_dashboard_default_sample = {
    'submission_date':    '20170101',
    'country':            'DE',
    'app_version':        '54.0.1',
    'distribution_id':    None,
    'engine':             'google',
    'source':             'urlbar',
    'count' :             4,
}


@pytest.fixture()
def generate_search_dashboard_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=search_dashboard_default_sample,
        schema=search_dashboard_schema
    )


@pytest.fixture()
def expected_search_dashboard_data(generate_search_dashboard_data):
    return generate_search_dashboard_data([
        {'country': 'US'},
        {'app_version': '52.0.3'},
        {'distribution_id': 'totally not null'},
        {'engine': 'yahoo'},
        {'engine': 'bing'},
        {'count': 20},
    ])


# Testing functions

def test_basic_aggregation(main_summary, expected_search_dashboard_data):
    actual = search_dashboard_etl(main_summary)


    assert sorted(actual.collect()) == \
        sorted(expected_search_dashboard_data.collect())
