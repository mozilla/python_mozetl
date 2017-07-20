import functools
import pytest
from pyspark.sql.types import (
    StructField, ArrayType, StringType, LongType, StructType
)
from mozetl.search.dashboard import search_dashboard_etl, explode_search_counts


def define_dataframe_factory(field_dict):
    return (StructType(
        [StructField(name, *value[0]) for name, value in field_dict.items()]
    ), {name: value[1] for name, value in field_dict.items()})


def generate_search_count(engine='google', source='urlbar', count=4):
    return {
        'engine': engine,
        'source': source,
        'count':  count,
    }


main_schema, main_default_sample = define_dataframe_factory({
    'submission_date_s3': ((StringType(), False), '20170101'),
    'submission_date':    ((StringType(), False), '20170101'),
    'country':            ((StringType(), True),  'DE'),
    'app_version':        ((StringType(), True),  '54.0.1'),
    'distribution_id':    ((StringType(), True),  None),
    'search_counts':      (
        (ArrayType(StructType([
            StructField('engine', StringType(), False),
            StructField('source', StringType(), False),
            StructField('count',  LongType(),   False),
        ])), True),
        generate_search_count()
    )
})


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
            {'search_counts': [
                generate_search_count(engine='bing'),
                generate_search_count(engine='yahoo'),
            ]}
        ] + 
        [{}] * 5 # Some duplicate default rows to test aggregation
    )


@pytest.fixture
def simple_main_summary(generate_main_summary_data):
    return generate_main_summary_data(
        [
            {'search_counts': [
                generate_search_count(engine='bing'),
                generate_search_count(engine='yahoo'),
            ]}
        ]
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


@pytest.fixture()
def exploded_simple_main_summary(dataframe_factory):
    return dataframe_factory.create_dataframe(
        [
            {'engine': 'yahoo'},
            {'engine': 'bing'},
        ],
        {
            'submission_date_s3': '20170101',
            'submission_date':    '20170101',
            'country':            'DE',
            'app_version':        '54.0.1',
            'distribution_id':    None,
            'engine': 'google',
            'source': 'urlbar',
            'count':  4,
        },

        StructType([
            StructField('submission_date_s3', StringType(), False),
            StructField('submission_date',    StringType(), False),
            StructField('country',            StringType(), True),
            StructField('app_version',        StringType(), True),
            StructField('distribution_id',    StringType(), True),
            StructField('engine',             StringType(), False),
            StructField('source',             StringType(), False),
            StructField('count',              LongType(),   False),
        ])
    )


# Testing functions

# def test_explode(simple_main_summary):
#     print explode_search_counts(simple_main_summary).collect()
#     assert False
 
 
def test_explode_search_counts(simple_main_summary,
                               exploded_simple_main_summary,
                               df_equals):
    actual = explode_search_counts(simple_main_summary)

    for x in zip(actual.collect(), exploded_simple_main_summary.collect()):
        print x[0]
        print x[1]
    assert df_equals(actual, exploded_simple_main_summary)


# def test_basic_aggregation(main_summary, expected_search_dashboard_data):
#     actual = search_dashboard_etl(main_summary)
# 
#     assert df_equals(actual, expected_search_dashboard_data)
