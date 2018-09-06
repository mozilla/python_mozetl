import functools
import pytest
from collections import namedtuple
from pyspark.sql.types import (
    StructField, ArrayType, BooleanType, StringType, LongType, StructType, DoubleType
)
from mozetl.search.aggregates import (
    search_aggregates, search_clients_daily,
    explode_search_counts, add_derived_columns, MAX_CLIENT_SEARCH_COUNT
)


# Some boilerplate to help define example dataframes for testing

# A helper class for declaratively creating dataframe factories
dataframe_field = namedtuple('dataframe_field', [
    'name',
    'default_value',
    'type',
    'nullable',
])


def to_field(field_tuple):
    """Create a dataframe_field from a tuple"""
    return dataframe_field(*field_tuple)


def get_dataframe_factory_config(fields):
    """Parse a list of dataframe_fields to a schema and set of default values"""
    schema = StructType([
        StructField(field.name, field.type, field.nullable)
        for field in fields
    ])
    default_sample = {field.name: field.default_value for field in fields}

    return schema, default_sample


@pytest.fixture
def define_dataframe_factory(dataframe_factory):
    def partial(fields):
        """Create a dataframe_factory from a set of field configs"""
        schema, default_sample = get_dataframe_factory_config(fields)
        return functools.partial(
            dataframe_factory.create_dataframe,
            base=default_sample,
            schema=schema
        )

    return partial


# Boilerplate for generating example main_summary tables
def generate_search_count(engine='google', source='urlbar', count=4):
    return {
        'engine': engine,
        'source': source,
        'count':  count,
    }


addons_type = ArrayType(StructType([
    StructField('addon_id', StringType(), False),
    StructField('blocklisted', BooleanType(), True),
    StructField('name', StringType(), True),
    StructField('user_disabled', BooleanType(), True),
    StructField('app_disabled', BooleanType(), True),
    StructField('version', StringType(), True),
    StructField('scope', LongType(), True),
    StructField('type', StringType(), True),
    StructField('foreign_install', BooleanType(), True),
    StructField('has_binary_components', BooleanType(), True),
    StructField('install_day', LongType(), True),
    StructField('update_day', LongType(), True),
    StructField('signed_state', LongType(), True),
    StructField('is_system', BooleanType(), True),
    StructField('is_web_extension', BooleanType(), True),
    StructField('multiprocess_compatible', BooleanType(), True),
]))


def generate_addon(addon_id, name, version):
    return {
        'addon_id': addon_id,
        'name': name,
        'version': version,
    }


active_addons = [
    generate_addon('random@mozilla.com', 'random', '0.1'),
    generate_addon(
        'followonsearch@mozilla.com',
        'Follow-on Search Telemetry',
        '0.9.5'
    )
]


search_type = ArrayType(StructType([
    StructField('engine', StringType(), False),
    StructField('source', StringType(), False),
    StructField('count', LongType(), False),
]))


main_summary_schema = [
    ('client_id', 'a', StringType(), False),
    ('sample_id', '42', StringType(), False),
    ('submission_date', '20170101', StringType(), False),
    ('os', 'windows', StringType(), True),
    ('channel', 'release', StringType(), True),
    ('country', 'DE', StringType(), True),
    ('locale', 'de', StringType(), True),
    ('search_cohort', None, StringType(), True),
    ('app_version', '54.0.1', StringType(), True),
    ('distribution_id', None, StringType(), True),
    ('subsession_counter', 1, LongType(), True),
    ('search_counts', [generate_search_count()], search_type, True),
    ('active_addons', active_addons, addons_type, True),
    # 30 minutes in active_ticks (30min * 60sec/min / 5sec/tick)
    ('active_ticks', 360, LongType(), True),
    (
        'scalar_parent_browser_engagement_tab_open_event_count',
        5,
        LongType(),
        True
    ),
    (
        'scalar_parent_browser_engagement_max_concurrent_tab_count',
        10,
        LongType(),
        True
    ),
    ('subsession_start_date', '2017-01-01 10:00', StringType(), False),
    # One hour per ping
    ('subsession_length', 60 * 60, LongType(), False),
    # Roughly 2016-01-01
    ('profile_creation_date', 16801, LongType(), False),
    ('default_search_engine', 'google', StringType(), False),
    (
        'default_search_engine_data_load_path',
        'jar:[app]/omni.ja!browser/google.xml',
        StringType(),
        False
    ),
    (
        'default_search_engine_data_submission_url',
        'https://www.google.com/search?q=&ie=utf-8&oe=utf-8&client=firefox-b',
        StringType(),
        False
    ),
]

exploded_schema = filter(lambda x: x[0] != 'search_counts', main_summary_schema) + [
    ('engine', 'google', StringType(), False),
    ('source', 'urlbar', StringType(), False),
    ('count', 4, LongType(), False),
]

derived_schema = exploded_schema + [
    ('type', 'chrome-sap', StringType(), False),
    ('addon_version', '0.9.5', StringType(), False),
]


@pytest.fixture()
def generate_main_summary_data(define_dataframe_factory):
    return define_dataframe_factory(map(to_field, main_summary_schema))


@pytest.fixture
def main_summary(generate_main_summary_data):
    return generate_main_summary_data(
        [
            {
                'client_id': 'b',
                'country': 'US',
            },
            {'app_version': '52.0.3'},
            {'distribution_id': 'totally not null'},
            {'search_counts': [
                generate_search_count(engine='bing'),
                generate_search_count(engine='yahoo'),
            ]}
        ] +
        # Some duplicate default rows to test aggregation
        [{}] * 5 +
        # Client with no searches
        [{'client_id': 'c', 'search_counts': None}]
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


@pytest.fixture()
def generate_exploded_data(define_dataframe_factory):
    return define_dataframe_factory(map(to_field, exploded_schema))


@pytest.fixture()
def exploded_simple_main_summary(generate_exploded_data):
    return generate_exploded_data([
        {'engine': 'yahoo'},
        {'engine': 'bing'},
    ])


@pytest.fixture()
def exploded_data_for_derived_cols(generate_exploded_data):
    return generate_exploded_data([
        {'source': 'sap:urlbar:SomeCodeHere'},
        {'source': 'follow-on:urlbar:SomeCodeHere'},
        {'source': 'in-content:sap:SomeCodeHere'},
        {'source': 'in-content:sap-follow-on:SomeCodeHere'},
        {'source': 'in-content:organic:something'},
        {'source': 'unknowngarbagestring'},
        {'source': 'urlbar'},
    ])


@pytest.fixture()
def derived_columns(define_dataframe_factory):
    # template for the expected results
    factory = define_dataframe_factory(map(to_field, derived_schema))

    return factory([
        {'source': 'sap:urlbar:SomeCodeHere',
         'type': 'tagged-sap'},
        {'source': 'follow-on:urlbar:SomeCodeHere',
         'type': 'tagged-follow-on'},
        {'source': 'in-content:sap:SomeCodeHere',
         'type': 'tagged-sap'},
        {'source': 'in-content:sap-follow-on:SomeCodeHere',
         'type': 'tagged-follow-on'},
        {'source': 'in-content:organic:something',
         'type': 'organic'},
        {'source': 'unknowngarbagestring',
         'type': 'unknown'},
        {'source': 'urlbar',
         'type': 'sap'},
    ])


@pytest.fixture()
def expected_search_dashboard_data(define_dataframe_factory):
    # template for the expected results
    factory = define_dataframe_factory(map(to_field, [
        ('submission_date', '20170101', StringType(), False),
        ('country', 'DE', StringType(), True),
        ('locale', 'de', StringType(), True),
        ('search_cohort', None, StringType(), True),
        ('app_version', '54.0.1', StringType(), True),
        ('distribution_id', None, StringType(), True),
        ('addon_version', '0.9.5', StringType(), False),
        ('default_search_engine', 'google', StringType(), False),
        ('engine', 'google', StringType(), False),
        ('source', 'urlbar', StringType(), False),
        ('tagged-sap', None, LongType(), True),
        ('tagged-follow-on', None, LongType(), True),
        ('tagged_sap', None, LongType(), True),
        ('tagged_follow_on', None, LongType(), True),
        ('sap', 4, LongType(), True),
        ('organic', None, LongType(), True),
        ('unknown', None, LongType(), True),
        ('client_count', 1, LongType(), True),
    ]))

    return factory([
        {'country': 'US'},
        {'app_version': '52.0.3'},
        {'distribution_id': 'totally not null'},
        {'engine': 'yahoo'},
        {'engine': 'bing'},
        {'sap': 20},
    ])


@pytest.fixture()
def expected_search_clients_daily_data(define_dataframe_factory):
    # template for the expected results
    factory = define_dataframe_factory(map(to_field, [
        ('client_id', 'a', StringType(), False),
        ('sample_id', '42', StringType(), False),
        ('submission_date', '20170101', StringType(), False),
        ('os', 'windows', StringType(), True),
        ('channel', 'release', StringType(), True),
        ('country', 'DE', StringType(), True),
        ('locale', 'de', StringType(), True),
        ('search_cohort', None, StringType(), True),
        ('app_version', '54.0.1', StringType(), True),
        ('distribution_id', None, StringType(), True),
        ('addon_version', '0.9.5', StringType(), False),
        ('engine', 'google', StringType(), True),
        ('source', 'urlbar', StringType(), True),
        ('tagged-sap', None, LongType(), True),
        ('tagged-follow-on', None, LongType(), True),
        ('tagged_sap', None, LongType(), True),
        ('tagged_follow_on', None, LongType(), True),
        ('sap', 4, LongType(), True),
        ('organic', None, LongType(), True),
        ('unknown', None, LongType(), True),
        # Roughly 2016-01-01
        ('profile_creation_date', 16801, LongType(), False),
        ('default_search_engine', 'google', StringType(), False),
        (
            'default_search_engine_data_load_path',
            'jar:[app]/omni.ja!browser/google.xml',
            StringType(),
            False
        ),
        (
            'default_search_engine_data_submission_url',
            'https://www.google.com/search?q=&ie=utf-8&oe=utf-8&client=firefox-b',
            StringType(),
            False
        ),
        ('sessions_started_on_this_day', 1, LongType(), True),
        (
            'profile_age_in_days',
            366,
            LongType(),
            True
        ),
        ('subsession_hours_sum', 1.0, DoubleType(), True),
        ('active_addons_count_mean', 2.0, DoubleType(), True),
        ('max_concurrent_tab_count_max', 10, LongType(), True),
        ('tab_open_event_count_sum', 5, LongType(), True),
        ('active_hours_sum', .5, DoubleType(), True),
    ]))

    return factory([
        {'client_id': 'b', 'country': 'US'},
        # Covers 5 dupe rows and custom app_version, distribution_id rows
        {
            'app_version': '52.0.3',
            'sap': 28,
            'sessions_started_on_this_day': 7,
            'subsession_hours_sum': 7.0,
            'tab_open_event_count_sum': 35,
            'active_hours_sum': 3.5,
        },
        {'engine': 'bing'},
        {'engine': 'yahoo'},
        {
            'client_id': 'c',
            'unknown': None,
            'sap': 0,
            'tagged-sap': None,
            'tagged-follow-on': None,
            'tagged_sap': None,
            'tagged_follow_on': None,
            'source': None,
            'engine': None,
        }
    ])


# Testing functions

def test_explode_search_counts(simple_main_summary,
                               exploded_simple_main_summary,
                               df_equals):
    actual = explode_search_counts(simple_main_summary)

    assert df_equals(actual, exploded_simple_main_summary)


def test_explode_search_counts_bing_absurd(generate_main_summary_data,
                                           generate_exploded_data,
                                           df_equals):
    main_summary_bing_absurd = generate_main_summary_data(
        [
            {'search_counts': [
                generate_search_count(engine='bing', count=(MAX_CLIENT_SEARCH_COUNT + 1)),
                generate_search_count(engine='yahoo'),
            ]}
        ]
    )

    # expected result only includes yahoo, because the bing entry had an absurd
    # number of searches
    expected = generate_exploded_data([{'engine': 'yahoo'}])

    actual = explode_search_counts(main_summary_bing_absurd)

    assert df_equals(expected, actual)


def test_add_derived_columns(exploded_data_for_derived_cols,
                             derived_columns,
                             df_equals):
    actual = add_derived_columns(exploded_data_for_derived_cols)

    assert df_equals(actual, derived_columns)


def test_basic_aggregation(main_summary,
                           expected_search_dashboard_data,
                           df_equals):
    actual = search_aggregates(main_summary)
    assert df_equals(actual, expected_search_dashboard_data)


def test_search_clients_daily(main_summary,
                              expected_search_clients_daily_data,
                              df_equals):
    actual = search_clients_daily(main_summary)

    assert df_equals(actual, expected_search_clients_daily_data)
