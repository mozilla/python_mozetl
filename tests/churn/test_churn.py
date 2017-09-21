import functools
from datetime import timedelta, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType,
    LongType, IntegerType, BooleanType
)

from mozetl.churn import churn

SPBE = "scalar_parent_browser_engagement_"

"""
Calendar for reference

    January 2017
Su Mo Tu We Th Fr Sa
 1  2  3  4  5  6  7
 8  9 10 11 12 13 14
15 16 17 18 19 20 21
22 23 24 25 26 27 28
29 30 31

# General guidelines

There are a few gotches that you should look out for while creating
new tests for this suite that are all churn specific.

* assign rows new client_ids if you want them to show up as unique
rows in the output datafrane
* create test fixtures for data shared across multiple tests
* use snippets to create small datasets for single purpose use
"""

main_schema = StructType([
    StructField("app_version",           StringType(), True),
    StructField("attribution",           StructType([
        StructField("source",            StringType(), True),
        StructField("medium",            StringType(), True),
        StructField("campaign",          StringType(), True),
        StructField("content",           StringType(), True)]), True),
    StructField("channel",               StringType(),  True),
    StructField("client_id",             StringType(),  True),
    StructField("country",               StringType(),  True),
    StructField("default_search_engine", StringType(),  True),
    StructField("distribution_id",       StringType(),  True),
    StructField("locale",                StringType(),  True),
    StructField("normalized_channel",    StringType(),  True),
    StructField("profile_creation_date", LongType(),    True),
    StructField("submission_date_s3",    StringType(),  False),
    StructField("subsession_length",     LongType(),    True),
    StructField("subsession_start_date", StringType(),  True),
    StructField("sync_configured",       BooleanType(), True),
    StructField("sync_count_desktop",    IntegerType(), True),
    StructField("sync_count_mobile",     IntegerType(), True),
    StructField("timestamp",             LongType(),    True),
    StructField(SPBE + "total_uri_count", IntegerType(), True),
    StructField(SPBE + "unique_domains_count", IntegerType(), True)])

default_sample = {
    "app_version":           "57.0.0",
    "attribution": {
        "source": "source-value",
        "medium": "medium-value",
        "campaign": "campaign-value",
        "content": "content-value"
    },
    "channel":               "release",
    "client_id":             "client-id",
    "country":               "US",
    "default_search_engine": "wikipedia",
    "distribution_id":       "mozilla42",
    "locale":                "en-US",
    "normalized_channel":    "release",
    "profile_creation_date": 17181,
    "submission_date_s3":    "20170115",
    "subsession_length":     3600,
    "subsession_start_date": "2017-01-15",
    "sync_configured":       False,
    "sync_count_desktop":    1,
    "sync_count_mobile":     1,
    "timestamp":             1484438400000000000,  # nanoseconds
    SPBE + "total_uri_count":      20,
    SPBE + "unique_domains_count": 3
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=main_schema
    )


def seconds_since_epoch(date):
    """ Calculate the total number of seconds since unix epoch.

    :date datetime: datetime to calculate seconds
    """
    epoch = datetime.utcfromtimestamp(0)
    return int((date - epoch).total_seconds())


def generate_dates(subsession_date, submission_offset=0, creation_offset=0):
    """ Generate a tuple containing information about all pertinent dates
    in the input for the churn dataset.

    :date datetime.date: date as seen by the client
    :submission_offset int: offset into the future for submission_date_s3
    :creation_offset int: offset into the past for the profile creation date
    """

    submission_date = subsession_date + timedelta(submission_offset)
    profile_creation_date = subsession_date - timedelta(creation_offset)

    # variables for conversion
    seconds_in_day = 60 * 60 * 24
    nanoseconds_in_second = 10**9

    date_snippet = {
        "subsession_start_date": (
            datetime.strftime(subsession_date, "%Y-%m-%d")
        ),
        "submission_date_s3": (
            datetime.strftime(submission_date, "%Y%m%d")
        ),
        "profile_creation_date": (
            seconds_since_epoch(profile_creation_date) / seconds_in_day
        ),
        "timestamp": (
            seconds_since_epoch(submission_date) * nanoseconds_in_second
        )
    }

    return date_snippet


# Generate the datasets
# Sunday, also the first day in this collection period.
subsession_start = datetime(2017, 1, 15)
week_start_ds = datetime.strftime(subsession_start, "%Y%m%d")


@pytest.fixture
def single_profile_df(generate_data):
    recent_ping = generate_dates(
        subsession_start + timedelta(3), creation_offset=3)

    # create a duplicate ping for this user, earlier than the previous
    old_ping = generate_dates(subsession_start)

    snippets = [recent_ping, old_ping]
    return generate_data(snippets)


@pytest.fixture
def multi_profile_df(generate_data):

    # generate different cohort of users based on creation date
    cohort_0 = generate_dates(subsession_start, creation_offset=14)
    cohort_1 = generate_dates(subsession_start, creation_offset=7)
    cohort_2 = generate_dates(subsession_start, creation_offset=0)

    # US has a user on release and beta
    # CA has a user on release
    # release users use firefox for 2 hours
    # beta users use firefox for 1 hour

    seconds_in_hour = 60 * 60

    user_0 = cohort_0.copy()
    user_0.update({
        "client_id": "user_0",
        "country": "US",
        "normalized_channel": "release",
        "subsession_length": seconds_in_hour * 2
    })

    user_1 = cohort_1.copy()
    user_1.update({
        "client_id": "user_1",
        "country": "US",
        "normalized_channel": "release",
        "subsession_length": seconds_in_hour * 2
    })

    user_2 = cohort_2.copy()
    user_2.update({
        "client_id": "user_2",
        "country": "CA",
        "normalized_channel": "beta",
        "subsession_length": seconds_in_hour
    })

    snippets = [user_0, user_1, user_2]
    return generate_data(snippets)


# testing d2v conflicting stability releases on the same day
# testing effective version
# testing effective release-channel version


def test_ignored_submissions_outside_of_period(generate_data):
    # All pings within 17 days of the submission start date are valid.
    # However, only pings with ssd within the 7 day retention period
    # are used for computation. Generate pings for this case.
    late_submission = generate_dates(subsession_start, submission_offset=18)
    early_subsession = generate_dates(subsession_start - timedelta(7))
    late_submissions_df = generate_data([late_submission, early_subsession])

    df = churn.extract_subset(late_submissions_df, week_start_ds, 7, 10, False)
    assert df.count() == 0


def test_latest_submission_from_client_exists(single_profile_df, effective_version):
    df = churn.transform(single_profile_df, effective_version, week_start_ds)
    assert df.count() == 1


def test_profile_usage_length(single_profile_df, effective_version):
    # there are two pings each with 1 hour of usage
    df = churn.transform(single_profile_df, effective_version, week_start_ds)
    rows = df.collect()

    assert rows[0].usage_hours == 2


def test_current_cohort_week_is_zero(single_profile_df, effective_version):
    df = churn.transform(single_profile_df, effective_version, week_start_ds)
    rows = df.collect()

    actual = rows[0].current_week
    expect = 0

    assert actual == expect


def test_multiple_cohort_weeks_exist(multi_profile_df, effective_version):
    df = churn.transform(multi_profile_df, effective_version, week_start_ds)
    rows = df.select('current_week').collect()

    actual = set([row.current_week for row in rows])
    expect = set([0, 1, 2])

    assert actual == expect


def test_cohort_by_channel_count(multi_profile_df, effective_version):
    df = churn.transform(multi_profile_df, effective_version, week_start_ds)
    rows = df.where(df.channel == 'release-cck-mozilla42').collect()

    assert len(rows) == 2


def test_cohort_by_channel_aggregates(multi_profile_df, effective_version):
    df = churn.transform(multi_profile_df, effective_version, week_start_ds)
    rows = (
        df
        .groupBy(df.channel)
        .agg(F.sum('n_profiles').alias('n_profiles'),
             F.sum('usage_hours').alias('usage_hours'))
        .where(df.channel == 'release-cck-mozilla42')
        .collect()
    )
    assert rows[0].n_profiles == 2
    assert rows[0].usage_hours == 4


@pytest.fixture
def nulled_attribution_df(generate_data):
    partial_attribution = {
        'client_id': 'partial',
        'attribution': {
            'content': 'content'
        }
    }

    nulled_attribution = {
        'client_id': 'nulled',
        'attribution': None,
    }

    snippets = [partial_attribution, nulled_attribution]
    return generate_data(snippets)


def test_nulled_stub_attribution_content(nulled_attribution_df, effective_version):
    df = churn.transform(nulled_attribution_df, effective_version,  week_start_ds)
    rows = (
        df
        .select('content')
        .distinct()
        .collect()
    )
    actual = set([r.content for r in rows])
    expect = set(['content', 'unknown'])

    assert actual == expect


def test_nulled_stub_attribution_medium(nulled_attribution_df, effective_version):
    df = churn.transform(nulled_attribution_df, effective_version, week_start_ds)
    rows = (
        df
        .select('medium')
        .distinct()
        .collect()
    )
    actual = set([r.medium for r in rows])
    expect = set(['unknown'])

    assert actual == expect


def test_simple_string_dimensions(generate_data, effective_version):
    snippets = [
        {
            'distribution_id': None,
            'default_search_engine': None,
            'locale': None
        }
    ]
    input_df = generate_data(snippets)
    df = churn.transform(input_df, effective_version, week_start_ds)
    rows = df.collect()

    assert rows[0].distribution_id == 'unknown'
    assert rows[0].default_search_engine == 'unknown'
    assert rows[0].locale == 'unknown'


def test_empty_total_uri_count(generate_data, effective_version):
    snippets = [{SPBE + 'total_uri_count': None}]
    input_df = generate_data(snippets)
    df = churn.transform(input_df, effective_version, week_start_ds)
    rows = df.collect()

    assert rows[0].total_uri_count == 0


def test_total_uri_count_per_client(generate_data, effective_version):
    snippets = [
        {SPBE + 'total_uri_count': 1},
        {SPBE + 'total_uri_count': 2}
    ]
    input_df = generate_data(snippets)
    df = churn.transform(input_df, effective_version, week_start_ds)
    rows = df.collect()

    assert rows[0].total_uri_count == 3


def test_average_unique_domains_count(generate_data, effective_version):
    snippets = [
        # averages to 4
        {'client_id': '1', SPBE + 'unique_domains_count': 6},
        {'client_id': '1', SPBE + 'unique_domains_count': 2},
        # averages to 8
        {'client_id': '2', SPBE + 'unique_domains_count': 12},
        {'client_id': '2', SPBE + 'unique_domains_count': 4}
    ]
    input_df = generate_data(snippets)
    df = churn.transform(input_df, effective_version, week_start_ds)
    rows = df.collect()

    # (4 + 8) / 2 == 6
    assert rows[0].unique_domains_count_per_profile == 6


@pytest.mark.skip()
def test_top_countries():
    assert churn.TOP_COUNTRIES("US") == "US"
    assert churn.TOP_COUNTRIES("HK") == "HK"
    assert churn.TOP_COUNTRIES("MR") == "ROW"
    assert churn.TOP_COUNTRIES("??") == "ROW"
    assert churn.TOP_COUNTRIES("Random") == "ROW"
    assert churn.TOP_COUNTRIES(None) == "ROW"