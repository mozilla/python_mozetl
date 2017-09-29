import functools
import arrow

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType,
    LongType, IntegerType, BooleanType
)

from mozetl.churn import churn, schema

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
"""

main_summary_schema = StructType([
    StructField("app_version", StringType(), True),
    StructField("attribution", StructType([
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("content", StringType(), True)]), True),
    StructField("channel", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("country", StringType(), True),
    StructField("default_search_engine", StringType(), True),
    StructField("distribution_id", StringType(), True),
    StructField("locale", StringType(), True),
    StructField("normalized_channel", StringType(), True),
    StructField("profile_creation_date", LongType(), True),
    StructField("submission_date_s3", StringType(), False),
    StructField("subsession_length", LongType(), True),
    StructField("subsession_start_date", StringType(), True),
    StructField("sync_configured", BooleanType(), True),
    StructField("sync_count_desktop", IntegerType(), True),
    StructField("sync_count_mobile", IntegerType(), True),
    StructField("timestamp", LongType(), True),
    StructField(SPBE + "total_uri_count", IntegerType(), True),
    StructField(SPBE + "unique_domains_count", IntegerType(), True)])


new_profile_schema = StructType([
    StructField("submission", StringType(), True),
    StructField("environment", StructType([
        StructField("profile", StructType([
            StructField("creation_date", LongType(), True),
        ]), True),
        StructField("build", StructType([
            StructField("version", StringType(), True),
        ]), True),
        StructField("partner", StructType([
            StructField("distribution_id", StringType(), True),
        ]), True),
        StructField("settings", StructType([
            StructField("locale", StringType(), True),
            StructField("is_default_browser", StringType(), True),
            StructField("default_search_engine", StringType(), True),
            StructField("attribution", StructType([
                StructField("source", StringType(), True),
                StructField("medium", StringType(), True),
                StructField("campaign", StringType(), True),
                StructField("content", StringType(), True)]), True),
        ]), True),
    ]), True),
    StructField("client_id", StringType(), True),
    StructField("metadata", StructType([
        StructField("geo_country", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("normalized_channel", StringType(), True),
        StructField("creation_timestamp", LongType(), True),
    ]), True)
])


# variables for conversion
SECONDS_PER_DAY = 60 * 60 * 24

# Generate the datasets
# Sunday, also the first day in this collection period.
subsession_start = arrow.get(2017, 1, 15).replace(tzinfo='utc')
week_start_ds = subsession_start.format("YYYYMMDD")

main_summary_sample = {
    "app_version": "57.0.0",
    "attribution": {
        "source": "source-value",
        "medium": "medium-value",
        "campaign": "campaign-value",
        "content": "content-value"
    },
    "channel": "release",
    "client_id": "client-id",
    "country": "US",
    "default_search_engine": "wikipedia",
    "distribution_id": "mozilla42",
    "locale": "en-US",
    "normalized_channel": "release",
    "profile_creation_date": subsession_start.timestamp / SECONDS_PER_DAY,
    "submission_date_s3": subsession_start.format("YYYYMMDD"),
    "subsession_length": 3600,
    "subsession_start_date": str(subsession_start),
    "sync_configured": False,
    "sync_count_desktop": 1,
    "sync_count_mobile": 1,
    "timestamp": subsession_start.timestamp * 10 ** 9,  # nanoseconds
    SPBE + "total_uri_count": 20,
    SPBE + "unique_domains_count": 3
}


new_profile_sample = {
    "submission": subsession_start.format("YYYYMMDD"),
    "environment": {
        "profile": {
            "creation_date": long(subsession_start.timestamp / SECONDS_PER_DAY)
        },
        "build": {
            "version": "57.0.0",
        },
        "partner": {
            "distribution_id": "mozilla57"
        },
        "settings": {
            "locale": "en-US",
            "is_default_browser": True,
            "default_search_engine": "google",
            "attribution": {
                "source": "source-value",
                "medium": "medium-value",
                "campaign": "campaign-value",
                "content": "content-value"
            },
        }
    },
    "client_id": "new-profile",
    "metadata": {
        "geo_country": "US",
        "timestamp": (subsession_start.timestamp+3600) * 10 ** 9,
        "normalized_channel": "release",
        "creation_timestamp": subsession_start.timestamp * 10 ** 9
    }
}


@pytest.fixture()
def generate_main_summary_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=main_summary_sample,
        schema=main_summary_schema
    )


@pytest.fixture()
def generate_new_profile_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=new_profile_sample,
        schema=new_profile_schema
    )


def generate_dates(subsession_date, submission_offset=0, creation_offset=0):
    """ Generate a tuple containing information about all pertinent dates
    in the input for the churn dataset.

    :date datetime.date: date as seen by the client
    :submission_offset int: offset into the future for submission_date_s3
    :creation_offset int: offset into the past for the profile creation date
    """

    submission_date = subsession_date.replace(days=submission_offset)
    profile_creation_date = subsession_date.replace(days=-creation_offset)

    date_snippet = {
        "subsession_start_date": str(subsession_date),
        "submission_date_s3": submission_date.format("YYYYMMDD"),
        "profile_creation_date": (
            profile_creation_date.timestamp / SECONDS_PER_DAY
        ),
        "timestamp": submission_date.timestamp * 10 ** 9
    }

    return date_snippet


@pytest.fixture
def single_profile_df(generate_main_summary_data):
    recent_ping = generate_dates(
        subsession_start.replace(days=3), creation_offset=3)

    # create a duplicate ping for this user, earlier than the previous
    old_ping = generate_dates(subsession_start)

    snippets = [recent_ping, old_ping]
    return generate_main_summary_data(snippets)


@pytest.fixture
def multi_profile_df(generate_main_summary_data):
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
    return generate_main_summary_data(snippets)


def test_extract_main_summary(spark, generate_main_summary_data):
    df = churn.extract(
        generate_main_summary_data(None),
        spark.createDataFrame([], new_profile_schema),
        week_start_ds, 1, 0, False
    )
    assert df.count() == 1


def test_clean_new_profile_sample_id(generate_new_profile_data):
    df = churn.clean_new_profile(
        generate_new_profile_data([{
            "client_id": "c4582ba1-79fc-1f47-ae2a-671118dccd8b"
        }])
    )
    expect = "4"

    assert df.first().sample_id == expect


def test_extract_new_profile(spark, generate_new_profile_data):
    df = churn.extract(
        spark.createDataFrame([], main_summary_schema),
        generate_new_profile_data([dict()]),
        week_start_ds, 1, 0, False
    )
    assert df.count() == 1

    row = df.first()
    assert row['subsession_length'] is None
    assert (row['profile_creation_date'] ==
            new_profile_sample['environment']['profile']['creation_date'])
    assert row['scalar_parent_browser_engagement_total_uri_count'] is None


def test_ignored_submissions_outside_of_period(spark, generate_main_summary_data):
    # All pings within 17 days of the submission start date are valid.
    # However, only pings with ssd within the 7 day retention period
    # are used for computation. Generate pings for this case.
    late_submission = generate_dates(subsession_start, submission_offset=18)
    early_subsession = generate_dates(subsession_start.replace(days=-7))
    late_submissions_df = generate_main_summary_data([late_submission, early_subsession])

    df = churn.extract(
        late_submissions_df,
        spark.createDataFrame([], new_profile_schema),
        week_start_ds, 7, 10, False
    )
    assert df.count() == 0


def test_multiple_sources_transform(effective_version,
                                    generate_main_summary_data,
                                    generate_new_profile_data):
    main_summary = generate_main_summary_data([
        {"client_id": "1"},
        {"client_id": "3"},
    ])
    new_profile = generate_new_profile_data([
        {"client_id": "1"},
        {"client_id": "2"},
        {"client_id": "2"},
    ])
    sources = churn.extract(main_summary, new_profile, week_start_ds, 1, 0, False)
    df = churn.transform(sources, effective_version, week_start_ds)

    # There are two different channels
    assert df.count() == 2

    assert (
        df.select(F.sum("n_profiles").alias("n_profiles"))
        .first().n_profiles
    ) == 3


def test_latest_submission_from_client_exists(single_profile_df,
                                              effective_version):
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


@pytest.fixture()
def test_transform(generate_main_summary_data, effective_version):
    def _test_transform(snippets, week_start=week_start_ds):
        return churn.transform(
            generate_main_summary_data(snippets),
            effective_version,
            week_start
        )

    return _test_transform


def test_transform_adheres_to_schema(test_transform):
    df = test_transform(None)

    def column_types(schema):
        return {
            col.name: col.dataType.typeName()
            for col in schema.fields
        }

    assert column_types(df.schema) == column_types(schema.churn_schema)


def test_nulled_stub_attribution(test_transform):
    df = test_transform([
        {
            'client_id': 'partial',
            'attribution': {
                'content': 'content'
            }
        },
        {
            'client_id': 'nulled',
            'attribution': None,
        }
    ])

    rows = (
        df
        .select('content', 'medium')
        .distinct()
        .collect()
    )
    actual = set([r.content for r in rows])
    expect = set(['content', 'unknown'])
    assert actual == expect

    actual = set([r.medium for r in rows])
    expect = set(['unknown'])
    assert actual == expect


def test_simple_string_dimensions(test_transform):
    df = test_transform([
        {
            'distribution_id': None,
            'default_search_engine': None,
            'locale': None
        }
    ])
    rows = df.collect()

    assert rows[0].distribution_id == 'unknown'
    assert rows[0].default_search_engine == 'unknown'
    assert rows[0].locale == 'unknown'


def test_empty_session_length(test_transform):
    df = test_transform([
        {'client_id': '1', 'subsession_length': None},
        {'client_id': '2', 'subsession_length': 3600},
        {'client_id': '3', 'subsession_length': None},
        {'client_id': '3', 'subsession_length': 3600},
    ])
    row = df.first()

    assert row.usage_hours == 2


def test_empty_total_uri_count(test_transform):
    df = test_transform([
        {SPBE + 'total_uri_count': None}
    ])
    rows = df.collect()

    assert rows[0].total_uri_count == 0


def test_total_uri_count_per_client(test_transform):
    df = test_transform([
        {SPBE + 'total_uri_count': 1},
        {SPBE + 'total_uri_count': 2}
    ])
    rows = df.collect()

    assert rows[0].total_uri_count == 3


def test_average_unique_domains_count(test_transform):
    df = test_transform([
        # averages to 4
        {'client_id': '1', SPBE + 'unique_domains_count': 6},
        {'client_id': '1', SPBE + 'unique_domains_count': 2},
        # averages to 8
        {'client_id': '2', SPBE + 'unique_domains_count': 12},
        {'client_id': '2', SPBE + 'unique_domains_count': 4}
    ])
    rows = df.collect()

    # (4 + 8) / 2 == 6
    assert rows[0].unique_domains_count_per_profile == 6


def test_top_countries(test_transform):
    df = test_transform([
        {"client_id": "1", "country": "US"},
        {"client_id": "2", "country": "HK"},
        {"client_id": "3", "country": "MR"},
        {"client_id": "4", "country": "??"},
        {"client_id": "5", "country": "Random"},
        {"client_id": "6", "country": "None"},
    ])

    def country_count(geo):
        return (
            df
            .where(F.col("geo") == geo)
            .groupBy("geo")
            .agg(F.sum("n_profiles").alias("n_profiles"))
            .first()
            .n_profiles
        )

    assert country_count("US") == 1
    assert country_count("HK") == 1
    assert country_count("ROW") == 4


def test_sync_usage(test_transform):
    # Generate a set of (test_func, snippet). Feed the snippets into
    # the transformation, and then run the test function on each
    # case afterwards.
    def test_case(df, uid=None, expect=None):
        actual = (
            df
            .where(F.col("distribution_id") == uid)
            .first()
            .sync_usage
        )
        assert actual == expect, "failed on {}".format(uid)

    def case(name, expect, desktop, mobile, configured):
        uid = "{}_{}".format(name, expect)
        snippet = {
            "client_id": uid,
            "distribution_id": uid,
            "sync_count_desktop": desktop,
            "sync_count_mobile": mobile,
            "sync_configured": configured,
        }
        test_func = functools.partial(test_case, uid=uid, expect=expect)
        return snippet, test_func

    snippets, test_func = zip(*[
        # unobservable
        case("1", "unknown", None, None, None),
        # no reported devices, and not configured
        case("2", "no", 0, 0, False),
        # no reported devices, but sync is configured
        case("3", "single", 0, 0, True),
        # has a single device, and is configured
        case("4", "single", 1, 0, True),
        # a single device is reported, but everything else is unobserved
        case("5", "single", 1, None, None),
        # sync is somehow disabled, but reporting a single device
        case("6", "single", None, 1, False),
        # same as case #5, but with multiple devices
        case("7", "multiple", 2, None, None),
        # multiple desktop devices, but sync is not configured
        case("8", "multiple", 0, 2, False),
        # a mobile and desktop device, but not configured
        case("9", "multiple", 1, 1, False),
    ])

    df = test_transform(list(snippets))

    for test in test_func:
        test(df)
