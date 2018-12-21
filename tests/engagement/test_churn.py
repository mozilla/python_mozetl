import functools
import copy

import pytest
from pyspark.sql import functions as F

from mozetl.engagement.churn import job, schema
from . import data
from .data import SUBSESSION_START, WEEK_START_DS

SPBE = "scalar_parent_browser_engagement_"


@pytest.fixture
def single_profile_df(generate_main_summary_data):
    recent_ping = data.generate_dates(
        SUBSESSION_START.replace(days=3), creation_offset=3)

    # create a duplicate ping for this user, earlier than the previous
    old_ping = data.generate_dates(SUBSESSION_START)

    snippets = [recent_ping, old_ping]
    return generate_main_summary_data(snippets)


@pytest.fixture
def multi_profile_df(generate_main_summary_data):
    # generate different cohort of users based on creation date
    cohort_0 = data.generate_dates(SUBSESSION_START, creation_offset=14)
    cohort_1 = data.generate_dates(SUBSESSION_START, creation_offset=7)
    cohort_2 = data.generate_dates(SUBSESSION_START, creation_offset=0)

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
    df = job.extract(
        generate_main_summary_data(None),
        spark.createDataFrame([], data.new_profile_schema),
        WEEK_START_DS, 1, 0, False
    )
    assert df.count() == 1


def test_clean_new_profile_sample_id(generate_new_profile_data):
    df = job.clean_new_profile(
        generate_new_profile_data([{
            "client_id": "c4582ba1-79fc-1f47-ae2a-671118dccd8b"
        }])
    )
    expect = "4"

    assert df.first().sample_id == expect


def test_extract_new_profile(spark, generate_new_profile_data):
    df = job.extract(
        spark.createDataFrame([], data.main_summary_schema),
        generate_new_profile_data([dict()]),
        WEEK_START_DS, 1, 0, False
    )
    assert df.count() == 1

    row = df.first()
    assert row['subsession_length'] is None
    assert (row['profile_creation_date'] ==
            data.new_profile_sample['environment']['profile']['creation_date'])
    assert row['scalar_parent_browser_engagement_total_uri_count'] is None


def test_ignored_submissions_outside_of_period(spark, generate_main_summary_data):
    # All pings within 17 days of the submission start date are valid.
    # However, only pings with ssd within the 7 day retention period
    # are used for computation. Generate pings for this case.
    late_submission = data.generate_dates(SUBSESSION_START, submission_offset=18)
    early_subsession = data.generate_dates(SUBSESSION_START.replace(days=-7))
    late_submissions_df = generate_main_summary_data([late_submission, early_subsession])

    df = job.extract(
        late_submissions_df,
        spark.createDataFrame([], data.new_profile_schema),
        WEEK_START_DS, 7, 10, False
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
    sources = job.extract(main_summary, new_profile, WEEK_START_DS, 1, 0, False)
    df = job.transform(sources, effective_version, WEEK_START_DS)

    # There are two different channels
    assert df.count() == 2

    assert (
        df.select(F.sum("n_profiles").alias("n_profiles"))
        .first().n_profiles
    ) == 3


def test_latest_submission_from_client_exists(single_profile_df,
                                              effective_version):
    df = job.transform(single_profile_df, effective_version, WEEK_START_DS)
    assert df.count() == 1


def test_profile_usage_length(single_profile_df, effective_version):
    # there are two pings each with 1 hour of usage
    df = job.transform(single_profile_df, effective_version, WEEK_START_DS)
    rows = df.collect()

    assert rows[0].usage_hours == 2


def test_current_cohort_week_is_zero(single_profile_df, effective_version):
    df = job.transform(single_profile_df, effective_version, WEEK_START_DS)
    rows = df.collect()

    actual = rows[0].current_week
    expect = 0

    assert actual == expect


def test_multiple_cohort_weeks_exist(multi_profile_df, effective_version):
    df = job.transform(multi_profile_df, effective_version, WEEK_START_DS)
    rows = df.select('current_week').collect()

    actual = set([row.current_week for row in rows])
    expect = set([0, 1, 2])

    assert actual == expect


def test_cohort_by_channel_count(multi_profile_df, effective_version):
    df = job.transform(multi_profile_df, effective_version, WEEK_START_DS)
    rows = df.where(df.channel == 'release-cck-mozilla42').collect()

    assert len(rows) == 2


def test_cohort_by_channel_aggregates(multi_profile_df, effective_version):
    df = job.transform(multi_profile_df, effective_version, WEEK_START_DS)
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
    def _test_transform(snippets, week_start=WEEK_START_DS):
        return job.transform(
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

    snippets, test_func = list(zip(*[
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
    ]))

    df = test_transform(list(snippets))

    for test in test_func:
        test(df)


def test_attribution_from_new_profile(effective_version,
                                      generate_main_summary_data,
                                      generate_new_profile_data):
    main_summary = generate_main_summary_data([
        {'client_id': '1', 'attribution': {'source': 'mozilla.org'}},
        {'client_id': '3', 'attribution': None},
        {'client_id': '4', 'attribution': None},
        {
            'client_id': '5',
            'attribution': {'source': 'mozilla.org'},
            'timestamp': SUBSESSION_START.shift(days=1).timestamp * 10 ** 9,
        },
        {
            'client_id': '6',
            'attribution': {'source': 'mozilla.org'},
            'timestamp': SUBSESSION_START.shift(days=1).timestamp * 10 ** 9,
        },
        {'client_id': '7', 'attribution': {'source': 'mozilla.org'}},
    ])

    def update_attribution(attribution):
        # only updates the attribution section in the environment
        env = copy.deepcopy(data.new_profile_sample['environment'])
        env['settings']['attribution'] = attribution
        return env

    new_profile = generate_new_profile_data([
        # new profile without a main summary companion
        {'client_id': '2', 'environment': update_attribution({'source': 'mozilla.org'})},
        # recover null attribution
        {'client_id': '3', 'environment': update_attribution({'source': 'mozilla.org'})},
        # new-profile ping used to recover attribution, but outside of the
        # the current retention period
        {
            'client_id': '4',
            'environment': update_attribution({'source': 'mozilla.org'}),
            'submission': SUBSESSION_START.shift(days=-7).format("YYYYMMDD"),
        },
        # avoid accidentally overwriting an existing value with an empty structure
        {'client_id': '5', 'environment': update_attribution({})},
        # main-pings have higher latency than new-profile pings, so the main
        # ping attribution state will be set correctly
        {'client': '6', 'environment': update_attribution(None)},
        # new-profile timestamp is newer than main-ping, so attribution for the
        # client is unset
        {
            'client_id': '7',
            'environment': update_attribution(None),
            'timestamp': SUBSESSION_START.shift(days=1).timestamp * 10 ** 9,
        },
    ])
    sources = job.extract(main_summary, new_profile, WEEK_START_DS, 2, 0, False)
    df = job.transform(sources, effective_version, WEEK_START_DS)

    assert df.where("source='mozilla.org'").agg(F.sum("n_profiles")).first()[0] == 6


def test_invalid_profile_creation_date(test_transform):
    df = test_transform([
        {
            "client_id": "created when?",
            "profile_creation_date": None
        },
        {
            "client_id": "older than firefox",
            "profile_creation_date": data.format_pcd(data.SUBSESSION_START.shift(years=-50))
        },
        {
            "client_id": "created in the future",
            "profile_creation_date": data.format_pcd(data.SUBSESSION_START.shift(days=+10))
        },
        {"client_id": "sanity"},
    ])

    # null and the single sane row
    assert df.count() == 2

    # attributes like `geo` head to null space if pcd is invalid
    # pings in these tests share a common geo value ("US")
    df.where("geo='US'").agg(F.sum("n_profiles")).first()[0] == 1
    df.where("geo='unknown'").agg(F.sum("n_profiles")).first()[0] == 3
