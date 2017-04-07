import json
from datetime import timedelta, datetime

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
    "subsession_length":     1000,
    "subsession_start_date": "2017-01-15",
    "sync_configured":       False,
    "sync_count_desktop":    1,
    "sync_count_mobile":     1,
    "timestamp":             1491244610603260700,  # microseconds
    "total_uri_count":       20,
    "unique_domains_count":  3
}


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
    microseconds_in_second = 10**6

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
            seconds_since_epoch(submission_date) * microseconds_in_second
        )
    }

    return date_snippet


def generate_samples(snippets=None):
    """ Generate samples from the default sample. Snippets overwrite specific
    fields in the default sample.

    :snippets list(dict): A list of dictionary attributes to update
    """
    if snippets is None:
        return [json.dumps(default_sample)]

    samples = []
    for snippet in snippets:
        sample = default_sample.copy()
        sample.update(snippet)
        samples.append(json.dumps(sample))
    return samples


def samples_to_df(spark, samples):
    jsonRDD = spark.sparkContext.parallelize(samples)
    return spark.read.json(jsonRDD)


def snippets_to_df(spark, snippets):
    samples = generate_samples(snippets)
    return samples_to_df(spark, samples)


# Generate the datasets
# Sunday, also the first day in this collection period.
subsession_start = datetime(2017, 1, 15)
week_start_ds = datetime.strftime(subsession_start, "%Y%m%d")


@pytest.fixture
def late_submissions_df(spark):
    # All pings within 17 days of the submission start date are valid.
    # However, only pings with ssd within the 7 day retention period
    # are used for computation. Generate pings for this case.

    late_submission = generate_dates(subsession_start, submission_offset=18)
    early_subsession = generate_dates(subsession_start - timedelta(7))

    snippets = [late_submission, early_subsession]
    return snippets_to_df(spark, snippets)


@pytest.fixture
def single_profile_df(spark):
    recent_ping = generate_dates(
        subsession_start + timedelta(3), creation_offset=3)

    # create a duplicate ping for this user, earlier than the previous
    old_ping = generate_dates(subsession_start)

    snippets = [recent_ping, old_ping]
    return snippets_to_df(spark, snippets)


@pytest.fixture
def multi_profile_df(spark):

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
        "country": "US",
        "channel": "release",
        "subsession_length": seconds_in_hour * 2
    })

    user_1 = cohort_1.copy()
    user_1.update({
        "country": "US",
        "channel": "release",
        "subsession_length": seconds_in_hour * 2
    })

    user_2 = cohort_2.copy()
    user_2.update({
        "country": "CA",
        "channel": "release",
        "subsession_length": seconds_in_hour
    })

    snippets = [user_0, user_1, user_2]
    return snippets_to_df(spark, snippets)


def test_ignored_submissions(late_submissions_df):
    df = churn.compute_churn_week(late_submissions_df, week_start_ds)
    assert df.count() == 0


def test_latest_submission(single_profile_df):
    df = churn.compute_churn_week(single_profile_df, week_start_ds)
    assert df.count() == 1


def test_current_acqusition_week(single_profile_df):
    df = churn.compute_churn_week(single_profile_df, week_start_ds)
    rows = df.collect()

    actual = rows[0].current_week
    expect = 0

    assert actual == expect


def test_multiple_cohort_weeks(multi_profile_df):
    df = churn.compute_churn_week(multi_profile_df, week_start_ds)
    rows = df.select('current_week').collect()

    actual = set([row.current_week for row in rows])
    expect = set([0, 1, 2])

    assert actual == expect
