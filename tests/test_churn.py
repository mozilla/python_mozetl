import json
import os
from datetime import timedelta, datetime

from click.testing import CliRunner
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructField, StructType, StringType,
    LongType, IntegerType, BooleanType
)
from mozetl.churn import churn


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
    StructField("total_uri_count",       IntegerType(), True),
    StructField("unique_domains_count", IntegerType(), True)])

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


def generate_samples(snippets=None, base_sample=default_sample):
    """ Generate samples from the default sample. Snippets overwrite specific
    fields in the default sample.

    :snippets list(dict): A list of dictionary attributes to update
    """
    if snippets is None:
        return [json.dumps(base_sample)]

    samples = []
    for snippet in snippets:
        sample = base_sample.copy()
        sample.update(snippet)
        samples.append(json.dumps(sample))
    return samples


def samples_to_df(spark, samples):
    """ Turn a list of dictionaries into a dataframe. """
    jsonRDD = spark.sparkContext.parallelize(samples)
    return spark.read.json(jsonRDD, schema=main_schema)


def snippets_to_df(spark, snippets, base_sample=default_sample):
    """ Turn a list of snippets into a fully instantiated dataframe.

    A snippet are attributes that overwrite the base dictionary. This allows
    for the generation of new rows without excess repetition.
    """
    samples = generate_samples(snippets, base_sample)
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
    return snippets_to_df(spark, snippets)


@pytest.fixture
def release_info():
    info = {
        "major": {
            "52.0": "2017-03-07"
        },
        "minor": {
            "51.0.1": "2017-01-26",
            "52.0.1": "2017-03-17",
            "52.0.2": "2017-03-29"
        }
    }

    return info


@pytest.fixture(autouse=True)
def no_get_release_info(release_info, monkeypatch):
    """ Disable get_release_info because of requests change over time. """

    def mock_get_release_info():
        return release_info
    monkeypatch.setattr(churn, 'get_release_info', mock_get_release_info)

    # disable fetch_json to cover all the bases
    monkeypatch.delattr(churn, 'fetch_json')


def test_date_to_version_range(release_info):
    result = churn.make_d2v(release_info)

    start = datetime.strptime(
        release_info['minor']['51.0.1'], '%Y-%m-%d')
    end = datetime.strptime(
        release_info['minor']['52.0.2'], '%Y-%m-%d')

    assert len(result) == (end - start).days + 1
    assert result['2017-03-08'] == '52.0'
    assert result['2017-03-17'] == '52.0.1'


# testing d2v conflicting stability releases on the same day
# testing effective version
# testing effective release-channel version


def test_ignored_submissions_outside_of_period(late_submissions_df):
    df = churn.compute_churn_week(late_submissions_df, week_start_ds)
    assert df.count() == 0


def test_latest_submission_from_client_exists(single_profile_df):
    df = churn.compute_churn_week(single_profile_df, week_start_ds)
    assert df.count() == 1


def test_profile_usage_length(single_profile_df):
    # there are two pings each with 1 hour of usage
    df = churn.compute_churn_week(single_profile_df, week_start_ds)
    rows = df.collect()

    assert rows[0].usage_hours == 2


def test_current_cohort_week_is_zero(single_profile_df):
    df = churn.compute_churn_week(single_profile_df, week_start_ds)
    rows = df.collect()

    actual = rows[0].current_week
    expect = 0

    assert actual == expect


def test_multiple_cohort_weeks_exist(multi_profile_df):
    df = churn.compute_churn_week(multi_profile_df, week_start_ds)
    rows = df.select('current_week').collect()

    actual = set([row.current_week for row in rows])
    expect = set([0, 1, 2])

    assert actual == expect


def test_cohort_by_channel_count(multi_profile_df):
    df = churn.compute_churn_week(multi_profile_df, week_start_ds)
    rows = df.where(df.channel == 'release-cck-mozilla42').collect()

    assert len(rows) == 2


def test_cohort_by_channel_aggregates(multi_profile_df):
    df = churn.compute_churn_week(multi_profile_df, week_start_ds)
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
def nulled_attribution_df(spark):
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
    return snippets_to_df(spark, snippets)


def test_nulled_stub_attribution_content(nulled_attribution_df):
    df = churn.compute_churn_week(nulled_attribution_df, week_start_ds)
    rows = (
        df
        .select('content')
        .distinct()
        .collect()
    )
    actual = set([r.content for r in rows])
    expect = set(['content', 'unknown'])

    assert actual == expect


def test_nulled_stub_attribution_medium(nulled_attribution_df):
    df = churn.compute_churn_week(nulled_attribution_df, week_start_ds)
    rows = (
        df
        .select('medium')
        .distinct()
        .collect()
    )
    actual = set([r.medium for r in rows])
    expect = set(['unknown'])

    assert actual == expect


def test_simple_string_dimensions(spark):
    input_df = snippets_to_df(spark, [
        {
            'distribution_id': None,
            'default_search_engine': None,
            'locale': None
        }
    ])
    df = churn.compute_churn_week(input_df, week_start_ds)
    rows = df.collect()

    assert rows[0].distribution_id == 'unknown'
    assert rows[0].default_search_engine == 'unknown'
    assert rows[0].locale == 'unknown'


def test_empty_total_uri_count(spark):
    input_df = snippets_to_df(spark, [{'total_uri_count': None}])
    df = churn.compute_churn_week(input_df, week_start_ds)
    rows = df.collect()

    assert rows[0].total_uri_count == 0


def test_total_uri_count_per_client(spark):
    snippets = [{'total_uri_count': 1}, {'total_uri_count': 2}]
    input_df = snippets_to_df(spark, snippets)
    df = churn.compute_churn_week(input_df, week_start_ds)
    rows = df.collect()

    assert rows[0].total_uri_count == 3


def test_average_unique_domains_count(spark):
    snippets = [
        # averages to 4
        {'client_id': '1', 'unique_domains_count': 6},
        {'client_id': '1', 'unique_domains_count': 2},
        # averages to 8
        {'client_id': '2', 'unique_domains_count': 12},
        {'client_id': '2', 'unique_domains_count': 4}
    ]
    input_df = snippets_to_df(spark, snippets)
    df = churn.compute_churn_week(input_df, week_start_ds)
    rows = df.collect()

    # (4 + 8) / 2 == 6
    assert rows[0].unique_domains_count_per_profile == 6


def test_adjust_start_date_pins_to_sunday_no_lag():
    # A wednesday
    args = ['20170118', False]
    actual = churn.adjust_start_date(*args)
    expected = '20170115'

    assert actual == expected


def test_adjust_start_date_accounts_for_lag():
    # Going back 10 days happens to be Sunday
    args = ['20170118', True]
    actual = churn.adjust_start_date(*args)
    expected = '20170108'

    assert actual == expected


def test_process_backfill():
    actual = []
    expected = ['20170101', '20170108', '20170115', '20170122']

    def callback(date):
        actual.append(date)

    args = ['20170101', '20170125', callback]
    churn.process_backfill(*args)

    assert actual == expected


def test_cli(spark, multi_profile_df, tmpdir, monkeypatch):

    # use the file:// prefix
    def mock_format_spark_path(bucket, prefix):
        return 'file://{}/{}'.format(bucket, prefix)
    monkeypatch.setattr(churn, 'format_spark_path', mock_format_spark_path)

    # write `main_summary` to disk
    bucket = str(tmpdir)
    input_prefix = 'main_summary/v3'
    output_prefix = 'churn/v2'

    path = churn.format_spark_path(bucket, input_prefix)

    multi_profile_df.write.partitionBy('submission_date_s3').parquet(path)

    runner = CliRunner()
    args = [
        week_start_ds,
        bucket,
        '--input-bucket', bucket,
        '--input-prefix', input_prefix,
        '--no-lag',  # week_start_ds already accounts for the lag time
    ]
    result = runner.invoke(churn.main, args)
    assert result.exit_code == 0

    # check that the files are correctly partitioned
    folder = os.path.join(bucket, output_prefix)
    assert any(item.startswith('week_start') for item in os.listdir(folder))

    # check that there is only a single partition
    folder = os.path.join(folder, 'week_start={}'.format(week_start_ds))
    assert len([item for item in os.listdir(folder)
                if item.endswith('.parquet')]) == 1

    # check that the dataset conforms to expected output
    path = churn.format_spark_path(bucket, output_prefix)
    df = spark.read.parquet(path)
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
