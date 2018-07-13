"""
Calculate Firefox monthly- and daily-active users.

main_summary is updated daily; re-runs should be idempotent between
updates.
C.f. https://bugzilla.mozilla.org/show_bug.cgi?id=1370522,
     https://bugzilla.mozilla.org/show_bug.cgi?id=1240849
"""
import csv
import datetime as DT
import logging
import os

from pyspark.sql import SparkSession
import boto3
import botocore
import click
from boto3.s3.transfer import S3Transfer
from moztelemetry.standards import (
    filter_date_range,
    count_distinct_clientids
)

from mozetl import utils as U

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEVELOPMENT = False

STORAGE_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis"
STORAGE_SUB_DIR = 'spenrose/maudau' if DEVELOPMENT else 'mreid/maudau'
MAUDAU_ROLLUP_BASENAME = "engagement_ratio.csv"
MAUDAU_SNAPSHOT_TEMPLATE = "engagement_ratio.{}.csv"
DASHBOARD_BUCKET = "net-mozaws-prod-metrics-data"
DASHBOARD_KEY = "firefox-dashboard/{}".format(MAUDAU_ROLLUP_BASENAME)

# MONTH as in "monthly active"
MONTH = 28


def get_rollup_s3_paths(basename):
    return (STORAGE_BUCKET, os.path.join(STORAGE_SUB_DIR, basename))


def count_active_users(frame, end_date, days_back):
    """
    Tally active users according to externally-defined heuristics,
    specifically client-reported subsession_start_date.

    :frame DataFrame(): conforming to main_summary schema
    :end_date DT.date(): the last day on which to count activity.
    :days_back int: how far back to go; 0 for daily and 28 for monthly.

    Note that return values are not stable over time as activity trickles
    in with long submission latencies.

    See https://bugzilla.mozilla.org/show_bug.cgi?id=1240849
    """
    params = U.generate_filter_parameters(end_date, days_back)

    filtered = filter_date_range(
        frame,
        frame.subsession_start_date,
        params['min_activity_iso'],
        params['max_activity_iso'],
        frame.submission_date_s3,
        params['min_submission_string'],
        params['max_submission_string'])
    return count_distinct_clientids(filtered)


def parse_last_rollup(basename, start_date=None):
    """
    Extract the valid old counts from basename and return them with the
    first date that needs to be re-counted.
    """
    start_date = start_date or DT.date.today()
    since = start_date - U.ACTIVITY_SUBMISSION_LAG
    carryover = []
    with open(basename) as f:
        reader = csv.DictReader(f)
        last_day = None
        for row in reader:
            day = U.parse_as_submission_date(row['day'])
            if day >= since:
                break
            if last_day is not None:
                if (day - last_day) > DT.timedelta(1):
                    since = last_day + DT.timedelta(1)
                    break
            last_day = day
            carryover.append(row)
    return since, carryover


def get_last_rollup(transferer):
    '''
    Always chose the latest rollup, even if start_date is old.
    '''
    basename = MAUDAU_ROLLUP_BASENAME
    key = os.path.join(STORAGE_SUB_DIR, basename)
    try:
        transferer.download_file(
            STORAGE_BUCKET,
            key,
            basename)
    except botocore.exceptions.ClientError as e:
        # If the file wasn't there, that's ok. Otherwise, abort!
        if e.response['Error']['Code'] != "404":
            raise e
        else:
            msg = "Did not find an existing file at '{}'".format(basename)
            logger.info(msg)
        return None
    return basename


def generate_counts(frame, since, until=None):
    '''
    A thin wrapper around moztelemetry counting functions.

    :frame main_summary or an isomorphic subset
    :since DT.date()
    :until DT.date() (unit test convenience)
    '''
    cols = [frame.client_id.alias('clientId'),
            'subsession_start_date', 'submission_date_s3']
    narrow = frame.select(cols)
    updates = []
    today = DT.date.today()
    generated = U.format_as_submission_date(today)
    start = since
    until = until or today
    while start < until:
        dau = count_active_users(narrow, start, 0)
        mau = count_active_users(narrow, start, MONTH)
        day = U.format_as_submission_date(start)
        d = {'day': day, 'dau': dau, 'mau': mau, 'generated_on': generated}
        updates.append(d)
        start += DT.timedelta(1)
    return updates


def write_locally(results):
    '''
    :results [{'day': '%Y%m%d', 'dau': <int>,
              'mau': <int>, 'generated_on': '%Y%m%d'}, ...]
    '''
    publication_date = U.format_as_submission_date(DT.date.today())
    basename = MAUDAU_SNAPSHOT_TEMPLATE.format(publication_date)
    cols = ["day", "dau", "mau", "generated_on"]
    with open(basename, 'w') as f:
        writer = csv.DictWriter(f, cols)
        writer.writeheader()
        writer.writerows(results)
    return basename


def publish_to_s3(s3client, bucket, prefix, basename):
    '''
    Write the file twice to storage (once dated, once as "latest")
    and once to the production dashboard.
    '''
    dated_key = os.path.join(prefix, basename)
    U.upload_file_to_s3(s3client, basename, bucket, dated_key)
    latest_key = os.path.join(prefix, MAUDAU_ROLLUP_BASENAME)
    U.upload_file_to_s3(s3client, basename, bucket, latest_key)
    if DEVELOPMENT:
        return
    U.upload_file_to_s3(s3client, basename, DASHBOARD_BUCKET, DASHBOARD_KEY)


@click.command()
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
@click.option('--output_bucket',
              default=STORAGE_BUCKET,
              help='Bucket of the output dataset')
@click.option('--output_prefix',
              default=STORAGE_SUB_DIR,
              help='Prefix of the output dataset')
def main(input_bucket, input_prefix, output_bucket, output_prefix):
    """Calculate Firefox monthly- and daily-active users."""

    s3client = boto3.client('s3', 'us-west-2')
    transferer = S3Transfer(s3client)
    last_rollup_basename = get_last_rollup(transferer)
    if last_rollup_basename:
        since, carryover = parse_last_rollup(last_rollup_basename)
        logging.info("Generating counts since {}".format(since))
    else:
        since, carryover = None, []
        logging.info("Generating counts since beginning")
    spark = (
        SparkSession
        .builder
        .appName("maudau")
        .getOrCreate()
    )
    path = U.format_spark_path(input_bucket, input_prefix)
    logging.info("Loading main_summary from {}".format(path))
    main_summary = spark.read.option("mergeSchema", "true").parquet(path)
    updates = generate_counts(main_summary, since)
    logging.info("Generated counts for {} days".format(len(updates)))
    results = carryover + updates
    output_basename = write_locally(results)
    publish_to_s3(s3client, output_bucket, output_prefix, output_basename)
    if not DEVELOPMENT:
        logging.info("Published to S3; done.")
