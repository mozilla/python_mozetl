import datetime as DT
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import click
from moztelemetry.standards import filter_date_range
from mozetl.utils import (
    format_spark_path,
    generate_filter_parameters
)
from fields import MAIN_SUMMARY_FIELD_AGGREGATORS

ACTIVITY_SUBMISSION_LAG = DT.timedelta(10)
MAIN_SUMMARY_VERSION = 4
MAIN_SUMMARY_PATH = "s3://telemetry-parquet/main_summary/v{}".format(
    MAIN_SUMMARY_VERSION)
WRITE_VERSION = '3'
STORAGE_BUCKET = 'net-mozaws-prod-us-west-2-pipeline-analysis'
STORAGE_PREFIX = '/spenrose/clients-daily/v{}/'.format(WRITE_VERSION)


def load_main_summary(spark):
    return (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(MAIN_SUMMARY_PATH)
    )


def extract_search_counts(frame):
    """
    The result should have exactly as many rows as the input.
    """
    two_columns = frame.select(F.col("document_id").alias("did"), "search_counts")
    exploded = two_columns.select(
        "did", F.explode("search_counts").alias("search_struct"))
    unpacked = exploded.select(
        "did",
        F.expr("search_struct.count").alias("search_count_atom")
    )
    grouped = unpacked.groupBy("did").agg({"search_count_atom": "sum"})
    extracted = grouped.select(
        "did", F.col("sum(search_count_atom)").alias("search_count")
    )
    nulls = two_columns.select(
        "did").where(
        "search_counts is NULL").select(
        "did", F.lit(0).alias("search_count")
    )
    intermediate = extracted.unionAll(nulls)
    result = frame.join(intermediate, frame.document_id == intermediate.did)
    return result


def extract_month(first_day, frame):
    """
    Pull a month's worth of activity out of frame according to the
    heuristics implemented in moztelemetry.standards.

    :first_day DT.date(Y, m, 1)
    :frame DataFrame homologous with main_summary
    """
    month = first_day.month
    day_pointer = first_day
    while day_pointer.month == month:
        day_pointer += DT.timedelta(1)
    last_day = day_pointer - DT.timedelta(1)
    days_back = (last_day - first_day).days
    params = generate_filter_parameters(last_day, days_back)
    filtered = filter_date_range(
        frame,
        frame.subsession_start_date,
        params['min_activity_iso'],
        params['max_activity_iso'],
        frame.submission_date_s3,
        params['min_submission_string'],
        params['max_submission_string'])
    return filtered


def to_profile_day_aggregates(frame_with_extracts):
    with_activity_date = frame_with_extracts.select(
        "*", F.expr("substr(subsession_start_date, 1, 10)").alias("activity_date")
    )
    grouped = with_activity_date.groupby('client_id', 'activity_date')
    return grouped.agg(*MAIN_SUMMARY_FIELD_AGGREGATORS)


def write_by_activity_day(
        results, day_pointer, output_bucket,
        output_prefix, partition_count):
    month = day_pointer.month
    prefix_template = os.path.join(output_prefix, 'activity_date_s3={}')
    while day_pointer.month == month:
        isoday = day_pointer.isoformat()
        prefix = prefix_template.format(isoday)
        output_path = format_spark_path(output_bucket, prefix)
        data_for_date = results.where(results.activity_date == isoday)
        data_for_date.coalesce(partition_count).write.parquet(output_path)
        day_pointer += DT.timedelta(1)


def get_partition_count_for_writing(is_sampled):
    '''
    Return a reasonable partition count.

    using_sample_id: boolean
    One day is O(140MB) if filtering down to a single sample_id, but
    O14GB) if not. Google reports 256MB < partition size < 1GB as ideal.
    '''
    if is_sampled:
        return 1
    return 25


@click.command()
@click.argument('--date')
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
              default=STORAGE_PREFIX,
              help='Prefix of the output dataset')
@click.option('--sample_id',
              default=None,
              help='Sample_id to restrict results to')
def main(date, input_bucket, input_prefix, output_bucket,
         output_prefix, sample_id):
    spark = (SparkSession
             .builder
             .appName("engagement_modeling")
             .getOrCreate())
    date = DT.datetime.strptime(date, '%Y-%m-%d').date()
    date = DT.date(date.year, date.month, 1)
    main_summary = load_main_summary(spark)
    month_frame = extract_month(date, main_summary)
    if sample_id:
        clause = "sample_id='{}'".format(sample_id)
        month_frame = month_frame.where(clause)
    with_searches = extract_search_counts(month_frame)
    results = to_profile_day_aggregates(with_searches)
    partition_count = get_partition_count_for_writing(bool(sample_id))
    # Per https://issues.apache.org/jira/browse/PARQUET-142 ,
    # don't write _SUCCESS files, which interfere w/ReDash discovery
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    write_by_activity_day(
        results, date, output_bucket, output_prefix, partition_count
    )


if __name__ == '__main__':
    main()
