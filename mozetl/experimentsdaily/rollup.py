import click
from pyspark.sql import SparkSession
from mozetl.clientsdaily.rollup import extract_search_counts
from mozetl.utils import extract_submission_window_for_activity_day
from mozetl.utils import format_spark_path

EXCLUDED_ID = 'pref-flip-screenshots-release-1369150'


def load_experiments_summary(spark, parquet_path):
    return (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(parquet_path)
        .where("experiment_id != '{}'".format(EXCLUDED_ID))
    )


def to_experiment_profile_day_aggregates(frame_with_extracts):
    from mozetl.clientsdaily.fields import EXPERIMENT_FIELD_AGGREGATORS
    from mozetl.clientsdaily.fields import ACTIVITY_DATE_COLUMN
    if "activity_date" not in frame_with_extracts.columns:
        with_activity_date = frame_with_extracts.select(
            "*", ACTIVITY_DATE_COLUMN
        )
    else:
        with_activity_date = frame_with_extracts
    grouped = with_activity_date.groupby(
        'experiment_id', 'client_id', 'activity_date')
    return grouped.agg(*EXPERIMENT_FIELD_AGGREGATORS)


@click.command()
@click.option('--date',
              default=None,
              help='Start date to run on')
@click.option('--input-bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input-prefix',
              default='experiments/v1/',
              help='Prefix of the input dataset')
@click.option('--output-bucket',
              default='net-mozaws-prod-us-west-2-pipeline-analysis',
              help='Bucket of the output dataset')
@click.option('--output-prefix',
              default='/experiments_daily',
              help='Prefix of the output dataset')
@click.option('--output-version',
              default=3,
              help='Version of the output dataset')
@click.option('--lag-days',
              default=10,
              help='Number of days to allow for submission latency')
def main(date, input_bucket, input_prefix, output_bucket,
         output_prefix, output_version, lag_days):
    """
    Aggregate by (client_id, experiment_id, day).

    Note that the target day will actually be `lag-days` days before
    the supplied date. In other words, if you pass in 2017-01-20 and
    set `lag-days` to 5, the aggregation will be processed for
    day 2017-01-15 (the resulting data will cover submission dates
    including the activity day itself plus 5 days of lag for a total
    of 6 days).
    """
    spark = SparkSession.builder.appName("experiments_daily").getOrCreate()
    parquet_path = format_spark_path(input_bucket, input_prefix)
    frame = load_experiments_summary(spark, parquet_path)
    day_frame, start_date = extract_submission_window_for_activity_day(frame, date, lag_days)
    searches_frame = extract_search_counts(frame)
    results = to_experiment_profile_day_aggregates(searches_frame)
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )  # Don't write _SUCCESS files, which interfere w/ReDash discovery
    output_base_path = "{}/v{}/activity_date_s3={}".format(
        format_spark_path(output_bucket, output_prefix),
        output_version,
        start_date.strftime('%Y-%m-%d'))
    results.write.mode("overwrite").parquet(output_base_path)


if __name__ == '__main__':
    main()
