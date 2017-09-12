import datetime as DT
from pyspark.sql import SparkSession, functions as F
import click
from mozetl.clientsdaily.rollup import extract_search_counts
from mozetl.clientsdaily.fields import EXPERIMENT_FIELD_AGGREGATORS
from mozetl.utils import format_spark_path

LAG = 10
ACTIVITY_DATE_COLUMN = F.expr(
    "substr(subsession_start_date, 1, {})".format(LAG)
).alias("activity_date")
EXPERIMENTS_SUMMARY_PATH = 's3://telemetry-parquet/experiments/v1/'
EXCLUDED_ID = 'pref-flip-screenshots-release-1369150'
ACTIVITY_SUBMISSION_LAG = DT.timedelta(LAG)
WRITE_VERSION = '2'
STORAGE_BUCKET = 'net-mozaws-prod-us-west-2-pipeline-analysis'
STORAGE_PREFIX = '/spenrose/experiments-daily/v{}/'.format(WRITE_VERSION)


def load_experiments_summary(spark, parquet_path):
    return (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(parquet_path)
        .where("experiment_id != '{}'".format(EXCLUDED_ID))
    )


def to_experiment_profile_day_aggregates(frame_with_extracts):
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
@click.argument('--date')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='experiments/v1/',
              help='Prefix of the input dataset')
@click.option('--output_bucket',
              default=STORAGE_BUCKET,
              help='Bucket of the output dataset')
@click.option('--output_prefix',
              default=STORAGE_PREFIX,
              help='Prefix of the output dataset')
def main(date, input_bucket, input_prefix, output_bucket, output_prefix):
    spark = (SparkSession
             .builder
             .appName("engagement_modeling")
             .getOrCreate())
    parquet_path = format_spark_path(input_bucket, input_prefix)
    frame = load_experiments_summary(spark, parquet_path)
    searches_frame = extract_search_counts(frame)
    results = to_experiment_profile_day_aggregates(searches_frame)
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )  # Don't write _SUCCESS files, which interfere w/ReDash discovery
    output_path = format_spark_path(output_bucket, output_prefix)
    results.write.mode("overwrite").parquet(output_path)


if __name__ == '__main__':
    main()
