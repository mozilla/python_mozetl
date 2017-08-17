"""
Bug 1349065, 1374831 - Sync Bookmark Validation Dataset

This notebook is adapted from a gist that transforms the `sync_summary` into
a flat table to avoid straining the resources on the Presto cluster.[1]

[1] https://gist.github.com/kitcambridge/364f56182f3e96fb3131bf38ff648609
"""

import logging

import arrow
import click
from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract(spark, path, start_date):
    """Register a temporary `sync_summary` view on the start date."""

    (
        spark.read.parquet(path)
        .where(F.col("submission_date_s3") == start_date)
        .createOrReplaceTempView("sync_summary")
    )


def transform(spark):
    """Create the bookmark problem and summary tables."""

    query = """
    SELECT s.app_build_id, s.app_version, s.app_display_version, s.app_name,
       s.app_channel, s.uid, s.deviceID as device_id,
       s.submission_date_s3 AS submission_day,
       date_format(from_unixtime(s.when / 1000), 'YYYYMMdd') AS sync_day,
       s.when,
       s.status,
       e.name AS engine_name,
       e.status AS engine_status,
       e.failureReason AS engine_failure_reason,
       e.validation.problems IS NOT NULL AS engine_has_problems,
       e.validation.version AS engine_validation_version,
       e.validation.checked AS engine_validation_checked,
       e.validation.took AS engine_validation_took,
       p.name AS engine_validation_problem_name,
       p.count AS engine_validation_problem_count
    FROM sync_summary s
    LATERAL VIEW explode(s.engines) AS e
    LATERAL VIEW OUTER explode(e.validation.problems) AS p
    WHERE s.failureReason IS NULL
    """
    engine_validation_results = spark.sql(query)

    bmk_validation_results = (
        engine_validation_results
        .filter(engine_validation_results["engine_name"] == "bookmarks")
    )

    # Bookmark validations with problems.
    bmk_validation_problems = (
        bmk_validation_results
        .filter(bmk_validation_results["engine_has_problems"])
    )

    # All bookmark validations, including without problems.
    bmk_total_per_day = (
        bmk_validation_results
        .filter(bmk_validation_results["engine_validation_checked"].isNotNull())
        .groupBy("sync_day")
        .agg(
            F.countDistinct("uid", "device_id", "when").alias("total_bookmark_validations"),
            F.countDistinct("uid").alias("total_validated_users"),
            F.sum("engine_validation_checked").alias("total_bookmarks_checked")
        )
    )

    bmk_validation_problems.createOrReplaceTempView("bmk_validation_problems")
    bmk_total_per_day.createOrReplaceTempView("bmk_total_per_day")


def load(spark, bucket, prefix, version, start_date):
    """Save tables to disk."""

    for table_name in ["bmk_validation_problems", "bmk_total_per_day"]:
        path = (
            "s3://{}/{}/{}/v{}/start_date={}"
            .format(bucket, prefix, table_name, version, start_date)
        )

        logger.info("Saving table {} on start_date {} to {}"
                    .format(table_name, start_date, path))

        df = spark.sql("SELECT * FROM {}".format(table_name))
        df.repartition(1).write.parquet(path, mode="overwrite")


@click.command()
@click.option('--start_date', required=True, help="Date to process")
@click.option('--end_date', help="Optional end date to run until")
@click.option('--bucket', default='net-mozaws-prod-us-west-2-pipeline-analysis')
@click.option('--prefix', default='kit/sync')
@click.option('--input_bucket', default='telemetry-parquet')
@click.option('--input_prefix', default='sync_summary/v1')
def main(start_date, end_date, bucket, prefix, input_bucket, input_prefix):
    spark = (SparkSession
             .builder
             .appName("sync_bookmark")
             .getOrCreate())

    # configure the spark session to partition the data reasonably
    parquet_size = 512 * 1024 * 1024
    spark.conf.set("parquet.block.size", parquet_size)
    spark.conf.set("dfs.blocksize", parquet_size)

    version = 1
    input_path = "s3://{}/{}".format(input_bucket, input_prefix)

    # use the airflow date convention
    ds_format = "YYYYMMDD"
    start = arrow.get(start_date, ds_format)
    end = arrow.get(end_date if end_date else start_date, ds_format)

    for date in arrow.Arrow.range('day', start, end):
        current_date = date.format(ds_format)
        logger.info("Processing sync bookmark validation for {}"
                    .format(current_date))
        extract(spark, input_path, current_date)
        transform(spark)
        load(spark, bucket, prefix, version, current_date)

    spark.stop()
