import operator

import arrow
import click
from pyspark.sql import SparkSession, functions as F

from mozetl.churn import churn, utils
from mozetl.churn.churn import (
    SECONDS_PER_DAY, TOP_COUNTRIES, DEFAULT_DATE, SECONDS_PER_HOUR, MAX_SUBSESSION_LENGTH
)


def transform(main_summary, start_ds):

    client_date = utils.to_datetime('subsession_start_date', "yyyy-MM-dd")

    pcd = F.from_unixtime(F.col("profile_creation_date") * SECONDS_PER_DAY)
    pcd_format =  F.date_format(pcd, 'yyyy-MM-dd')
    is_valid_pcd = [
        pcd_format.isNotNull(),
        (pcd_format > DEFAULT_DATE),
        (pcd <= client_date),
    ]
    valid_pcd = (
        F.when(reduce(operator.__and__, is_valid_pcd), pcd)
        .otherwise(F.lit(None))
    )
    days_since_creation = F.datediff(client_date, valid_pcd)

    device_count = (
        F.coalesce(F.col("sync_count_desktop"), F.lit(0)) +
        F.coalesce(F.col("sync_count_mobile"), F.lit(0))
    )
    # Bug 1289573: Support values like "mozilla86" and "mozilla86-utility-existing"
    is_funnelcake = F.col('distribution_id').rlike("^mozilla[0-9]+.*$")

    subsession_length = (
        F.when(F.col('subsession_length') > MAX_SUBSESSION_LENGTH, MAX_SUBSESSION_LENGTH)
        .otherwise(F.when(F.col('subsession_length') < 0, 0)
                   .otherwise(F.col('subsession_length')))
    )
    usage_hours = subsession_length / SECONDS_PER_HOUR

    mapping = {
        # attributes
        'client_id': None,
        'subsession_start': client_date,
        'profile_creation': F.date_format(valid_pcd, "yyyy-MM-dd"),
        'days_since_creation': (
            F.when(days_since_creation < 0, F.lit(-1))  # -1 is the value for bad data
            .otherwise(days_since_creation)
            .cast("long")),
        'channel': 'normalized_channel',
        'app_version': None,
        'geo': (
            F.when(F.col("country").isin(TOP_COUNTRIES), F.col("country"))
            .otherwise(F.lit("ROW"))),
        'distribution_id': None,
        'is_funnelcake': F.when(is_funnelcake, F.lit("yes")).otherwise(F.lit("no")),
        'source': 'attribution.source',
        'medium': 'attribution.medium',
        'campaign': 'attribution.campaign',
        'content': 'attribution.content',
        'sync_usage': (
            F.when(device_count > 1, F.lit("multiple"))
            .otherwise(
                F.when((device_count == 1) | F.col("sync_configured"), F.lit("single"))
                .otherwise(
                    F.when(F.col("sync_configured").isNotNull(), F.lit("no"))
                    .otherwise(F.lit(None))))),
        'is_active': (
            F.when(client_date < utils.to_datetime(F.lit(start_ds)), F.lit("no"))
            .otherwise(F.lit("yes"))),
        # metrics
        'usage_hours': usage_hours,
        'sum_squared_usage_hours': usage_hours ** 2,
        'total_uri_count': 'scalar_parent_browser_engagement_total_uri_count',
        'unique_domains_count': 'scalar_parent_browser_engagement_unique_domains_count',
    }
    expr = utils.build_col_expr(mapping)

    cleaned_data = (
        main_summary
        .select(expr)
        .fillna({
            'profile_creation': DEFAULT_DATE,
            'is_funnelcake': "no",
            "days_since_creation": -1,
        })
        .fillna(0)
        .fillna(0.0)
        .fillna('unknown')
    )

    return cleaned_data


def save(cleaned, path):
    (
        cleaned
        .write
        .parquet(path, mode="overwrite")
    )


@click.command()
@click.option('--start_date', required=True)
@click.option('--path', default='retention_intermediate',
              help="Path on hdfs to store intermediate data")
@click.option('--input-bucket', default='telemetry-parquet',
              help="input bucket containing main_summary")
@click.option('--input-prefix', default='main_summary/v4',
              help="input prefix containing main_summary")
@click.option('--period', default=1,
              help="length of the retention period in days")
@click.option('--slack', default=2,
              help="number of days to account for submission latency")
@click.option('--sample/--no-sample', default=False)
def main(start_date, path, input_bucket, input_prefix,
         period, slack, sample):
    """Compute churn / retention information for unique segments of
    Firefox users acquired during a specific period of time.
    """
    spark = (
        SparkSession
        .builder
        .appName("retention")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # If this job is scheduled, we need the input date to lag a total of
    # 10 days of slack for incoming data. Airflow subtracts 7 days to
    # account for the weekly nature of this report.
    start_ds = utils.format_date(
        arrow.get(start_date, utils.DS_NODASH),
        utils.DS_NODASH,
        -slack
    )

    main_summary = (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(utils.format_spark_path(input_bucket, input_prefix))
    )

    new_profile = (
        spark
        .read
        .parquet(
            "s3://net-mozaws-prod-us-west-2-pipeline-data/"
            "telemetry-new-profile-parquet/v1/"
        )
    )

    extracted = churn.extract(main_summary, new_profile, start_ds, period, slack, sample)

    retention = transform(extracted, start_ds)
    save(retention, path)


if __name__ == '__main__':
    main()
