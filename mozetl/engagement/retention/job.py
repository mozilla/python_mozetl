"""
# 1 Day Retention

This job generates the intermediate dataset used to generate the 1 day retention
dataset, as outlined in [this proposal][1]. The primary use-case to provide a
descriptive view of engagement over sub-populations of users over various
attributes (e.g. channel, version, acquisition attribution) and time. This
dataset is spun off of the churn dataset, retaining many of the same dimensions.
This job takes advantage of the new-profile ping that was [added to the
`churn_v3` job][2]. This data source will become irrelevant if the shutdown
pingsender is enabled for first sessions.

This job generally runs with a [2 day latency to account for 95% of pings][3] to
arrive along the data pipeline using the shutdown ping-sender. This means the
data is generally only valid for Firefox 55+. If you would like to perform
engagement analysis on pre-55 data, please refer to `mozetl.engagement.churn`
for data latency and data sources that would be advantageous.

This intermediate dataset is aggregated by a job in
`mozilla/telemetry-batch-view`, which takes advantage of the HyperLogLog
data structure to count over sets of clients. In addition, a small set of
engagement metrics have been included for describing cohorts in moore detail.

See the [`Firefox Telemetry Retention: Dataset Example Usage` Dashboard][4] for
details on using this dataset.


[1]: https://bugzilla.mozilla.org/show_bug.cgi?id=1381840
[2]: https://bugzilla.mozilla.org/show_bug.cgi?id=1389231
[3]: https://sql.telemetry.mozilla.org/dashboard/telemetry-health
[4]: https://sql.telemetry.mozilla.org/dashboard/firefox-telemetry-retention-dataset-example-usage
"""
import operator

import arrow
import click
from pyspark.sql import SparkSession, functions as F

from mozetl.engagement.churn import job as churn_job
from mozetl.engagement.churn import utils
from mozetl.engagement.churn.job import (
    SECONDS_PER_DAY, DEFAULT_DATE, SECONDS_PER_HOUR,
    MAX_SUBSESSION_LENGTH
)
from .schema import retention_schema
from functools import reduce


def valid_pcd(pcd, client_date):
    """Determine if the profile creation date column is valid given the date
    reported by the client."""
    pcd_format = F.date_format(pcd, 'yyyy-MM-dd')
    is_valid_pcd = [
        pcd_format.isNotNull(),
        (pcd_format > DEFAULT_DATE),
        (pcd <= client_date),
    ]
    return (
        F.when(reduce(operator.__and__, is_valid_pcd), pcd)
        .otherwise(F.lit(None))
    )


def transform(main_summary, start_ds):
    """Process a subset of `main_summary` to be used in aggregates."""
    client_date = utils.to_datetime('subsession_start_date', 'yyyy-MM-dd')

    pcd = valid_pcd(
        F.from_unixtime(F.col('profile_creation_date') * SECONDS_PER_DAY),
        client_date
    )

    days_since_creation = F.datediff(client_date, pcd)

    # Bug 1289573: Support values like 'mozilla86' and 'mozilla86-utility-existing'
    is_funnelcake = F.col('distribution_id').rlike('^mozilla[0-9]+.*$')

    subsession_length = (
        F.when(F.col('subsession_length') > MAX_SUBSESSION_LENGTH, MAX_SUBSESSION_LENGTH)
        .otherwise(F.when(F.col('subsession_length') < 0, 0)
                   .otherwise(F.col('subsession_length')))
    )
    usage_hours = subsession_length / SECONDS_PER_HOUR

    mapping = {
        # attributes
        'client_id': None,
        'subsession_start': F.date_format(client_date, 'yyyy-MM-dd'),
        'profile_creation': F.date_format(pcd, 'yyyy-MM-dd'),
        'days_since_creation': (
            F.when(days_since_creation < 0, F.lit(-1))  # -1 is the value for bad data
            .otherwise(days_since_creation)
            .cast('long')),
        'channel': 'normalized_channel',
        'app_version': (
            # Support before Firefox 55 is limited due to high submission latency
            F.when(F.col('app_version').isNull() |
                   (F.split('app_version', '\\.')[0] >= '55'), F.col('app_version'))
            .otherwise(F.lit("older"))
        ),
        'geo': churn_job.in_top_countries('country'),
        'distribution_id': None,
        'is_funnelcake': is_funnelcake,
        'source': 'attribution.source',
        'medium': 'attribution.medium',
        'campaign': 'attribution.campaign',
        'content': 'attribution.content',
        'sync_usage': churn_job.sync_usage(
            'sync_count_desktop',
            'sync_count_mobile',
            'sync_configured'
        ),
        'is_active': client_date <= utils.to_datetime(F.lit(start_ds)),
        # metrics
        'usage_hours': usage_hours,
        'sum_squared_usage_hours': usage_hours ** 2,
        'total_uri_count':
            F.col('scalar_parent_browser_engagement_total_uri_count').cast('long'),
        'unique_domains_count':
            F.col('scalar_parent_browser_engagement_unique_domains_count').cast('long'),
    }
    expr = utils.build_col_expr(mapping)

    cleaned_data = (
        main_summary
        .select(expr)
        .fillna({
            'profile_creation': DEFAULT_DATE,
            'days_since_creation': -1,
        })
        .fillna(0)
        .fillna(0.0)
        .fillna('unknown')
        .select([field.name for field in retention_schema.fields])
    )

    return cleaned_data


def save(cleaned, path):
    cleaned.write.parquet(path, mode='overwrite')


@click.command()
@click.option('--start-date', required=True)
@click.option('--path', default='retention_intermediate',
              help='Spark compatible route for sharing intermediate data '
                   'with the aggregate job.')
@click.option('--input-bucket', default='telemetry-parquet',
              help='input bucket containing main_summary')
@click.option('--input-prefix', default='main_summary/v4',
              help='input prefix containing main_summary')
@click.option('--period', default=1,
              help='length of the retention period in days')
@click.option('--slack', default=2,
              help='number of days to account for submission latency')
@click.option('--sample/--no-sample', default=False)
def main(start_date, path, input_bucket, input_prefix,
         period, slack, sample):

    spark = SparkSession.builder.appName('retention').getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')

    start_ds = utils.format_date(
        arrow.get(start_date, utils.DS_NODASH),
        utils.DS_NODASH,
        -slack
    )

    main_summary = (
        spark
        .read
        .option('mergeSchema', 'true')
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

    extracted = churn_job.extract(main_summary, new_profile, start_ds, period, slack, sample)

    retention = transform(extracted, start_ds)
    save(retention, path)


if __name__ == '__main__':
    main()
