""" Firefox Desktop Churn and Retention Cohorts

Tracked in Bug 1226379 [1]. The underlying dataset is generated via
the telemetry-batch-view [2] code, and is generated once a day. The
aggregated churn data is updated weekly.

Due to the client reporting latency, we need to wait 10 days for the
data to stabilize. If the date is passed into report through the
environment, it is assumed that the date is at least a week greater
than the report start date.  For example, if today is `20170323`,
airflow will set the environment date to be '20170316'. The date is
then set back 10 days to `20170306`, and pinned to the nearest
Sunday. This example date happens to be a Monday, so the update will
be set to `20170305`.

Code is based on the previous FHR analysis code [3].  Details and
definitions are in Bug 1198537 [4].

The production location of this dataset can be found in the following
location: `s3://telemetry-parquet/churn/v2`.

[1] https://bugzilla.mozilla.org/show_bug.cgi?id=1226379
[2] https://git.io/vSBAt
[3] https://github.com/mozilla/churn-analysis
[4] https://bugzilla.mozilla.org/show_bug.cgi?id=1198537
"""

import logging
import operator

import arrow
import click
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from mozetl.churn import utils, release
from mozetl.churn.schema import churn_schema
from utils import (
    DS, DS_NODASH, preprocess_col_expr, build_col_expr
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SOURCE_COLUMNS = [
    "app_version",
    "attribution",
    "client_id",
    "country",
    "default_search_engine",
    "distribution_id",
    "locale",
    "normalized_channel",
    "profile_creation_date",
    "submission_date_s3",
    "subsession_length",
    "subsession_start_date",
    "sync_configured",
    "sync_count_desktop",
    "sync_count_mobile",
    "timestamp",
    "scalar_parent_browser_engagement_total_uri_count",
    "scalar_parent_browser_engagement_unique_domains_count"
]

TOP_COUNTRIES = {
    "US", "DE", "FR", "RU", "BR", "IN", "PL", "ID", "GB", "CN",
    "IT", "JP", "CA", "ES", "UA", "MX", "AU", "VN", "EG", "AR",
    "PH", "NL", "IR", "CZ", "HU", "TR", "RO", "GR", "AT", "CH",
    "HK", "TW", "BE", "FI", "VE", "SE", "DZ", "MY"
}

# The number of seconds in a single hour, casted to float, so we get
# the fractional part when converting.
SECONDS_PER_HOUR = float(60 * 60)
SECONDS_PER_DAY = 24 * 60 * 60
MAX_SUBSESSION_LENGTH = 60 * 60 * 48  # 48 hours in seconds.
DEFAULT_DATE = '2000-01-01'  # The default date used for cleaning columns


def extract_subset(main_summary, start_ds, period, slack, is_sampled):
    """
    Extract data from the main summary table taking into account the
    retention period and submission latency.

    :param main_summary: dataframe pointing to main_summary.v4
    :param start_ds:     start date of the retention period
    :param period:       length of the retention period
    :param slack:        slack added to account for submission latency
    :return:             a dataframe containing the raw subset of data
    """
    start = arrow.get(start_ds, DS_NODASH)

    predicates = [
        (F.col("subsession_start_date") >= utils.format_date(start, DS)),
        (F.col("subsession_start_date") < utils.format_date(start, DS, period)),
        (F.col("submission_date_s3") >= utils.format_date(start, DS_NODASH)),
        (F.col("submission_date_s3") < utils.format_date(start, DS_NODASH,
                                                         period + slack)),
    ]

    if is_sampled:
        predicates.append((F.col("sample_id") == "57"))

    return (
        main_summary
        .where(reduce(operator.__and__, predicates))
        .select(SOURCE_COLUMNS)
    )


def prepare_client_rows(main_summary):
    """Coalesce client pings into a DataFrame that contains one row for
    each client."""
    in_columns = {
        "client_id",
        "timestamp",
        "scalar_parent_browser_engagement_total_uri_count",
        "scalar_parent_browser_engagement_unique_domains_count",
        "subsession_length"
    }
    out_columns = (
        set(main_summary.columns) |
        {
            "usage_seconds",
            "total_uri_count",
            "unique_domains_count_per_profile"
        }
    )
    assert(in_columns <= set(main_summary.columns))

    # Get the newest ping per client and append to original dataframe
    window_spec = (
        Window
        .partitionBy(F.col('client_id'))
        .orderBy(F.col('timestamp').desc())
    )
    newest_per_client = (
        main_summary
        .withColumn('client_rank', F.row_number().over(window_spec))
        .where(F.col("client_rank") == 1)
    )

    # Compute per client aggregates lost during newest client computation
    select_expr = build_col_expr({
        "client_id": None,
        "total_uri_count": (
            F.coalesce(
                "scalar_parent_browser_engagement_total_uri_count",
                F.lit(0)
            )),
        "unique_domains_count": (
            F.coalesce(
                "scalar_parent_browser_engagement_unique_domains_count",
                F.lit(0)
            )),
        # Clamp broken subsession values to [0, MAX_SUBSESSION_LENGTH].
        "subsession_length": (
            F.when(F.col('subsession_length') > MAX_SUBSESSION_LENGTH,
                   MAX_SUBSESSION_LENGTH)
            .otherwise(
                F.when(F.col('subsession_length') < 0, 0)
                .otherwise(F.col('subsession_length'))))
    })

    per_client_aggregates = (
        main_summary
        .select(*select_expr)
        .groupby('client_id')
        .agg(
            F.sum('subsession_length').alias('usage_seconds'),
            F.sum('total_uri_count').alias('total_uri_count'),
            F.avg('unique_domains_count')
            .alias('unique_domains_count_per_profile')
        )
    )

    # Join the two intermediate datasets
    return (
        newest_per_client
        .join(per_client_aggregates, 'client_id', 'inner')
        .select(*out_columns)
    )


def clean_columns(prepared_clients, effective_version, start_ds):
    """Clean columns in preparation for aggregation.

    This removes invalid values, tidies up dates, and limits the scope of
    several dimensions.

    :param prepared_clients:    `main_summary` rows that conform to the
                                schema of `prepare_client_rows(...)`
    :param effective_version:   DataFrame mapping dates to the active Firefox
                                version that was distributed at that time
    :param start_ds:            DateString to determine whether a row that is
                                being processed is active during the current
                                week
    :returns:                   DataFrame with cleaned columns and rows
    """

    # Temporary column used for determining the validity of a row
    is_valid = "_is_valid"

    pcd = F.from_unixtime(F.col("profile_creation_date") * SECONDS_PER_DAY)
    client_date = utils.to_datetime('subsession_start_date', "yyyy-MM-dd")
    days_since_creation = F.datediff(client_date, pcd)
    device_count = (
        F.coalesce(F.col("sync_count_desktop"), F.lit(0)) +
        F.coalesce(F.col("sync_count_mobile"), F.lit(0))
    )
    is_funnelcake = F.col('distribution_id').rlike("^mozilla[0-9]+.*$")

    attr_mapping = {
        'distribution_id': None,
        'default_search_engine': None,
        'locale': None,
        'subsession_start': client_date,
        'channel': (
            F.when(is_funnelcake, F.concat(
                F.col("normalized_channel"), F.lit("-cck-"), F.col("distribution_id")
            ))
            .otherwise(F.col(("normalized_channel")))),
        'geo': (
            F.when(F.col("country").isin(TOP_COUNTRIES), F.col("country"))
            .otherwise(F.lit("ROW"))),
        # Bug 1289573: Support values like "mozilla86" and "mozilla86-utility-existing"
        'is_funnelcake': (
            F.when(is_funnelcake, F.lit("yes"))
            .otherwise(F.lit("no"))),
        'acquisition_period': F.date_sub(F.next_day(pcd, 'Sun'), 7),
        'sync_usage': (
            F.when(device_count > 1, F.lit("multiple"))
            .otherwise(
                F.when((device_count == 1) | F.col("sync_configured"), F.lit("single"))
                .otherwise(
                    F.when(F.col("sync_configured").isNotNull(), F.lit("no"))
                    .otherwise(F.lit(None))))),
        'current_version': F.col("app_version"),
        'current_week': (
            # -1 is a placeholder for bad data
            F.when(days_since_creation < 0, F.lit(-1))
            .otherwise(F.floor(days_since_creation / 7))
            .cast("long")),
        'source': F.col('attribution.source'),
        'medium': F.col('attribution.medium'),
        'campaign': F.col('attribution.campaign'),
        'content': F.col('attribution.content'),
        'is_active': (
            F.when(client_date < utils.to_datetime(F.lit(start_ds)), F.lit("no"))
            .otherwise(F.lit("yes"))),
    }

    usage_hours = F.col('usage_seconds') / SECONDS_PER_HOUR
    metric_mapping = {
        'n_profiles': F.lit(1),
        'total_uri_count': None,
        'unique_domains_count_per_profile': None,
        'usage_hours': usage_hours,
        'sum_squared_usage_hours': F.pow(usage_hours, 2),
    }

    # Set the attributes to null if it's invalid
    select_attr = build_col_expr({
        attr: F.when(F.col(is_valid), expr).otherwise(F.lit(None))
        for attr, expr in preprocess_col_expr(attr_mapping).iteritems()
    })
    select_metrics = build_col_expr(metric_mapping)
    select_expr = select_attr + select_metrics

    cleaned_data = (
        # Compile per-client rows for the current retention period
        prepared_clients
        # Filter out seemingly impossible rows. One very obvious notion
        # is to make sure that a profile is always created before a sub-session.
        # Unlike `sane_date` in previous versions, this is idempotent and only
        # depends on the data.
        .withColumn("profile_creation", F.date_format(pcd, 'yyyy-MM-dd'))
        .withColumn(is_valid, (
            F.col("profile_creation").isNotNull() &
            (F.col("profile_creation") > DEFAULT_DATE) &
            (pcd <= client_date)
        ))
        .select("profile_creation", *select_expr)
        # Set default values for the rows
        .fillna({
            'acquisition_period': DEFAULT_DATE,
            'is_funnelcake': "no",
            "current_week": -1,
        })
        .fillna(0)
        .fillna(0.0)
        .fillna('unknown')
    )
    result = release.with_effective_version(
        cleaned_data,
        effective_version,
        "profile_creation"
    )

    return result


def transform(main_summary, effective_version, start_ds):
    """Compute the churn data for this week. Note that it takes 10 days
    from the end of this period for all the activity to arrive. This data
    should be from Sunday through Saturday.

    df: DataFrame of the dataset relevant to computing the churn
    week_start: datestring of this time period
    """
    columns = [field.name for field in churn_schema.fields]
    metrics = [
        "n_profiles",
        "usage_hours",
        "sum_squared_usage_hours",
        "total_uri_count",
        "unique_domains_count_per_profile",
    ]
    attributes = set(columns) - set(metrics)

    # One row per client, normalized subsessions
    prepared_clients = prepare_client_rows(main_summary)
    cleaned_data = clean_columns(prepared_clients, effective_version, start_ds)

    # Most aggregates are sums
    agg_expr = {col: F.sum(col).alias(col) for col in metrics}

    # unique_domains_count_per_profile is an odd one, since it is the average
    # across a group of users. In SQL terms:
    # `sum(udcpp) / sum(n_profiles)`
    udcpp = "unique_domains_count_per_profile"
    agg_expr[udcpp] = (agg_expr[udcpp] / agg_expr["n_profiles"]).alias(udcpp)

    # final aggregated data
    records = (
        cleaned_data
        .groupBy(*attributes)
        .agg(*agg_expr.values())
    )

    return records.select(columns)


def save(dataframe, bucket, prefix, start_ds):
    path = utils.format_spark_path(
        bucket, '{}/week_start={}'.format(prefix, start_ds)
    )

    logger.info("Writing output as parquet to {}".format(path))

    (
        dataframe
        .repartition(1)
        .write
        .parquet(path, mode="overwrite")
    )

    logger.info("Finished week {}".format(start_ds))


@click.command()
@click.option('--start_date', required=True)
@click.option('--bucket', required=True)
@click.option('--prefix', default='churn/v2',
              help="output prefix associated with the s3 bucket")
@click.option('--input-bucket', default='telemetry-parquet',
              help="input bucket containing main_summary")
@click.option('--input-prefix', default='main_summary/v4',
              help="input prefix containing main_summary")
@click.option('--period', default=7,
              help="length of the retention period in days")
@click.option('--slack', default=10,
              help="number of days to account for submission latency")
@click.option('--sample/--no-sample', default=False)
def main(start_date, bucket, prefix, input_bucket, input_prefix,
         period, slack, sample):
    """Compute churn / retention information for unique segments of
    Firefox users acquired during a specific period of time.
    """
    spark = (
        SparkSession
        .builder
        .appName("churn")
        .getOrCreate()
    )

    # If this job is scheduled, we need the input date to lag a total of
    # 10 days of slack for incoming data. Airflow subtracts 7 days to
    # account for the weekly nature of this report.
    start_ds = utils.format_date(
        arrow.get(start_date, DS_NODASH),
        DS_NODASH,
        -slack
    )

    main_summary = (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(utils.format_spark_path(input_bucket, input_prefix))
    )
    extracted = extract_subset(main_summary, start_ds, period, slack, sample)

    # Build the "effective version" cache:
    effective_version = release.create_effective_version_table(spark)

    churn = transform(extracted, effective_version, start_ds)
    save(churn, bucket, prefix, start_ds)


if __name__ == '__main__':
    main()
