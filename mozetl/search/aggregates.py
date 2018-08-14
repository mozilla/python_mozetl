'''Firefox Desktop Search Count Datasets

This job produces derived datasets that make it easy to explore search count
data.

The search_aggregates job is used to populate an executive search dashboard.
For more information, see Bug 1381140.

The search_clients_daily job produces a dataset keyed by
`(client_id, submission_date, search_counts.engine, search_counts.source)`.
This allows for deeper analysis into user level search behavior.
'''
import click
import logging
import datetime
from pyspark.sql.functions import (
    explode, col, when, udf, sum, first, count, datediff, from_unixtime, mean,
    size, max, lit
)
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession

from mozetl.constants import SEARCH_SOURCE_WHITELIST


DEFAULT_INPUT_BUCKET = 'telemetry-parquet'
DEFAULT_INPUT_PREFIX = 'main_summary/v4'
DEFAULT_SAVE_MODE = 'error'
MAX_CLIENT_SEARCH_COUNT = 10000

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def agg_first(col):
    return first(col).alias(col)


def search_clients_daily(main_summary):
    return agg_search_data(
        main_summary,
        [
            'client_id',
            'submission_date',
            'engine',
            'source',
        ],
        map(agg_first, [
            'country',
            'app_version',
            'distribution_id',
            'locale',
            'search_cohort',
            'addon_version',
            'os',
            'channel',
            'profile_creation_date',
            'default_search_engine',
            'default_search_engine_data_load_path',
            'default_search_engine_data_submission_url',
            'sample_id',
        ]) + [
            # Count of 'first' subsessions seen for this client_day
            (
                count(when(col('subsession_counter') == 1, 1))
                .alias('sessions_started_on_this_day')
            ),
            first(datediff(
                'subsession_start_date',
                from_unixtime(col('profile_creation_date') * 24 * 60 * 60)
            )).alias('profile_age_in_days'),
            sum(col('subsession_length')/3600.0).alias('subsession_hours_sum'),
            mean(size('active_addons')).alias('active_addons_count_mean'),
            (
                max('scalar_parent_browser_engagement_max_concurrent_tab_count')
                .alias('max_concurrent_tab_count_max')
            ),
            (
                sum('scalar_parent_browser_engagement_tab_open_event_count')
                .alias('tab_open_event_count_sum')
            ),
            (
                sum(col('active_ticks') * 5 / 3600.0)
                .alias('active_hours_sum')
            ),
        ]
    )


def search_aggregates(main_summary):
    return agg_search_data(
        main_summary,
        [
            'addon_version',
            'app_version',
            'country',
            'distribution_id',
            'engine',
            'locale',
            'search_cohort',
            'source',
            'submission_date',
            'default_search_engine',
        ],
        []
    ).where(col('engine').isNotNull())


def agg_search_data(main_summary, grouping_cols, agg_functions):
    """Augment, Explode, and Aggregate search data

    The augmented and exploded dataset has the same columns as main_summary
    with the addition of the following:

        engine: A key in the search_counts field representing a search engine.
                e.g. 'hoolie'
        source: A key in the search_counts field representing a search source
                e.g. 'urlbar'
        tagged-sap: Sum of all searches with partner codes from an SAP
        tagged-follow-on: Sum of all searches with partner codes from a downstream query
        sap: Sum of all searches originating from a direct user interaction with the Firefox UI
        addon_version: The version of the followon-search@mozilla.com addon
    """

    exploded = explode_search_counts(main_summary)
    augmented = add_derived_columns(exploded)

    # Do all aggregations
    aggregated = (
        augmented
        .groupBy(grouping_cols + ['type'])
        .agg(*(agg_functions + [sum('count').alias('count')]))
    )

    # Pivot on search type
    pivoted = (
        aggregated
        .groupBy([column for column in aggregated.columns
                  if column not in ['type', 'count']])
        .pivot(
            'type',
            ['organic', 'tagged-sap', 'tagged-follow-on', 'sap', 'unknown']
        )
        .sum('count')
        # Add convenience columns with underscores instead of hyphens.
        # This makes the table easier to query from Presto.
        .withColumn('tagged_sap', col('tagged-sap'))
        .withColumn('tagged_follow_on', col('tagged-follow-on'))
    )

    return pivoted


def get_search_addon_version(active_addons):
    if not active_addons:
        return None
    return next((a[5] for a in active_addons if a[0] == "followonsearch@mozilla.com"),
                None)


def explode_search_counts(main_summary):
    exploded_col_name = 'single_search_count'
    search_fields = [exploded_col_name + '.' + field
                     for field in ['engine', 'source', 'count']]

    exploded_search_counts = (
        main_summary
        .withColumn(exploded_col_name, explode(col('search_counts')))
        .select(['*'] + search_fields)
        .filter('single_search_count.count < %s' % MAX_CLIENT_SEARCH_COUNT)
        .drop(exploded_col_name)
        .drop('search_counts')
    )

    zero_search_users = (
        main_summary
        .where(col('search_counts').isNull())
        .withColumn('engine', lit(None))
        .withColumn('source', lit(None))
        # Using 0 instead of None for search_count makes many queries easier
        # (e.g. average searche per user)
        .withColumn('count', lit(0))
        .drop('search_counts')
    )

    return exploded_search_counts.union(zero_search_users)


def add_derived_columns(exploded_search_counts):
    '''Adds the following columns to the provided dataset:

    type:           One of 'in-content-sap', 'follow-on', or 'chrome-sap'.
    addon_version:  The version of the followon-search@mozilla addon, or None
    '''
    udf_get_search_addon_version = udf(get_search_addon_version, StringType())

    def _generate_when_expr(source_mappings):
        if not source_mappings:
            return 'unknown'
        source_mapping = source_mappings[0]
        return when(col('source').startswith(source_mapping[0]), source_mapping[1]).otherwise(
            _generate_when_expr(source_mappings[1:])
        )
    when_expr = (
        when(col('source').isin(SEARCH_SOURCE_WHITELIST), 'sap')
        .otherwise(
            _generate_when_expr([('in-content:sap:', 'tagged-sap'),
                                 ('in-content:sap-follow-on:', 'tagged-follow-on'),
                                 ('in-content:organic:', 'organic'),
                                 ('sap:', 'tagged-sap'),
                                 ('follow-on:', 'tagged-follow-on')])
        )
    )

    return (
        exploded_search_counts.withColumn('type', when_expr)
        .withColumn('addon_version', udf_get_search_addon_version('active_addons'))
    )


def generate_rollups(submission_date, output_bucket, output_prefix,
                     output_version, transform_func,
                     input_bucket=DEFAULT_INPUT_BUCKET,
                     input_prefix=DEFAULT_INPUT_PREFIX,
                     save_mode=DEFAULT_SAVE_MODE, orderBy=[]):
    """Load main_summary, apply transform_func, and write to S3"""
    logger.info('Running the {0} ETL job...'.format(transform_func.__name__))
    start = datetime.datetime.now()
    spark = (
        SparkSession
        .builder
        .appName('search_dashboard_etl')
        .getOrCreate()
    )

    source_path = 's3://{}/{}/submission_date_s3={}'.format(
        input_bucket,
        input_prefix,
        submission_date
    )
    output_path = 's3://{}/{}/v{}/submission_date_s3={}'.format(
        output_bucket,
        output_prefix,
        output_version,
        submission_date
    )

    logger.info('Loading main_summary...')
    main_summary = spark.read.parquet(source_path)

    logger.info('Applying transformation function...')
    search_dashboard_data = transform_func(main_summary)

    if orderBy:
        search_dashboard_data = search_dashboard_data.orderBy(*orderBy)

    logger.info('Saving rollups to: {}'.format(output_path))
    (
        search_dashboard_data
        .write
        .mode(save_mode)
        .save(output_path)
    )

    spark.stop()
    logger.info('... done (took: %s)', str(datetime.datetime.now() - start))


# Generate ETL jobs - these are useful if you want to run a job from ATMO
def search_aggregates_etl(submission_date, bucket, prefix,
                          **kwargs):
    generate_rollups(submission_date, bucket, prefix,
                     3, search_aggregates, **kwargs)


def search_clients_daily_etl(submission_date, bucket, prefix,
                             **kwargs):
    generate_rollups(submission_date, bucket, prefix,
                     2, search_clients_daily, orderBy=['sample_id'], **kwargs)


# Generate click commands - wrap ETL jobs to accept click arguements
def gen_click_command(etl_job):
    """Wrap an ETL job with click arguements"""
    @click.command()
    @click.option('--submission_date', required=True)
    @click.option('--bucket', required=True)
    @click.option('--prefix', required=True)
    @click.option('--input_bucket',
                  default=DEFAULT_INPUT_BUCKET,
                  help='Bucket of the input dataset')
    @click.option('--input_prefix',
                  default=DEFAULT_INPUT_PREFIX,
                  help='Prefix of the input dataset')
    @click.option('--save_mode',
                  default=DEFAULT_SAVE_MODE,
                  help='Save mode for writing data')
    def wrapper(*args, **posargs):
        return etl_job(*args, **posargs)

    return wrapper


search_aggregates_click = gen_click_command(search_aggregates_etl)
search_clients_daily_click = gen_click_command(search_clients_daily_etl)
