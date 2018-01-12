'''Firefox Desktop Search Count Dashboard

This job produces a derived dataset that makes it easy to explore search count
data. This dataset will be used to populate an executive search dashboard.
For more information, see Bug 1381140 [1].

[1] https://bugzilla.mozilla.org/show_bug.cgi?id=1381140

'''
import click
import logging
import datetime
from pyspark.sql.functions import explode, col, when, udf
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession


DEFAULT_INPUT_BUCKET = 'telemetry-parquet'
DEFAULT_INPUT_PREFIX = 'main_summary/v4'
DEFAULT_SAVE_MODE = 'error'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def search_dashboard_etl(main_summary):
    # todo: this function should consume already exploded and augmented data
    exploded = explode_search_counts(main_summary)
    augmented = add_derived_columns(exploded)
    group_cols = filter(lambda x: x not in ['count', 'type'], augmented.columns)

    return (
        augmented
        .groupBy(group_cols)
        .pivot(
            'type',
            ['tagged-sap', 'tagged-follow-on', 'sap']
        )
        .sum('count')
    )


def get_search_addon_version(active_addons):
    if not active_addons:
        return None
    return next((a[5] for a in active_addons if a[0] == "followonsearch@mozilla.com"),
                None)


def explode_search_counts(main_summary):
    input_columns = [
        'search_counts',
        'submission_date',
        'country',
        'app_version',
        'distribution_id',
        'locale',
        'search_cohort',
        'active_addons',
    ]

    exploded_col_name = 'single_search_count'
    search_fields = [exploded_col_name + '.' + field
                     for field in ['engine', 'source', 'count']]
    udf_get_search_addon_version = udf(get_search_addon_version, StringType())

    return (
        main_summary
        .select(input_columns)
        .withColumn(exploded_col_name, explode(col('search_counts')))
        .withColumn('addon_version', udf_get_search_addon_version('active_addons'))
        .select(['*'] + search_fields)
        .filter('single_search_count.count < 1000')
        .drop(exploded_col_name)
        .drop('search_counts')
        .drop('active_addons')
    )


def add_derived_columns(exploded_search_counts):
    '''Adds the following columns to the provided dataset:

    type: One of 'in-content-sap', 'follow-on', or 'chrome-sap'.
    '''
    return (
        exploded_search_counts
        .withColumn(
            'type',
            when(col('source').startswith('sap:'), 'tagged-sap')
            .otherwise(
                when(col('source').startswith('follow-on:'), 'tagged-follow-on')
                .otherwise('sap')
            )
        )
    )


def generate_dashboard(submission_date, bucket, prefix,
                       input_bucket=DEFAULT_INPUT_BUCKET,
                       input_prefix=DEFAULT_INPUT_PREFIX,
                       save_mode=DEFAULT_SAVE_MODE):
    start = datetime.datetime.now()
    spark = (
        SparkSession
        .builder
        .appName('search_dashboard_etl')
        .getOrCreate()
    )

    version = 2
    source_path = 's3://{}/{}/submission_date_s3={}'.format(
        input_bucket,
        input_prefix,
        submission_date
    )
    output_path = 's3://{}/{}/v{}/submission_date_s3={}'.format(
        bucket,
        prefix,
        version,
        submission_date
    )

    logger.info('Loading main_summary...')
    main_summary = spark.read.parquet(source_path)

    logger.info('Running the search dashboard ETL job...')
    search_dashboard_data = search_dashboard_etl(main_summary)

    logger.info('Saving rollups to: {}'.format(output_path))
    (
        search_dashboard_data
        .repartition(10)
        .write
        .mode(save_mode)
        .save(output_path)
    )

    spark.stop()
    logger.info('... done (took: %s)', str(datetime.datetime.now() - start))


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
def main(submission_date, bucket, prefix, input_bucket, input_prefix,
         save_mode):
    generate_dashboard(submission_date, bucket, prefix, input_bucket,
                       input_prefix, save_mode)
