import click
from pyspark.sql.functions import col, lit
from .dashboard import (
    explode_search_counts,
    add_derived_columns,
    aggregate_search,
    run_main_summary_based_etl,
    DEFAULT_INPUT_BUCKET,
    DEFAULT_INPUT_PREFIX,
    DEFAULT_SAVE_MODE,
    DEFAULT_GROUPING_COLS,
)


DEFAULT_EXPERIMENT_GROUPING_COLS = (
    DEFAULT_GROUPING_COLS +
    [
        'client_id',
        'default_search_engine',
        'experiment_id',
        'branch',
    ]
)
DEFAULT_EXPERIMENT_IDS = [
    'search-defaults-release-1',
    'search-hijack-release-1',
    'search-default-study'
]


def search_experiment_etl(main_summary):
    return reduce(
        lambda x, y: x.union(y),
        map(
            lambda x: build_one_experiment(main_summary, x),
            DEFAULT_EXPERIMENT_IDS
        )
    )


def build_one_experiment(main_summary, experiment_id):
    filtered = filter_to_experiment(main_summary, experiment_id)
    exploded = explode_search_counts(filtered, DEFAULT_EXPERIMENT_GROUPING_COLS)
    augmented = add_derived_columns(exploded)
    aggregated = aggregate_search(augmented)

    return aggregated


def filter_to_experiment(dataset, experiment_id):
    return (
        dataset
        .withColumn('branch', col('experiments.' + experiment_id))
        .withColumn('experiment_id', lit(experiment_id))
        .where(col('branch').isNull() == False)
    )


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
    run_main_summary_based_etl(submission_date, bucket, prefix, 2,
                               search_experiment_etl, input_bucket,
                               input_prefix, save_mode)
