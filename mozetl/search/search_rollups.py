"""
main_summary to Vertica Search Rollups

This job is added by Bug 1364530 and was originally located at
[1]. Search rollups are computed and then ingested by Vertica.

[1] https://gist.github.com/SamPenrose/856aa21191ef9f0de18c94220cd311a8
"""

import logging
import re

import arrow
import boto3
import click
from pyspark.sql import SparkSession, functions as F

from mozetl import utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def explode_searches(dataframe):
    """Expand the search_count list for each ping"""
    columns = [
        "profile",
        "country",
        "locale",
        "distribution_id",
        "default_provider",
    ]

    # Restrict to a whitelist of search_source's to avoid double counting while
    # we test our new search_count telemetry developed in:
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1367554
    # https://bugzilla.mozilla.org/show_bug.cgi?id=1368089
    source_whitelist = [
        'searchbar', 'urlbar', 'abouthome', 'newtab', 'contextmenu', 'system'
    ]

    exploded_search_columns = [
        F.col("search_counts.engine").alias("search_provider"),
        F.col("search_counts.source").alias("search_source"),
        F.col("search_counts.count").alias("search_count")
    ]

    null_search_columns = [
        F.lit("NO_SEARCHES").alias("search_provider"),
        F.lit(0).alias("search_count"),
    ]

    # explode search counts
    exploded_searches = (
        dataframe
        .withColumn("search_counts", F.explode("search_counts"))
        .select(columns + exploded_search_columns)
        .where("search_count > -1 or search_count is null")
        .where(F.col("search_source").isNull() | F.col("search_source").isin(source_whitelist))
        .drop("search_source")
    )

    # `explode` loses null values, preserve these rows
    null_searches = (
        dataframe
        .where("search_counts is null")
        .select(columns + null_search_columns)
    )

    # return their union
    return exploded_searches.union(null_searches)


def with_shares(dataframe):
    """ Assign each client a weight for the contribution toward the
    rollup aggregates.

    Given a single profile with N permutations of
    (search_provider, country, locale, distribution_id,
    default_provider), N = an integer > 0, assign each row a
    profile_share of 1/N.

    Example #1: a user switches default mid-day -> she generates two
    rows, each with profile_count = 1 and profile_share = 0.5.

    Example #2: a profile is cloned to ten laptops, the users of which
    change default engines, travel across country borders, etc. ->
    they generate N rows whose profile_counts sum to 10 and whose
    profile_share sums to 1.0.
    """
    shares_df = (
        dataframe
        .groupBy("profile")
        .count()
        .where("count > 1")
        .withColumn("profile_share", F.lit(1.0) / F.col("count"))
    )

    # left join and default shares to 1.0
    return (
        dataframe
        .join(shares_df, "profile", "left")
        .na.fill(1.0, ["profile_share"])
    )


def rollup_searches(dataframe, attributes, mode):
    """ Gather all permutations of columns specified by the attributes arg:
    1) How many searches fall into each permutation? -> search_count
    2) How many unique profiles fall into each bucket -> profile_count
    3) What share of total profiles does this bucket represent? -> profile_share

    The distinction between profile_count and profile_share is
    necessary because on a given submission_date a single user of a
    profile may switch default search engine (or locale or geo), or a
    profile may be shared by multiple users on a day (causing all the
    other fields to vary if the users are on widely distributed
    machines).

    :dataframe DataFrame: slightly processed data
    :attributes list[str]: columns to group over
    :mode str: `daily` or `monthly`, used to remove `profile_shares`
    """
    metrics = {
        "search_count": F.sum("search_count").alias("search_count"),
        "profile": F.countDistinct("profile").alias("profile_count"),
    }
    if mode == "daily":
        metrics["profile_share"] = F.sum("profile_share").alias("profile_share")

    rollup = (
        dataframe
        .select(attributes + metrics.keys())
        .where("search_count > -1 OR search_count is null")
        .groupBy(attributes)
        .agg(*metrics.values())
    )
    return rollup


def transform(main_summary, mode):
    """ Group over attributes, and count the number of searches

    :main_summary DataFrame: source table
    :mode str: `daily` or `monthly`
    """

    # take a subset of the original dataframe
    columns = [
        F.col("client_id").alias("profile"),
        "country",
        "locale",
        "distribution_id",
        F.col("default_search_engine").alias("default_provider"),
        "search_counts",
    ]

    # attributes of the final search rollup
    attributes = [
        "country",
        "search_provider",
        "default_provider",
        "locale",
        "distribution_id",
    ]

    defaults = {
        "country": "XX",
        "search_provider": "NO_SEARCHES",
        "default_provider": "NO_DEFAULT",
        "locale": "xx",
        "distribution_id": "MOZILLA",
        "search_count": 0,
    }

    dataframe = main_summary.select(columns)
    exploded = explode_searches(dataframe).na.fill(defaults)

    # don't include shares if we process the monthly rollup
    if mode == "monthly":
        processed = exploded
    else:
        processed = with_shares(exploded)
    search_rollup = rollup_searches(processed, attributes, mode)

    return search_rollup


def format_spark_path(bucket, prefix):
    return "s3://{}/{}".format(bucket, prefix)


def get_end_date(ds_start, period):
    """ Return the end date given the start date and period. """
    date_start = arrow.get(ds_start, "YYYYMMDD")
    if period == "monthly":
        date_end = date_start.replace(months=+1)
    else:
        date_end = date_start.replace(days=+1)
    ds_end = date_end.format("YYYYMMDD")

    return ds_end


def extract(spark, path, ds_start, period):
    """Extract the source dataframe from the spark compatible path.

    spark: SparkSession
    path: path to parquet files in s3
    ds_start: inclusive date
    """
    ds_end = get_end_date(ds_start, period)

    return (
        spark.read
        .option("mergeSchema", "true")
        .parquet(path)
        .where((F.col("submission_date_s3") >= ds_start) &
               (F.col("submission_date_s3") < ds_end))
    )


def get_last_manifest_version(bucket, prefix, pattern):
    """ Get the version of the last manifest with the same pattern."""
    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)

    last_manifest_version = None
    for obj in bucket_obj.objects.filter(Prefix=prefix):
        # match for items that match the current pattern
        if re.search(pattern, obj.key):
            # capture the current version number of this pattern
            search_version = re.search("-v(.*?)\.txt", obj.key)
            if search_version and search_version.group(1):
                # get the captured group
                version = int(search_version.group(1))
                last_manifest_version = max(last_manifest_version, version)

    return last_manifest_version


def get_csv_locations(bucket, prefix):
    """Return the locations of csv files within a prefix. The final locations
    are formatted path strings that can be read by spark.
    """
    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)

    csv_files = []
    for obj in bucket_obj.objects.filter(Prefix=prefix):
        if obj.key.endswith(".csv"):
            csv_files.append("s3://{}/{}".format(bucket, obj.key))
    return csv_files


def write_manifest(bucket, prefix, mode, version, start_ds, csv_paths, retry_max=10):
    """ Write a manifest file with the location of the daily rollup files.

    Manifests are part of the current Vertica integration process. A manifest
    enumerates the locations of csv files that contain the rollup. This might be
    useful if the file is partitioned across multiple files.

    :bucket str: s3 bucket
    :prefix str: s3 prefix
    :mode str: either `daily` or `monthly`
    :version int: version of the rollup
    :start_ds str: yyyymmdd
    :csv_paths list[str]: a list of fill s3 path(s) to the csv rollup
    :retry_max int: max number of manifest files that should be generated
    """
    # create the s3 resource for this transaction
    s3 = boto3.client('s3', region_name='us-west-2')

    date_formatted = arrow.get(start_ds, "YYYYMMDD").format("YYYY-MM-DD")
    pattern = "{}-search-rollup-manifest-{}".format(mode, date_formatted)
    prev_version = get_last_manifest_version(bucket, prefix, pattern)

    # there have been too many revisions to this manifest
    if prev_version and prev_version > version + retry_max:
        raise Exception("Too many revisions to manifest")

    # name of the manifest file
    version = version if not prev_version else prev_version + 1
    manifest_basename = "{}-v{}.txt".format(pattern, version)

    # generate the key and data for this transaction
    key = "{}/manifests/{}".format(prefix, manifest_basename)
    data = '\n'.join(csv_paths) + '\n'

    # write the contents of the file to right location
    s3.put_object(Bucket=bucket,
                  Key=key,
                  Body=data,
                  ACL='bucket-owner-full-control')


def save(dataframe, bucket, prefix, mode, version, start_ds):
    """Write dataframe to an s3 location and generate a manifest

    :dataframe DataFrame: rollup data
    :bucket str: s3 bucket
    :prefix str: s3 prefix
    :mode str: either `daily` or `monthly`
    :version int: version of the rollup
    :start_ds str: yyyymmdd
    """

    # format the save location of the data
    start_date = arrow.get(start_ds, "YYYYMMDD")

    # select the relevant fields
    select_expr = [
        F.lit(start_date.format("YYYY-MM-DD")),
        "search_provider",
        "search_count",
        "country",
        "locale",
        "distribution_id",
        "default_provider",
        "profile_count",
        "profile_share",    # only for daily
        F.lit(start_date.replace(days=+1).format("YYYY-MM-DD")),
    ]

    # replace mode specific items, like rollup_date
    if mode == "monthly":
        select_expr[0] = F.lit(start_date.format("YYYY-MM"))

        # NOTE: beware of calling remove when there are Column elements in the
        # array because boolean operations are overloaded for dataframes.
        shares_index = map(str, select_expr).index("profile_share")
        del select_expr[shares_index]

    key = (
        "{}/{}/processed-{}.csv"
        .format(prefix, mode, start_date.format("YYYY-MM-DD"))
    )

    # persist the dataframe to disk
    logging.info("Writing dataframe to {}/{}".format(bucket, key))
    utils.write_csv_to_s3(dataframe.select(select_expr), bucket, key, header=False)

    csv_paths = get_csv_locations(bucket, key)

    # write the manifest to disk
    write_manifest(bucket, prefix, mode, version, start_ds, csv_paths)


@click.command()
@click.argument('start_date')
@click.argument('mode', type=click.Choice(['daily', 'monthly']))
@click.argument('bucket')
@click.argument('prefix')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
def main(start_date, mode, bucket, prefix, input_bucket, input_prefix):
    spark = (
        SparkSession
        .builder
        .appName("search_rollups")
        .getOrCreate()
    )

    version = 2
    source_path = format_spark_path(input_bucket, input_prefix)

    logging.info(
        "Extracting main_summary from {}"
        "starting {} over a {} period..."
        .format(source_path, start_date, mode)
    )
    main_summary = extract(spark, source_path, start_date, mode)

    logging.info("Running the search rollup...")
    rollup = transform(main_summary, mode)

    logging.info("Saving rollup to disk...")
    save(rollup, bucket, prefix, mode, version, start_date)


if __name__ == '__main__':
    main()
