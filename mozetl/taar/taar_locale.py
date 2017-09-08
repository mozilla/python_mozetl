"""
Bug 1396549 - TAAR Top addons per locale dictionary
This notebook is adapted from a gist that computes the top N addons per
locale after filtering for good candidates (e.g. no unsigned, no disabled,
...) [1].

[1] https://gist.github.com/mlopatka/46dddac9d063589275f06b0443fcc69d
"""

import boto3
import click
import json
import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


LOCALE_FILE_NAME = "top10_dict"


def store_new_state(source_file_name, s3_dest_file_name, s3_prefix, bucket):
    """Store the new json file containing current top addons per locale to S3.

    :param source_file_name: The name of the local source file.
    :param s3_dest_file_name: The name of the destination file on S3.
    :param s3_prefix: The S3 prefix in the bucket.
    :param bucket: The S3 bucket.
    """
    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)

    # Update the state in the analysis bucket.
    key_path = s3_prefix + s3_dest_file_name
    transfer.upload_file(source_file_name, bucket, key_path)


def get_addons(spark):
    """ Longitudinal sample is selected and freshest ping chosen per client.
    Only Firefox release clients are considered.
    Columns are exploded (over addon keys)  to include locale of each addon
    installation instance system addons, disabled addons, unsigned addons
    are filtered out.
    Sorting by addon-installations and grouped by locale.
    """
    return spark.sql("""
        WITH sample AS (
        SELECT client_id,
        settings[0].locale AS locality,
        EXPLODE(active_addons[0])
        FROM longitudinal
        WHERE normalized_channel='release'
          AND build IS NOT NULL
          AND build[0].application_name='Firefox'
        ),

        filtered_sample AS (
        SELECT locality, key AS addon_key FROM sample
        WHERE value['blocklisted'] = FALSE -- not blocklisted
          AND value['type'] = 'extension' -- nice webextensions only
          AND value['signed_state'] = 2 -- fully reviewed addons only
          AND value['user_disabled'] = FALSE -- active addons only get counted
          AND value['app_disabled'] = FALSE -- exclude compatibility disabled addons
          AND value['is_system'] = FALSE -- exclude system addons
          AND locality <> 'null'
          AND key <> 'loop@mozilla.org'
          AND key <> 'firefox@getpocket.com'
          AND key <> 'e10srollout@mozilla.org'
          AND key <> 'firefox-hotfix@mozilla.org'
          AND key <> 'ubufox@ubuntu.com'
          AND key is not null
        ),

        country_addon_pairs AS (
        SELECT
        COUNT(*) AS pair_cnts, addon_key, locality
        from filtered_sample
        GROUP BY locality, addon_key
        )

        SELECT * FROM country_addon_pairs
        ORDER BY locality, pair_cnts DESC
    """)


def compute_threshold(addon_df):
    """ Get a threshold to remove locales with a small
    number of addons installations.
    """
    addon_install_counts = (
        addon_df
        .groupBy('locality')
        .agg({'pair_cnts': 'sum'})
    )

    # Compute a threshold at the 25th percentile to remove locales with a
    # small number of addons installations.
    locale_pop_threshold =\
        addon_install_counts.approxQuantile('sum(pair_cnts)', [0.25], 0.2)[0]

    # Safety net in case the distribution gets really skewed, we should
    # require 2000 addon installation instances to make recommendations.
    return 2000 if locale_pop_threshold < 2000 else locale_pop_threshold


def transform(addon_df, threshold, num_addons):
    """ Converts the locale-specific addon data in to a dictionary.

    :param addon_df: the locale-specific addon dataframe;
    :param threshold: the minimum number of addon-installs per locale;
    :param num_addons: requested number of recommendations.
    :return: a dictionary {<locale>: ['GUID1', 'GUID2', ...]}
    """
    top10_per = {}

    # Decide that we can not make reasonable recommendations without
    # a minimum number of addon installations.
    grouped_addons = (
        addon_df
        .groupBy('locality')
        .agg({'pair_cnts': 'sum'})
        .collect()
    )
    list_of_locales =\
        [i['locality'] for i in grouped_addons if i['sum(pair_cnts)'] > threshold]

    for specific_locale in list_of_locales:
        # Most popular addons per locale sorted by number of installs
        # are added to the list.
        sorted_addon_guids = (
            addon_df
            .filter(addon_df.locality == specific_locale)
            .sort(addon_df.pair_cnts.desc())
            .collect()
        )

        # Creates a dictionary of locales (keys) and list of
        # recommendation GUIDS (values).
        top10_per[specific_locale] =\
            [addon_stats.addon_key for addon_stats in sorted_addon_guids[0:num_addons]]

    return top10_per


def store(dictionary, date, prefix, bucket):
    FULL_FILENAME = "{}.json".format(LOCALE_FILE_NAME)

    with open(FULL_FILENAME, "w+") as dict_file:
        # If we attempt to load invalid JSON from the assembled file,
        # the next function throws
        dict_file.write(json.dumps(dictionary, indent=2))

    archived_file_copy =\
        "{}{}.json".format(LOCALE_FILE_NAME, date)

    # Store a copy of the current JSON with datestamp.
    store_new_state(FULL_FILENAME, archived_file_copy, prefix, bucket)
    store_new_state(FULL_FILENAME, FULL_FILENAME, prefix, bucket)


def generate_dictionary(spark, num_addons):
    """ Wrap the dictionary generation functions in an
    easily testable way.
    """
    addon_df = get_addons(spark)
    locale_pop_threshold = compute_threshold(addon_df)
    return transform(addon_df, locale_pop_threshold, num_addons)


@click.command()
@click.option('--date', required=True)
@click.option('--bucket', default='telemetry-private-analysis-2')
@click.option('--prefix', default='taar/locale/')
@click.option('--num_addons', default=10)
def main(date, bucket, prefix, num_addons):
    spark = (SparkSession
             .builder
             .appName("taar_locale")
             .enableHiveSupport()
             .getOrCreate())

    logger.info("Processing top N addons per locale")
    locale_dict = generate_dictionary(spark, num_addons)
    store(locale_dict, date, prefix, bucket)

    spark.stop()
