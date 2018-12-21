"""
This ETL job computes the co-installation occurrence of white-listed
Firefox webextensions for a sample of the longitudinal telemetry dataset.
"""

import click
import datetime as dt
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import mozetl.taar.taar_utils as taar_utils


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

OUTPUT_BUCKET = 'telemetry-parquet'
OUTPUT_PREFIX = 'taar/lite/'
OUTPUT_BASE_FILENAME = 'guid_coinstallation'

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_KEY = 'telemetry-ml/addon_recommender/addons_database.json'
MAIN_SUMMARY_PATH = 's3://telemetry-parquet/main_summary/v4/'
ONE_WEEK_AGO = (dt.datetime.now() - dt.timedelta(days=7)).strftime('%Y%m%d')


def extract_telemetry(spark):
    """ load some training data from telemetry given a sparkContext
    """
    sc = spark.sparkContext

    # Define the set of feature names to be used in the donor computations.

    def get_initial_sample():
        """ Takes an initial sample from the longitudinal dataset
        (randomly sampled from main summary). Coarse filtering on:
        - number of installed addons (greater than 1)
        - corrupt and generally wierd telemetry entries
        - isolating release channel
        - column selection
        """
        # Could scale this up to grab more than what is in
        # longitudinal and see how long it takes to run.
        return (spark.table("longitudinal")  # noqa: E127,E501,E502,E999
                .where("active_addons IS NOT null")
                .where("size(active_addons[0]) > 1")
                .where("normalized_channel = 'release'")
                .where("build IS NOT NULL AND build[0].application_name = 'Firefox'")
                .selectExpr("client_id as client_id", "active_addons[0] as active_addons"))

    def get_addons_per_client(users_df):
        """ Extracts a DataFrame that contains one row
        for each client along with the list of active add-on GUIDs.
        """

        def is_valid_addon(guid, addon):
            """ Filter individual addons out to exclude, system addons,
            legacy addons, disabled addons, sideloaded addons.
            """
            return not (
                addon.is_system or
                addon.app_disabled or
                addon.type != "extension" or
                addon.user_disabled or
                addon.foreign_install or
                # make sure the amo_whitelist has been broadcast to worker nodes.
                guid not in broadcast_amo_whitelist.value or

                # Make sure that the Pioneer addon is explicitly
                # excluded
                guid == "pioneer-opt-in@mozilla.org"
            )

        # Create an add-ons dataset un-nesting the add-on map from each
        # user to a list of add-on GUIDs. Also filter undesired add-ons.
        return (
            users_df.rdd
                    .map(lambda p: (p["client_id"],
                                    [guid
                                     for guid, data in list(p["active_addons"].items())
                                     if is_valid_addon(guid, data)]))
                    .filter(lambda p: len(p[1]) > 1)
                    .toDF(["client_id", "addon_ids"])
        )

    client_features_frame = get_initial_sample()

    amo_white_list = taar_utils.load_amo_external_whitelist()
    logging.info("AMO White list loaded")

    broadcast_amo_whitelist = sc.broadcast(amo_white_list)
    logging.info("Broadcast AMO whitelist success")

    addons_info_frame = get_addons_per_client(client_features_frame)
    logging.info("Filtered for valid addons only.")

    taar_training = addons_info_frame.join(client_features_frame, 'client_id', 'inner') \
        .drop('active_addons') \
        .selectExpr("addon_ids as installed_addons")
    logging.info("JOIN completed on TAAR training data")

    return taar_training


def key_all(a):
    """
    Return (for each Row) a two column set of Rows that contains each individual
    installed addon (the key_addon) as the first column and an array of guids of
    all *other* addons that were seen co-installed with the key_addon. Excluding
    the key_addon from the second column to avoid inflated counts in later aggregation.
    """
    return [(i, [b for b in a if b is not i]) for i in a]


def transform(longitudinal_addons):
    # Only for logging, not used, but may be interesting for later analysis.
    guid_set_unique = longitudinal_addons.withColumn("exploded",
            F.explode(longitudinal_addons.installed_addons)).select(  # noqa: E501 - long lines
        "exploded").rdd.flatMap(lambda x: x).distinct().collect()
    logging.info("Number of unique guids co-installed in sample: " + str(len(guid_set_unique)))

    restructured = longitudinal_addons.rdd.flatMap(lambda x: key_all(x.installed_addons)).toDF(
        ['key_addon', "coinstalled_addons"])

    # Explode the list of co-installs and count pair occurrences.
    addon_co_installations = (restructured.select('key_addon',
        F.explode('coinstalled_addons').alias('coinstalled_addon'))  # noqa: E501 - long lines
                              .groupBy("key_addon", 'coinstalled_addon').count())

    # Collect the set of coinstalled_addon, count pairs for each key_addon.
    combine_and_map_cols = F.udf(lambda x, y: (x, y),
                                 StructType([StructField('id', StringType()),
                                             StructField('n', LongType())]))

    # Spark functions are sometimes long and unwieldy. Tough luck.
    # Ignore E128 and E501 long line errors
    addon_co_installations_collapsed = (addon_co_installations  # noqa: E128
                                        .select('key_addon',
                                            combine_and_map_cols('coinstalled_addon', 'count')  # noqa: E501
                                        .alias('id_n'))
                                        .groupby("key_addon")
                                        .agg(F.collect_list('id_n')
                                        .alias('coinstallation_counts')))
    logging.info(addon_co_installations_collapsed.printSchema())
    logging.info("Collecting final result of co-installations.")

    return addon_co_installations_collapsed


def load_s3(result_df, date, prefix, bucket):
    result_list = result_df.collect()
    result_json = {}

    for row in result_list:
        key_addon = row.key_addon
        coinstalls = row.coinstallation_counts
        value_json = {}
        for _id, n in coinstalls:
            value_json[_id] = n
        result_json[key_addon] = value_json

    taar_utils.store_json_to_s3(json.dumps(result_json, indent=2),
                                OUTPUT_BASE_FILENAME,
                                date,
                                prefix,
                                bucket)


@click.command()
@click.option('--date', required=True)
@click.option('--bucket', default=OUTPUT_BUCKET)
@click.option('--prefix', default=OUTPUT_PREFIX)
def main(date, bucket, prefix):
    spark = (SparkSession
             .builder
             .appName("taar_lite=")
             .enableHiveSupport()
             .getOrCreate())

    logging.info("Loading telemetry sample.")

    longitudinal_addons = extract_telemetry(spark)
    result_df = transform(longitudinal_addons)
    load_s3(result_df, date, prefix, bucket)

    spark.stop()
