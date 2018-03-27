"""
This ETL job computes the co-installation occurrence of white-listed
Firefox webextensions for a sample of the longitudinal telemetry dataset.
"""

import click            # noqa
import boto3            # noqa
import datetime as dt   # noqa
import json             # noqa
import logging          # noqa
from botocore.exceptions import ClientError   # noqa
from pyspark.sql import Row, SparkSession     # noqa
from pyspark.sql.functions import col, collect_list, explode, udf, sum as sum_, max as max_, first  # noqa
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType  # noqa

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

OUTPUT_BUCKET = 'telemetry-parquet'
OUTPUT_BUCKET = 'telemetry-parquet'
OUTPUT_PREFIX = 'taar/lite/'
OUTOUT_FILE_NAME = 'guid_coinstallation.json'

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_KEY = 'telemetry-ml/addon_recommender/addons_database.json'
MAIN_SUMMARY_PATH = 's3://telemetry-parquet/main_summary/v4/'
ONE_WEEK_AGO = (dt.datetime.now() - dt.timedelta(days=7)).strftime('%Y%m%d')


def write_to_s3(source_file_name, s3_dest_file_name, s3_prefix, bucket):
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


def store_json_to_s3(json_data, base_filename, date, prefix, bucket):
    """Saves the JSON data to a local file and then uploads it to S3.

    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param json_data: A string with the JSON content to write.
    :param base_filename: A string with the base name of the file to use for saving
        locally and uploading to S3.
    :param date: A date string in the "YYYYMMDD" format.
    :param prefix: The S3 prefix.
    :param bucket: The S3 bucket name.
    """
    FULL_FILENAME = "{}.json".format(base_filename)

    with open(FULL_FILENAME, "w+") as json_file:
        json_file.write(json_data)

    archived_file_copy =\
        "{}{}.json".format(base_filename, date)

    # Store a copy of the current JSON with datestamp.
    write_to_s3(FULL_FILENAME, archived_file_copy, prefix, bucket)
    write_to_s3(FULL_FILENAME, FULL_FILENAME, prefix, bucket)


# TODO: eventually replace this with the whitelist that Victor is writing ETL for.
def load_amo_external_whitelist():
    """ Download and parse the AMO add-on whitelist.

    :raises RuntimeError: the AMO whitelist file cannot be downloaded or contains
                          no valid add-ons.
    """
    final_whitelist = []
    amo_dump = {}
    try:
        # Load the most current AMO dump JSON resource.
        s3 = boto3.client('s3')
        s3_contents = s3.get_object(Bucket=AMO_DUMP_BUCKET, Key=AMO_DUMP_KEY)
        amo_dump = json.loads(s3_contents['Body'].read())
    except ClientError:
        log.exception("Failed to download from S3", extra={
            "bucket": AMO_DUMP_BUCKET,
            "key": AMO_DUMP_KEY})

    # If the load fails, we will have an empty whitelist, this may be problematic.
    for key, value in amo_dump.items():
        addon_files = value.get('current_version', {}).get('files', {})
        # If any of the addon files are web_extensions compatible, it can be recommended.
        if any([f.get("is_webextension", False) for f in addon_files]):
            final_whitelist.append(value['guid'])

    if len(final_whitelist) == 0:
        raise RuntimeError("Empty AMO whitelist detected")

    return final_whitelist


def load_training_from_telemetry(spark):
    """ load some training data from telemetry given a sparkContext
    """
    sc = spark.sparkContext

    # Define the set of feature names to be used in the donor computations.

    def get_initial_sample(pct_sample=10):
        # noqa: ignore=E127,E502,E999
        """ Takes an initial sample from the longitudinal dataset
        (randomly sampled from main summary). Coarse filtering on:
        - number of installed addons (greater than 1)
        - corrupt and generally wierd telemetry entries
        - isolating release channel
        - column selection
        -
        - pct_sample is an integer [1, 100] indicating sample size
        """
        # Could scale this up to grab more than what is in longitudinal and see how long it takes to run.
        return (spark.table("longitudinal")
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
                guid not in broadcast_amo_whitelist.value
            )

        # TODO: may need additional whitelisting to remove shield addons
        # it should be populated from the current list of shield studies.

        # Create an add-ons dataset un-nesting the add-on map from each
        # user to a list of add-on GUIDs. Also filter undesired add-ons.
        return (
            users_df.rdd
                .map(lambda p: (p["client_id"],
                                [guid for guid, data in p["active_addons"].items() if is_valid_addon(guid, data)]))
                .filter(lambda p: len(p[1]) > 1)
                .toDF(["client_id", "addon_ids"])
        )

    logging.info("Init loading client features")
    client_features_frame = get_initial_sample()
    logging.info("Loaded client features")

    amo_white_list = load_amo_external_whitelist()
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
    return [(i, [b for b in a if not b is i]) for i in a]


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
    longitudinal_addons = load_training_from_telemetry(spark)

    # Only for logging, not used, but may be interesting for later analysis.
    guid_set_unique = longitudinal_addons.withColumn("exploded", explode(longitudinal_addons.installed_addons)).select(
        "exploded").rdd.flatMap(lambda x: x).distinct().collect()
    logging.info("Number of unique guids co-installed in sample: " + str(len(guid_set_unique)))

    restructured = longitudinal_addons.rdd.flatMap(lambda x: key_all(x.installed_addons)).toDF(
        ['key_addon', "coinstalled_addons"])

    # Explode the list of co-installs and count pair occurrences.
    addon_co_installations = (restructured.select('key_addon', explode('coinstalled_addons').alias('coinstalled_addon'))
                              .groupBy("key_addon", 'coinstalled_addon').count())

    # Collect the set of coinstalled_addon, count pairs for each key_addon.
    combine_and_map_cols = udf(lambda x, y: (x, y),
                               StructType([
                                   StructField('id', StringType()),
                                   StructField('n', LongType())
                               ]))

    addon_co_installations_collapsed = (addon_co_installations
                                        .select('key_addon', combine_and_map_cols('coinstalled_addon', 'count')
                                        .alias('id_n'))
                                        .groupby("key_addon")
                                        .agg(collect_list('id_n')
                                        .alias('coinstallation_counts')))
    logging.info(addon_co_installations_collapsed.printSchema())
    logging.info("Collecting final result of co-installations.")

    result_list = addon_co_installations_collapsed.collect()

    result_json = {}
    for key_addon, coinstalls in result_list:
        value_json = {}
        for _id, n in coinstalls:
            value_json[_id] = n
        result_json[key_addon] = value_json

    store_json_to_s3(json.dumps(result_json, indent=2), OUTOUT_FILE_NAME,
                     date, prefix, bucket)

    spark.stop()

