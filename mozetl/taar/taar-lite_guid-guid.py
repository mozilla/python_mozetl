import boto3        # noqa
import itertools    # noqa
import json         # noqa
import sys          # noqa 
import logging      # noqa

from botocore.exceptions import ClientError    # noqa
from collections import Counter                # noqa
from pyspark.sql.functions import collect_list, explode, udf # noqa
from pyspark.sql.types import *                # noqa

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_KEY = 'telemetry-ml/addon_recommender/addons_database.json'

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

    def get_initial_sample():
        # noqa: ignore=E127,E502,E999
        """ Takes an initial sample from the longitudinal dataset
        (randomly sampled from main summary). Coarse filtering on:
        - number of installed addons (greater than 1)
        - corrupt and generally wierd telemetry entries
        - isolating release channel
        - column selection
        """
        return spark.sql("SELECT * FROM longitudinal") \
                    .where("active_addons IS NOT null")\
                    .where("size(active_addons[0]) > 1")\
                    .where("normalized_channel = 'release'")\
                    .where("build IS NOT NULL AND build[0].application_name = 'Firefox'")\
                    .selectExpr("client_id as client_id", "active_addons[0] as active_addons")

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
                # make sure the amo_whitelist has been broadcast to woreker nodes.
                guid not in broadcast_amo_whitelist.value
            )
        # TODO: may need addiitonal whitelisting to remove shield addons
        # TODO: Here we should have a blacklist as well, 
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

    log.info("Init loading client features")
    client_features_frame = get_initial_sample()
    log.info("Loaded client features")

    amo_white_list = load_amo_external_whitelist()
    log.info("AMO White list loaded")

    broadcast_amo_whitelist = sc.broadcast(amo_white_list)
    log.info("Broadcast AMO whitelist success")

    addons_info_frame = get_addons_per_client(client_features_frame)
    log.info("Filtered for valid addons only.")

    taar_training = addons_info_frame.join(client_features_frame, 'client_id', 'inner')\
                                     .drop('active_addons')\
                                     .selectExpr("addon_ids as installed_addons")
    log.info("JOIN completed on TAAR training data")

    return taar_training

def main():
	spark = (SparkSession.builder.enableHiveSupport().getOrCreate())
	df = load_training_from_telemetry(spark)
	df.take(10)


	# Isolate the individual guids that are found installed by any client who has more than one addon installed
	# then collect as a list for keying a new dict later.
	guid_set_unique = df.withColumn("exploded", explode(df.installed_addons)).select("exploded").rdd.flatMap(lambda x: x).distinct().collect()

	# print guid_set_unique[0:20]
	print 'Number of unique guids co-installed in sample: ' + str(len(guid_set_unique))

	unpack_udf = udf(
	    lambda l: [item for sublist in l for item in sublist]
	)


	schema_coinst = ArrayType(StructType([
	    StructField("guid", StringType(), False),
	    StructField("count", IntegerType(), False)
	]))

	count_udf = udf(
	    lambda s: Counter(s).most_common(), 
	    schema_coinst
	)


	small_subset = df.sample(False, 0.01).cache()

	df2 = small_subset.groupBy("installed_addons").agg(collect_list("installed_addons").alias("installed_addons_3")).withColumn("installed_addons", unpack_udf("installed_addons")).withColumn("installed_addons_2", count_udf("installed_addons"))

	df2.take(10)

	return df2
