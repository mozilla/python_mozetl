"""
This ETL job computes the installation rate of all addons and then
cross references against the whitelist to compute the total install
rate for all whitelisted addons.
"""


import click
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import mozetl.taar.taar_utils as taar_utils


OUTPUT_BUCKET = 'telemetry-parquet'
OUTPUT_PREFIX = 'taar/lite/'
OUTPUT_BASE_FILENAME = 'guid_install_ranking'


def extract_telemetry(spark):
    """ load some training data from telemetry given a sparkContext
    """
    sc = spark.sparkContext

    sqlContext = SQLContext.getOrCreate(sc)
    frame = sqlContext.sql("""select addon_guid, count(*) as install_count from
    (select client_id, explode(active_addons[0]) as (addon_guid, addon_row)
    from longitudinal
    WHERE normalized_channel='release' AND
    build IS NOT NULL AND
    build[0].application_name='Firefox') group by addon_guid
    """)
    return frame


def transform(frame):
    json_frame = frame.toJSON()
    result_list = json_frame.collect()
    json_data = {}
    for row in result_list:
        jdata = json.loads(row)
        json_data[jdata['addon_guid']] = jdata['install_count']

    return json_data


def load_s3(result_data, date, prefix, bucket):
    taar_utils.store_json_to_s3(json.dumps(result_data, indent=2),
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

    logging.info("Processing GUID install rankings")

    data_frame = extract_telemetry(spark)
    result_data = transform(data_frame)
    load_s3(result_data, date, prefix, bucket)


if __name__ == '__main__':
    main()
