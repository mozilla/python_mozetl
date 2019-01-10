"""
This ETL job computes the installation rate of all addons and then
cross references against the whitelist to compute the total install
rate for all whitelisted addons.
"""


import click
import json
import logging
from pyspark.sql import SparkSession
import mozetl.taar.taar_utils as taar_utils


OUTPUT_BUCKET = "telemetry-parquet"
OUTPUT_PREFIX = "taar/lite/"
OUTPUT_BASE_FILENAME = "guid_install_ranking"


def extract_telemetry(sparkSession):
    """ Load some training data from telemetry given a sparkContext
    """
    frame = sparkSession.sql(
        """
    SELECT
        addon_guid,
        count(*) as install_count
    FROM
        (SELECT
            EXPLODE(active_addons[0]) as (addon_guid, addon_row)
        FROM
            longitudinal
        WHERE
            normalized_channel='release' AND
            build IS NOT NULL AND
            build[0].application_name='Firefox'
        )
    GROUP BY addon_guid
    """
    )
    return frame


def transform(frame):
    """ Convert the dataframe to JSON and augment each record to
    include the install count for each addon.
    """

    def lambda_func(x):
        return (x.addon_guid, x.install_count)

    return dict(frame.rdd.map(lambda_func).collect())


def load_s3(result_data, date, prefix, bucket):
    taar_utils.store_json_to_s3(
        json.dumps(result_data, indent=2), OUTPUT_BASE_FILENAME, date, prefix, bucket
    )


@click.command()
@click.option("--date", required=True)
@click.option("--bucket", default=OUTPUT_BUCKET)
@click.option("--prefix", default=OUTPUT_PREFIX)
def main(date, bucket, prefix):
    spark = (
        SparkSession.builder.appName("taar_lite_ranking")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info("Processing GUID install rankings")

    data_frame = extract_telemetry(spark)
    result_data = transform(data_frame)
    load_s3(result_data, date, prefix, bucket)


if __name__ == "__main__":
    main()
