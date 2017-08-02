from os import environ
import logging
from datetime import datetime as dt, date, timedelta
import requests
from pyspark.sql.functions import col, countDistinct, lit
from pyspark.sql import SparkSession

SCHEMA_VERSION = 'v3'
TESTPILOT_EXPERIMENT_ENDPOINT = 'https://testpilot.firefox.com/api/experiments.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# No tests for this job right now since there's a decent chance this will never be touched
# again (except for upstream schema changes) and writing tests would be non-trivial
# TODO: If we end up doing any refactoring or run into bugs, we should write tests for this job


def fmt(the_date, date_format="%Y%m%d"):
    return dt.strftime(the_date, date_format)


def parse(d, date_format="%Y%m%d"):
    return dt.strptime(d, date_format).date()


def get_experiments():
    # If we end up having a lot of retired tests and the job gets slow we could filter by dates
    r = requests.get(TESTPILOT_EXPERIMENT_ENDPOINT)
    r.raise_for_status()
    return [el['addon_id'] for el in r.json()['results'] if 'addon_id' in el]


def get_active_users(dataset, addon_ids):
    tp_installed = dataset \
        .filter(dataset.addon_id == "@testpilot-addon") \
        .select("client_id") \
        .distinct()

    test_installed = dataset \
        .where(col("addon_id").isin(addon_ids)) \
        .select("client_id", "addon_id", "submission_date_s3") \
        .distinct()

    return tp_installed \
        .join(test_installed, test_installed.client_id == tp_installed.client_id) \
        .select(test_installed.client_id.alias("client_id"), "addon_id", "submission_date_s3")


def get_mau_dau(df, run_date_string):
    all_dau = df.filter(df.submission_date_s3 == run_date_string) \
        .agg(countDistinct(df.client_id).alias("dau")) \
        .select(lit("testpilot").alias("addon_id"), "dau")
    dau = df.filter(df.submission_date_s3 == run_date_string) \
        .groupBy("addon_id") \
        .agg(countDistinct("client_id").alias("dau")) \
        .union(all_dau)
    all_mau = df.agg(countDistinct(df.client_id).alias("mau")) \
        .select(lit("testpilot").alias("addon_id"), "mau")
    mau = df.groupBy(df.addon_id) \
        .agg(countDistinct(df.client_id).alias("mau")) \
        .union(all_mau)
    return dau.join(mau, dau.addon_id == mau.addon_id) \
        .select(mau.addon_id.alias("test"), dau.dau.alias("dau"), mau.mau.alias("mau"))


def get_spark_session(app_name):
    # TODO: move this into a general library with more default configs?
    return SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()


def main(bucket, prefix, input_bucket, input_prefix, run_date_string,
         sample_id=None, spark_session=None):
    run_date_dt = parse(run_date_string)
    input_location = "s3://{}/{}".format(input_bucket, input_prefix)
    addon_ids = get_experiments()

    logger.info("Calculating mau/dau for date {} and tests {}".format(run_date_dt, addon_ids))

    if spark_session is None:
        spark_session = get_spark_session("txp_mau_dau")
    try:
        dataset = spark_session.read.parquet(input_location) \
            .filter(col("submission_date_s3") >= fmt(run_date_dt - timedelta(28))) \
            .filter(col("submission_date_s3") <= run_date_string)
        if sample_id:
            dataset = dataset.filter(col("sample_id") == sample_id)
        active_users = get_active_users(dataset, addon_ids)

        mau_dau = get_mau_dau(active_users, run_date_string).persist()

        logger.info("Mau and Dau for {}: {}".format(run_date_dt, mau_dau.collect()))

        output_location = "s3n://{}/{}/{}/submission_date_s3={}".format(
            bucket, prefix, SCHEMA_VERSION, run_date_string
        )
        mau_dau.repartition(1).write.format("parquet").mode("overwrite").save(output_location)
        logger.info("Successfully wrote txp mau dau to {}".format(output_location))
    finally:
        if spark_session is None:
            spark_session.stop()


if __name__ == "__main__":
    bucket = environ['bucket']
    prefix = environ['prefix']
    input_bucket = environ['inbucket']
    input_prefix = environ['inprefix']
    run_date = environ.get('date', fmt(date.today() - timedelta(1)))
    sample_id = environ.get('sampleid', None)

    main(bucket, prefix, input_bucket, input_prefix, run_date, sample_id=sample_id)
