""""A system check for testing integration of various libraries with mozetl.

This sub-module will print out relevant version info. It will also read data
from `main_summary` and print basic statistics to verify that the system is
correctly set-up.
"""

import sys
import click
import logging

from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from mozetl.utils import (
    format_as_submission_date,
    format_spark_path,
    stop_session_safely,
)


logging.basicConfig(level=logging.DEBUG)


@click.command()
@click.option("--local/--no-local", default=False)
@click.option(
    "--submission-date-s3",
    type=str,
    default=format_as_submission_date(datetime.now() - timedelta(2)),
)
@click.option("--input-bucket", type=str, default="telemetry-parquet")
@click.option("--input-prefix", type=str, default="main_summary/v4")
@click.option("--output-bucket", type=str, default="telemetry-test-bucket")
@click.option("--output-prefix", type=str, default="mozetl_system_check")
def main(
    local, submission_date_s3, input_bucket, input_prefix, output_bucket, output_prefix
):
    # print argument information
    for k, v in locals().items():
        print("{}: {}".format(k, v))

    print("Python version: {}".format(sys.version_info))
    spark = SparkSession.builder.getOrCreate()
    print("Spark version: {}".format(spark.version))

    # run a basic count over a sample of `main_summary` from 2 days ago
    if not local:
        ds_nodash = submission_date_s3
        input_path = format_spark_path(input_bucket, input_prefix)
        output_path = format_spark_path(output_bucket, output_prefix)

        print(
            "Reading data for {ds_nodash} from {input_path} and writing to {output_path}".format(
                ds_nodash=ds_nodash, input_path=input_path, output_path=output_path
            )
        )

        main_summary = spark.read.parquet(input_path)
        subset = main_summary.where(
            "submission_date_s3 = '{}'".format(ds_nodash)
        ).where("sample_id='{}'".format(1))
        print("Saw {} documents".format(subset.count()))

        summary = subset.select(
            "memory_mb", "cpu_cores", "subsession_length"
        ).describe()
        summary.show()

        summary.write.parquet(
            output_path + "/submission_date_s3={}/".format(ds_nodash), mode="overwrite"
        )

    stop_session_safely(spark)
    print("Done!")
