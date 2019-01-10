import gzip
from datetime import datetime, timedelta

import boto3
import botocore
import click
from boto3.s3.transfer import S3Transfer
from moztelemetry.standards import snap_to_beginning_of_week
from pyspark.sql import SparkSession, functions as F
from six import text_type


def csv(f):
    return ",".join([text_type(a) for a in f])


def fmt(d, date_format="%Y%m%d"):
    return datetime.strftime(d, date_format)


def collect_and_upload_csv(df, filename, upload_config):
    """ Collect the dataframe into a csv file and upload to target locations. """
    client = boto3.client("s3", "us-west-2")
    transfer = S3Transfer(client)

    print("{}: Writing output to {}".format(datetime.utcnow(), filename))

    # Write the file out as gzipped csv
    with gzip.open(filename, "wb") as fout:
        fout.write(",".join(df.columns) + "\n")
        print("{}: Wrote header to {}".format(datetime.utcnow(), filename))
        records = df.rdd.collect()
        for r in records:
            try:
                fout.write(csv(r))
                fout.write("\n")
            except UnicodeEncodeError as e:
                print(
                    "{}: Error writing line: {} // {}".format(datetime.utcnow(), e, r)
                )
        print("{}: finished writing lines".format(datetime.utcnow()))

    # upload files to s3
    try:
        for config in upload_config:
            print(
                "{}: Uploading to {} at s3://{}/{}/{}".format(
                    datetime.utcnow(),
                    config["name"],
                    config["bucket"],
                    config["prefix"],
                    filename,
                )
            )

            s3_path = "{}/{}".format(config["prefix"], filename)
            transfer.upload_file(
                filename,
                config["bucket"],
                s3_path,
                extra_args={"ACL": "bucket-owner-full-control"},
            )
    except botocore.exceptions.ClientError as e:
        print("File for {} already exists, skipping upload: {}".format(filename, e))


def marginalize_dataframe(df, attributes, aggregates):
    """ Reduce the granularity of the dataset to the original set of attributes.
    The original set of attributes can be found on commit 2de3ef1 of mozilla-reports. """

    return df.groupby(attributes).agg(*[F.sum(x).alias(x) for x in aggregates])


def convert_week(spark, config, week_start=None):
    """ Convert a given retention period from parquet to csv. """
    df = spark.read.parquet(config["source"])

    # find the latest start date based on the dataset if not provided
    if not week_start:
        start_dates = df.select("week_start").distinct().collect()
        week_start = sorted(start_dates)[-1].week_start

    # find the week end for the filename
    week_end = fmt(datetime.strptime(week_start, "%Y%m%d") + timedelta(6))

    print("Running for the week of {} to {}".format(week_start, week_end))

    # find the target subset of data
    df = df.where(df.week_start == week_start)

    # marginalize the dataframe to the original attributes and upload to s3
    initial_attributes = [
        "channel",
        "geo",
        "is_funnelcake",
        "acquisition_period",
        "start_version",
        "sync_usage",
        "current_version",
        "current_week",
        "is_active",
    ]
    initial_aggregates = ["n_profiles", "usage_hours", "sum_squared_usage_hours"]

    upload_df = marginalize_dataframe(df, initial_attributes, initial_aggregates)
    filename = "churn-{}-{}.by_activity.csv.gz".format(week_start, week_end)
    collect_and_upload_csv(upload_df, filename, config["uploads"])

    # Bug 1355988
    # The size of the data explodes significantly with extra dimensions and is too
    # large to fit into the driver memory. We can write directly to s3 from a
    # dataframe.
    bucket = config["search_cohort"]["bucket"]
    prefix = config["search_cohort"]["prefix"]
    location = "s3://{}/{}/week_start={}".format(bucket, prefix, week_start)

    print("Saving additional search cohort churn data to {}".format(location))

    search_attributes = [
        "source",
        "medium",
        "campaign",
        "content",
        "distribution_id",
        "default_search_engine",
        "locale",
    ]
    attributes = initial_attributes + search_attributes
    upload_df = marginalize_dataframe(df, attributes, initial_aggregates)
    upload_df.write.csv(location, header=True, mode="overwrite", compression="gzip")

    print("Sucessfully finished churn_to_csv")


def assert_valid_config(config):
    """ Assert that the configuration looks correct. """
    # This could be replaced with python schema's
    assert set(["source", "uploads", "search_cohort"]).issubset(list(config.keys()))
    assert set(["bucket", "prefix"]).issubset(list(config["search_cohort"].keys()))
    for entry in config["uploads"]:
        assert set(["name", "bucket", "prefix"]).issubset(list(entry.keys()))


@click.command()
@click.option("--start_date", required=True)
@click.option("--debug", default=False)
def main(start_date, debug):

    spark = SparkSession.builder.appName("churn").getOrCreate()

    config = {
        "source": "s3://telemetry-parquet/churn/v2",
        "uploads": [
            {
                "name": "Pipeline-Analysis",
                "bucket": "net-mozaws-prod-us-west-2-pipeline-analysis",
                "prefix": "mreid/churn",
            },
            {
                "name": "Dashboard",
                "bucket": "net-mozaws-prod-metrics-data",
                "prefix": "telemetry-churn",
            },
        ],
        "search_cohort": {
            "bucket": "net-mozaws-prod-us-west-2-pipeline-analysis",
            "prefix": "amiyaguchi/churn_csv",
        },
    }
    assert_valid_config(config)

    if debug:
        config["uploads"] = [
            {
                "name": "Testing",
                "bucket": "net-mozaws-prod-us-west-2-pipeline-analysis",
                "prefix": "amiyaguchi/churn_csv_testing",
            }
        ]
        config["search_cohort"] = {
            "bucket": "net-mozaws-prod-us-west-2-pipeline-analysis",
            "prefix": "amiyaguchi/churn_csv_testing",
        }
        assert_valid_config(config)

    # Churn waits 10 days for pings to be sent from the client
    week_start_date = snap_to_beginning_of_week(
        datetime.strptime(start_date, "%Y%m%d") - timedelta(10), "Sunday"
    )
    week_start = fmt(week_start_date)

    convert_week(spark, config, week_start)
