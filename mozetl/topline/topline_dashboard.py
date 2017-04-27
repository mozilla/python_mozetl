""" Reformat Data for Topline Dashboard

This script replaces `run.sh` and `v4_reformat.py` that generates data ready
for dashboard consumption. The original system used a pipeline consisting of a
custom heka processor, a redshift database, a somewhat involved SQL roll-up,
and a reformatting and upload script.  This is replaced by the
ToplineSummaryView in telemetry-batch-view and this topline_dashboard script,
allowing this process to feed directly from main_summary.

This script assumes that any failures can be regenerated starting with the
original data backed-up in `telemetry-parquet/topline_summary/v1`. This is all
roll-ups up to the swap-over.
"""

import tempfile
import os
import shutil

import boto3
import click

from pyspark.sql import SparkSession, functions as F


def format_spark_path(bucket, prefix):
    """ Generate uri for accessing data on s3 """
    return "s3://{}/{}".format(bucket, prefix)


def reformat_data(df):
    # TODO
    return df


def write_dashboard_data(df, bucket, prefix, mode):
    # create a temporary directory to dump files to
    path = tempfile.mkdtemp()

    # write dataframe to working directory
    df.repartition(1).write.csv(path, header=True, mode='overwrite')

    # find the file that was created
    filepath = None
    for name in os.listdir(path):
        if name.endswith(".csv"):
            filepath = os.path.join(path, name)
            break

    # name of the output key
    key = "{}/v4-{}.csv".format(prefix, mode)

    # create the s3 resource for this transaction
    s3 = boto3.client('s3', region_name='us-west-2')

    # write the contents of the file to right location
    with open(filepath, 'rb') as data:
        s3.put_object(Bucket=bucket,
                      Key=key,
                      Body=data,
                      ACL='bucket-owner-full-control')

    shutil.rmtree(path)


@click.command()
@click.argument('report_start')
@click.argument('mode', type=click.Choice(['weekly', 'monthly']))
@click.argument('bucket')
@click.argument('prefix')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the ToplineSummary dataset')
@click.option('--input_prefix',
              default='topline_summary/v1',
              help='Prefix of the ToplineSummary dataset')
def main(report_start, mode, bucket, prefix, input_bucket, input_prefix):
    spark = (SparkSession
             .builder
             .appName("topline_dashboard")
             .getOrCreate())

    # the inclusion of mode doesn't matter, but we need report_start
    input_path = format_spark_path(
        input_bucket,
        "{}/mode={}".format(input_prefix, mode, report_start)
    )
    topline_summary = (
        spark
        .read.parquet(input_path)
        .where(F.col('report_start') == report_start)
    )

    # modified topline_summary
    dashboard_data = reformat_data(topline_summary)

    # write this data to the dashboard location
    write_dashboard_data(dashboard_data, bucket, prefix, mode)


if __name__ == '__main__':
    main()
