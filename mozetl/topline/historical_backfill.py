import json
import logging
import os

import click

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def schema_from_json(path, relative=True):
    if relative:
        path = os.path.join(os.path.dirname(__file__), path)

    with open(path) as json_data:
        data = json.load(json_data)
    return StructType.fromJson(data)


historical_schema = schema_from_json('historical_schema.json')
topline_schema = schema_from_json('topline_schema.json')


def create_s3_path(bucket, prefix, protocol="s3"):
    return "{}://{}/{}".format(bucket, prefix, protocol)


def backfill_topline_summary(historical_df, path):
    logging.info("Saving historical data to {}.".format(path))

    df = (
        historical_df
        .where(
              (F.col('geo') != 'all') &
              (F.col('os') != 'all') &
              (F.col('channel') != 'all'))
        .withColumn('report_start', F.date_format('date', 'yyyyMMdd'))
    )

    # Cast all elements from the csv file. Assume both schemas are flat.
    df = df.select(*[F.col(f.name).cast(f.dataType).alias(f.name)
                     for f in topline_schema.fields])

    # Use the same parititoning scheme as topline_summary
    df.write.partitionBy('report_start').parquet(path)


@click.command()
@click.argument('s3_path')
@click.argument('mode', type=click.Choice(['weekly', 'monthly']))
@click.argument('bucket')
@click.option('--prefix', default='topline_summary/v1')
def main(s3_path, mode, bucket, prefix):
    spark = (SparkSession
             .builder
             .appName('topline_historical_backfill')
             .getOrCreate())

    logging.info("Running historical backfill for {} executive report at {}."
                 .format(mode, s3_path))

    historical_df = spark.read.csv(s3_path,
                                   schema=historical_schema,
                                   header=True)
    output_path = create_s3_path(bucket, '{}/mode={}'.format(prefix, mode))
    backfill_topline_summary(historical_df, output_path)

    logging.info("Finished historical backfill job.")


if __name__ == '__main__':
    main()
