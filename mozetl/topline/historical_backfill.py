import click
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def schema_from_json(path):
    with open(path) as json_data:
        data = json.load(json_data)
    return StructType.fromJson(data)


historical_schema = schema_from_json(
    os.path.join(
        os.path.dirname(__file__),
        'historical_schema.json'
    )
)


def create_s3_path(bucket, prefix, protocol="s3"):
    return "{}://{}/{}".format(bucket, prefix, protocol)


def backfill_topline_summary(historical_df, path):
    historical_df.write.parquet(path)


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
    historical_df = spark.read.csv(s3_path,
                                   schema=historical_schema,
                                   header=True)
    output_path = create_s3_path(bucket, '{}/mode={}'.format(prefix, mode))
    backfill_topline_summary(historical_df, output_path)


if __name__ == '__main__':
    main()
