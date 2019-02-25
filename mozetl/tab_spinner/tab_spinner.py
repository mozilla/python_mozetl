import click
from pyspark.sql import SparkSession
from .generate_counts import run_spinner_etl


@click.command()
def main():
    spark = SparkSession.builder.appName("long_tab_spinners").getOrCreate()
    run_spinner_etl(spark.sparkContext)


if __name__ == "__main__":
    main()
