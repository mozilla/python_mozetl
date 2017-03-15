import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from python_etl import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('periodicity', choices=main.job_groups.keys())
    args = parser.parse_args()

    # Initialize Spark objects
    conf = SparkConf().setAppName('python_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Run
    main.job_groups[args.periodicity](sc, sqlContext)
