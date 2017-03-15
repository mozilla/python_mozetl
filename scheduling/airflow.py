import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from python_etl import main

if __name__ == "__main__":
    # Figure out what group of functions we're running
    periodicity_functions = {
        'daily': main.daily,
        'weekly': main.weekly,
    }

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('periodicity', choices=periodicity_functions.keys())
    args = parser.parse_args()

    # Initialize Spark objects
    conf = SparkConf().setAppName('python_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Run
    periodicity_functions[args.periodicity](sc, sqlContext)
