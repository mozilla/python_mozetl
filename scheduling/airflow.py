import mozetl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":
    conf = SparkConf().setAppName('python_mozetl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    mozetl.etl_job(sc, sqlContext)
