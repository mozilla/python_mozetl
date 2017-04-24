import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from python_etl.testpilot import pulse

if __name__ == "__main__":
    conf = SparkConf().setAppName('pulse_etl')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    # Make bdist_egg available to executer nodes
    sc.addPyFile(os.path.join('dist', os.listdir('dist')[0]))

    # Run job
    pulse.etl_job(sc, sqlContext)
