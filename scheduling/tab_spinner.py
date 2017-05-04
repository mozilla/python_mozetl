import os
from pyspark import SparkConf, SparkContext
from mozetl.tab_spinner import generate_counts

if __name__ == "__main__":
    conf = SparkConf().setAppName('long_tab_spinners')
    sc = SparkContext(conf=conf)

    # Make bdist_egg available to executer nodes
    sc.addPyFile(os.path.join('dist', os.listdir('dist')[0]))

    # Run job
    generate_counts.run_spinner_etl(sc)

    sc.stop()
