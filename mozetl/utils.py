import csv
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_csv(dataframe, path):
    """ Write a dataframe to local disk. """

    # NOTE: Before spark 2.1, toLocalIterator will timeout on some dataframes
    # because rdd materialization can take a long time. Cache the dataframe
    # into main memory, and materialize it with a count.
    dataframe.cache()
    logger.info("Writing {} rows to {}".format(dataframe.count(), path))

    with open(path, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            row = [unicode(s).encode('utf-8') for s in row]
            writer.writerow(row)
