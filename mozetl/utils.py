import csv
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_csv(dataframe, path):
    """ Write a dataframe to local disk.

    Disclaimer: Do not write csv files larger than driver memory. This
    is ~15GB for ec2 c3.xlarge (due to caching overhead).
    """

    # NOTE: Before spark 2.1, toLocalIterator will timeout on some dataframes
    # because rdd materialization can take a long time. Instead of using
    # an iterator over all partitions, collect everything into driver memory.
    logger.info("Writing {} rows to {}".format(dataframe.count(), path))

    with open(path, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(dataframe.columns)
        for row in dataframe.collect():
            row = [unicode(s).encode('utf-8') for s in row]
            writer.writerow(row)
