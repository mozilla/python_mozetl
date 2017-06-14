import csv
import logging
import os
import shutil
import tempfile

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_csv(dataframe, path, header=True):
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

        if header:
            writer.writerow(dataframe.columns)

        for row in dataframe.collect():
            row = [unicode(s).encode('utf-8') for s in row]
            writer.writerow(row)


def write_csv_to_s3(dataframe, bucket, key, header=True):
    path = tempfile.mkdtemp()
    if not os.path.exists(path):
        os.makedirs(path)
    filepath = os.path.join(path, 'temp.csv')

    write_csv(dataframe, filepath, header)

    # create the s3 resource for this transaction
    s3 = boto3.client('s3', region_name='us-west-2')

    # write the contents of the file to right location
    with open(filepath, 'rb') as data:
        s3.put_object(Bucket=bucket,
                      Key=key,
                      Body=data,
                      ACL='bucket-owner-full-control')

    logger.info('Sucessfully wrote {} to {}'.format(key, bucket))

    # clean up the temporary directory
    shutil.rmtree(path)
