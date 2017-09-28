import csv
import datetime as DT
import logging
import os
import shutil
import tempfile

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3

ACTIVITY_SUBMISSION_LAG = DT.timedelta(10)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_as_submission_date(date):
    return DT.date.strftime(date, "%Y%m%d")


def parse_as_submission_date(date_string):
    return DT.datetime.strptime(date_string, "%Y%m%d").date()


def format_spark_path(bucket, prefix):
    return "s3://{}/{}".format(bucket, prefix)


def generate_filter_parameters(end_date, days_back):
    d = {}
    min_activity_date = end_date - DT.timedelta(days_back)
    d['min_activity_iso'] = min_activity_date.isoformat()
    d['max_activity_iso'] = (end_date + DT.timedelta(1)).isoformat()

    d['min_submission_string'] = format_as_submission_date(min_activity_date)
    max_submission_date = end_date + ACTIVITY_SUBMISSION_LAG
    d['max_submission_string'] = format_as_submission_date(max_submission_date)
    return d


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
    upload_file_to_s3(s3, filepath, bucket, key)

    logger.info('Sucessfully wrote {} to {}'.format(key, bucket))

    # clean up the temporary directory
    shutil.rmtree(path)


def upload_file_to_s3(client, filepath, bucket, key,
                      ACL='bucket-owner-full-control'):
    with open(filepath, 'rb') as data:
        client.put_object(Bucket=bucket, Key=key, Body=data, ACL=ACL)


def delete_from_s3(bucket_name, keys_to_delete):
    bucket = boto3.resource('s3').Bucket(bucket_name)
    objects = [{'Key': key} for key in keys_to_delete]
    response = bucket.delete_objects(Delete={'Objects': objects})
    code = response['ResponseMetadata']['HTTPStatusCode']
    if code != 200:
        msg = "AWS returned {} when attempting to delete {}"
        raise RuntimeError(msg.format(code, keys_to_delete))


def send_ses(fromaddr,
             subject,
             body,
             recipient,
             filename=''):
    """Send an email via the Amazon SES service.
    Example:
    ```
    send_ses('me@example.com, 'greetings', "Hi!", 'you@example.com)
    ```

    Raises a RuntimeError if the message did not send correctly."""
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = fromaddr
    msg['To'] = recipient
    msg.attach(MIMEText(body))

    if filename:
        attachment = open(filename, 'rb').read()
        part = MIMEApplication(attachment)
        part.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(part)

    ses = boto3.client('ses', region_name='us-west-2')
    result = ses.send_raw_email(
        RawMessage={'Data': msg.as_string()}
    )

    if 'ErrorResponse' in result:
        raise RuntimeError("Error sending email: " + result)
