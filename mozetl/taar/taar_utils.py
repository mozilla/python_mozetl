# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.

import contextlib
import json
import logging
import os.path
import shutil
import tempfile

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_KEY = 'telemetry-ml/addon_recommender/addons_database.json'

AMO_WHITELIST_KEY = 'telemetry-ml/addon_recommender/whitelist_addons_database.json'


@contextlib.contextmanager
def selfdestructing_path(dirname):
    yield dirname
    shutil.rmtree(dirname)


def read_from_s3(s3_dest_file_name, s3_prefix, bucket):
    """
    Read JSON from an S3 bucket and return the decoded JSON blob
    """

    full_s3_name = '{}{}'.format(s3_prefix, s3_dest_file_name)
    conn = boto3.resource('s3', region_name='us-west-2')
    stored_data = json.loads(
        conn
        .Object(bucket, full_s3_name)
        .get()['Body']
        .read()
        .decode('utf-8')
    )
    return stored_data


def write_to_s3(source_file_name, s3_dest_file_name, s3_prefix, bucket):
    """Store the new json file containing current top addons per locale to S3.

    :param source_file_name: The name of the local source file.
    :param s3_dest_file_name: The name of the destination file on S3.
    :param s3_prefix: The S3 prefix in the bucket.
    :param bucket: The S3 bucket.
    """
    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)

    # Update the state in the analysis bucket.
    key_path = s3_prefix + s3_dest_file_name
    transfer.upload_file(source_file_name, bucket, key_path)


def store_json_to_s3(json_data, base_filename, date, prefix, bucket):
    """Saves the JSON data to a local file and then uploads it to S3.

    Two copies of the file will get uploaded: one with as "<base_filename>.json"
    and the other as "<base_filename><YYYYMMDD>.json" for backup purposes.

    :param json_data: A string with the JSON content to write.
    :param base_filename: A string with the base name of the file to use for saving
        locally and uploading to S3.
    :param date: A date string in the "YYYYMMDD" format.
    :param prefix: The S3 prefix.
    :param bucket: The S3 bucket name.
    """

    tempdir = tempfile.mkdtemp()

    with selfdestructing_path(tempdir):
        JSON_FILENAME = "{}.json".format(base_filename)
        FULL_FILENAME = os.path.join(tempdir, JSON_FILENAME)
        with open(FULL_FILENAME, "w+") as json_file:
            json_file.write(json_data)

        archived_file_copy =\
            "{}{}.json".format(base_filename, date)

        # Store a copy of the current JSON with datestamp.
        write_to_s3(FULL_FILENAME, archived_file_copy, prefix, bucket)
        write_to_s3(FULL_FILENAME, JSON_FILENAME, prefix, bucket)


def load_amo_external_whitelist():
    """ Download and parse the AMO add-on whitelist.

    :raises RuntimeError: the AMO whitelist file cannot be downloaded or contains
                          no valid add-ons.
    """
    final_whitelist = []
    amo_dump = {}
    try:
        # Load the most current AMO dump JSON resource.
        s3 = boto3.client('s3')
        s3_contents = s3.get_object(Bucket=AMO_DUMP_BUCKET, Key=AMO_WHITELIST_KEY)
        amo_dump = json.loads(s3_contents['Body'].read())
    except ClientError:
        logger.exception("Failed to download from S3", extra={
            "bucket": AMO_DUMP_BUCKET,
            "key": AMO_DUMP_KEY})

    # If the load fails, we will have an empty whitelist, this may be problematic.
    for key, value in amo_dump.items():
        addon_files = value.get('current_version', {}).get('files', {})
        # If any of the addon files are web_extensions compatible, it can be recommended.
        if any([f.get("is_webextension", False) for f in addon_files]):
            final_whitelist.append(value['guid'])

    if len(final_whitelist) == 0:
        raise RuntimeError("Empty AMO whitelist detected")

    return final_whitelist


def load_amo_curated_whitelist():
    """
    Return the curated whitelist of addon GUIDs
    """
    whitelist = read_from_s3('only_guids_top_200.json',
                             'telemetry-ml/addon_recommender/',
                             'telemetry-parquet')
    return list(whitelist)
