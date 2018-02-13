# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
This module replicates the scala script over at

https://github.com/mozilla/telemetry-batch-view/blob/1c544f65ad2852703883fe31a9fba38c39e75698/src/main/scala/com/mozilla/telemetry/views/HBaseAddonRecommenderView.scala
"""

from datetime import date
from datetime import datetime
from datetime import timedelta
from pprint import pprint
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import desc, row_number
import argparse
import dateutil.parser
import json
import boto3
from boto3.dynamodb.types import Binary as DynamoBinary
import time
import zlib


MAX_RECORDS = 50
EMPTY_TUPLE = (0, 0, [], [])


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    try:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
    except Exception:
        # Some dates are invalid and won't serialize to
        # ISO format if the year is < 1601.  Yes. This actually
        # happens.  Force the date to epoch in this case
        return date(1970, 1, 1).isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def filterDateAndClientID(row_jstr):
    """
    Filter out any rows where the client_id is None or where the
    subsession_start_date is not a valid date
    """
    (row, jstr) = row_jstr
    try:
        assert row.client_id is not None
        assert row.client_id != ""
        some_date = dateutil.parser.parse(row.subsession_start_date)
        if some_date.year < 1970:
            return False
        return True
    except Exception:
        return False


def list_transformer(row_jsonstr):
    """
    We need to merge two elements of the row data - namely the
    client_id and the start_date into the main JSON blob.

    This is then packaged into a 4-tuple of :

    The first integer represents the number of records that have been
    pushed into DynamoDB.

    The second is the length of the JSON data list. This prevents us
    from having to compute the length of the JSON list unnecessarily.

    The third element of the tuple is the list of JSON data.

    The fourth element is a list of invalid JSON blobs.  We maintain
    this to be no more than 50 elements long.
    """
    (row, json_str) = row_jsonstr
    client_id = row.client_id
    start_date = dateutil.parser.parse(row.subsession_start_date)
    start_date = start_date.date()
    start_date = start_date.strftime("%Y%m%d")
    jdata = json.loads(json_str)
    jdata['client_id'] = client_id
    jdata['start_date'] = start_date

    # Filter out keys with an empty value
    jdata = {key: value for key, value in jdata.items() if value}

    # We need to return a 4-tuple of values
    # (numrec_dynamodb_pushed, json_list_length, json_list, error_json)

    # These 4-tuples can be reduced in a map/reduce
    return (0, 1, [jdata], [])


class DynamoReducer(object):
    def __init__(self, region_name=None, table_name=None):

        if region_name is None:
            region_name = 'us-west-2'

        if table_name is None:
            table_name = 'taar_addon_data'

        self._region_name = region_name
        self._table_name = table_name

    def push_to_dynamo(self, data_tuple):
        """
        This connects to DynamoDB and pushes records in `item_list` into
        a table.

        We accumulate a list of up to 50 elements long to allow debugging
        of write errors.
        """
        # Transformt the data into something that DynamoDB will always
        # accept
        # Set TTL to 60 days from now
        ttl = int(time.time()) + 60*60*24*60
        item_list = [{'client_id': item['client_id'],
                      'TTL': ttl,
                      'json_payload': DynamoBinary(
                                          zlib.compress(json.dumps(item,
                                                                   default=json_serial)
                                                        .encode('utf8'))
                                      )
                      } for item in data_tuple[2]]

        conn = boto3.resource('dynamodb', region_name=self._region_name)
        table = conn.Table(self._table_name)
        try:
            with table.batch_writer(overwrite_by_pkeys=['client_id']) as batch:
                for item in item_list:
                    batch.put_item(Item=item)
            return []
        except Exception:
            # Something went wrong with the batch write write.
            if len(data_tuple[3]) == 50:
                # Too many errors already accumulated, just short circuit
                # and return
                return []
            try:
                error_accum = []
                conn = boto3.resource('dynamodb',
                                      region_name=self._region_name)
                table = conn.Table(self._table_name)
                for item in item_list:
                    try:
                        table.put_item(Item=item)
                    except Exception:
                        error_accum.append(item)
                return error_accum
            except Exception:
                # Something went wrong with the entire DynamoDB
                # connection. Just return the entire list of
                # JSON items
                return item_list

    def dynamo_reducer(self, list_a, list_b, force_write=False):
        """
        This function can be used to reduce tuples of the form in
        `list_transformer`. Data is merged and when MAX_RECORDS
        number of JSON blobs are merged, the list of JSON is batch written
        into DynamoDB.
        """
        new_list = [list_a[0] + list_b[0],
                    list_a[1] + list_b[1],
                    list_a[2] + list_b[2],
                    list_a[3] + list_b[3]]

        if new_list[1] >= MAX_RECORDS or force_write:
            error_blobs = self.push_to_dynamo(new_list)
            if len(error_blobs) > 0:
                # Gather up to maximum 50 error blobs
                new_list[3].extend(error_blobs[:50-new_list[1]])
                # Zero out the number of accumulated records
                new_list[1] = 0
            else:
                # No errors during write process
                # Update number of records written to dynamo
                new_list[0] += new_list[1]
                # Zero out the number of accumulated records
                new_list[1] = 0
                # Clear out the accumulated JSON records
                new_list[2] = []

        return tuple(new_list)


def etl(spark, run_date, region_name, table_name):
    """
    This function is responsible for extract, transform and load.

    Data is extracted from Parquet files in Amazon S3.
    Transforms and filters are applied to the data to create
    3-tuples that are easily merged in a map-reduce fashion.

    The 3-tuples are then loaded into DynamoDB using a map-reduce
    operation in Spark.
    """

    dynReducer = DynamoReducer(region_name, table_name)

    currentDate = run_date
    currentDateString = currentDate.strftime("%Y%m%d")
    print("Processing %s" % currentDateString)

    # Get the data for the desired date out of parquet
    template = "s3://telemetry-parquet/main_summary/v4/submission_date_s3=%s"
    datasetForDate = spark.read.parquet(template % currentDateString)

    print("Parquet data loaded")

    # Get the most recent (client_id, subsession_start_date) tuple
    # for each client since the main_summary might contain
    # multiple rows per client. We will use it to filter out the
    # full table with all the columns we require.

    clientShortList = datasetForDate.select("client_id",
                                            'subsession_start_date',
                                            row_number().over(
                                                Window.partitionBy('client_id')
                                                .orderBy(desc('subsession_start_date'))
                                            ).alias('clientid_rank'))
    print("clientShortList selected")
    clientShortList = clientShortList.where('clientid_rank == 1').drop('clientid_rank')
    print("clientShortList selected")

    select_fields = ["client_id",
                     "subsession_start_date",
                     "subsession_length",
                     "city",
                     "locale",
                     "os",
                     "places_bookmarks_count",
                     "scalar_parent_browser_engagement_tab_open_event_count",
                     "scalar_parent_browser_engagement_total_uri_count",
                     "scalar_parent_browser_engagement_unique_domains_count",
                     "active_addons",
                     "disabled_addons_ids"]
    dataSubset = datasetForDate.select(*select_fields)
    print("datasetForDate select fields completed")

    # Join the two tables: only the elements in both dataframes
    # will make it through.
    clientsData = dataSubset.join(clientShortList,
                                  ["client_id",
                                   'subsession_start_date'])

    print("clientsData join with client_id and subsession_start_date")

    # Convert the DataFrame to JSON and get an RDD out of it.
    subset = clientsData.select("client_id", "subsession_start_date")

    print("clientsData select of client_id and subsession_start_date completed")

    jsonDataRDD = clientsData.select("city",
                                     "subsession_start_date",
                                     "subsession_length",
                                     "locale",
                                     "os",
                                     "places_bookmarks_count",
                                     "scalar_parent_browser_engagement_tab_open_event_count",
                                     "scalar_parent_browser_engagement_total_uri_count",
                                     "scalar_parent_browser_engagement_unique_domains_count",
                                     "active_addons",
                                     "disabled_addons_ids").toJSON()

    print("jsonDataRDD selected")
    rdd = subset.rdd.zip(jsonDataRDD)
    print("subset rdd has been zipped")

    # Filter out any records with invalid dates or client_id
    filtered_rdd = rdd.filter(filterDateAndClientID)
    print("rdd filtered by date and client_id")

    # Transform the JSON elements into a 4-tuple as per docstring
    merged_filtered_rdd = filtered_rdd.map(list_transformer)
    print("rdd has been transformed into tuples")

    # Apply a MapReduce operation to the RDD
    reduction_output = merged_filtered_rdd.reduce(dynReducer.dynamo_reducer)
    print("1st pass dynamo reduction completed")

    # Apply the reducer one more time to force any lingering
    # data to get pushed into DynamoDB.
    final_reduction_output = dynReducer.dynamo_reducer(reduction_output,
                                                       EMPTY_TUPLE,
                                                       force_write=True)
    return final_reduction_output


def run_etljob(spark, run_date, region_name, table_name):
    reduction_output = etl(spark, run_date, region_name, table_name)
    report_data = (reduction_output[0], reduction_output[1])
    print("=" * 40)
    print("%d records inserted to DynamoDB.\n%d records remaining in queue." % report_data)
    print("=" * 40)
    return reduction_output


def main():
    args = parse_args()
    APP_NAME = "HBaseAddonRecommenderView"
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    reduction_output = run_etljob(spark,
                                  args.run_date,
                                  args.region_name,
                                  args.table_name)
    pprint(reduction_output)


def parse_args():
    def valid_date_type(arg_date_str):
        """custom argparse *date* type for user dates values given from the command line"""
        try:
            return datetime.strptime(arg_date_str, "%Y%m%d").date()
        except ValueError:
            msg = "Given Date ({0}) not valid! Expected format, YYYYMMDD!".format(arg_date_str)
            raise argparse.ArgumentTypeError(msg)

    yesterday_str = (date.today() - timedelta(days=1)).strftime("%Y%m%d")

    description = 'Copy data from telemetry S3 parquet files to DynamoDB'
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--date',
                        dest='run_date',
                        action='store',
                        type=valid_date_type,
                        default=yesterday_str,
                        required=False,
                        help='Start date for data submission (YYYYMMDD)')

    parser.add_argument('--region',
                        dest='region_name',
                        action='store',
                        type=str,
                        default='us-west-2',
                        required=True,
                        help='DynamoDB region to write to')

    parser.add_argument('--table',
                        dest='table_name',
                        action='store',
                        type=str,
                        default='taar_addon_data',
                        required=True,
                        help='DynamoDB table to write to')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    main()
