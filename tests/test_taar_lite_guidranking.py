"""Test suite for taar_lite_guidranking Job."""

import boto3
from moto import mock_s3
from mozetl.taar import taar_lite_guidranking
from pyspark.sql import Row

"""
Expected schema of co-installation counts dict.
| -- key_addon: string(nullable=true)
| -- coinstallation_counts: array(nullable=true)
| | -- element: struct(containsNull=true)
| | | -- id: string(nullable=true)
| | | -- n: long(nullable=true)
"""


MOCK_TELEMETRY_SAMPLE = [
    Row(addon_guid="test-guid-1", install_count=100),
    Row(addon_guid="test-guid-2", install_count=200),
    Row(addon_guid="test-guid-3", install_count=300),
    Row(addon_guid="test-guid-4", install_count=400),
]

EXPECTED_ADDON_INSTALLATIONS = {u'test-guid-1': 100,
                                u'test-guid-2': 200,
                                u'test-guid-3': 300,
                                u'test-guid-4': 400}


@mock_s3
def test_transform_is_valid(spark):
    """
    Check that the contents of a sample transformation of extracted
    data
    """
    # Build a dataframe using the mocked telemetry data sample
    # rdd = spark.sparkContext.parallelize(MOCK_TELEMETRY_SAMPLE)
    rdd = spark.createDataFrame(MOCK_TELEMETRY_SAMPLE)

    result_json = taar_lite_guidranking.transform(rdd)

    assert EXPECTED_ADDON_INSTALLATIONS == result_json


@mock_s3
def test_load_s3(spark):
    BUCKET = taar_lite_guidranking.OUTPUT_BUCKET
    PREFIX = taar_lite_guidranking.OUTPUT_PREFIX
    dest_filename = taar_lite_guidranking.OUTPUT_BASE_FILENAME + '.json'

    # Create the bucket before we upload
    conn = boto3.resource('s3', region_name='us-west-2')
    bucket_obj = conn.create_bucket(Bucket=BUCKET)

    rdd = spark.createDataFrame(MOCK_TELEMETRY_SAMPLE)
    result_json = taar_lite_guidranking.transform(rdd)
    taar_lite_guidranking.load_s3(result_json, '20180301', PREFIX, BUCKET)

    # Now check that the file is there
    available_objects = list(bucket_obj.objects.filter(Prefix=PREFIX))
    full_s3_name = '{}{}'.format(PREFIX, dest_filename)
    keys = [o.key for o in available_objects]
    assert full_s3_name in keys
