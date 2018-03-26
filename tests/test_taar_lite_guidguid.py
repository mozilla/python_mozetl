"""Test suite for taar_lite_guidguid Job."""

import json
import boto3
import pytest
from moto import mock_s3
from mozetl.taar import taar_lite_guidguid, taar_utils
from pyspark.sql import Row
from taar_utils import store_json_to_s3, load_amo_external_whitelist


"""
Expected schema of co-installation counts dict.
| -- key_addon: string(nullable=true) 
| -- coinstallation_counts: array(nullable=true) 
| | -- element: struct(containsNull=true) 
| | | -- id: string(nullable=true) 
| | | -- n: long(nullable=true)
"""

MOCK_TELEMETRY_SAMPLE = [
    Row(installed_addons=["test-guid-1", "test-guid-2", "test-guid-3"]),
    Row(installed_addons=["test-guid-1", "test-guid-3"]),
    Row(installed_addons=["test-guid-1", "test-guid-4"]),
    Row(installed_addons=["test-guid-2", "test-guid-5", "test-guid-6"]),
    Row(installed_addons=["test-guid-1", "test-guid-1"])
]

MOCK_ADDON_INSTALLATIONS = {
    "test-guid-1":
        {"test-guid-2": 1,
         "test-guid-3": 2,
         "test-guid-4": 2
         },
    "test-guid-2":
        {"test-guid-1": 2,
         "test-guid-5": 1,
         "test-guid-6": 1
         }}

MOCK_KEYED_ADDONS = [
    Row(key_addon='test-guid-1',
        coinstalled_addons=['test-guid-2','test-guid-3', 'test-guid-4']),
    Row(key_addon='test-guid-1',
        coinstalled_addons=['test-guid-3','test-guid-4']),
    Row(key_addon="test-guid-2",
        coinstalled_addons=['test-guid-1','test-guid-5', 'test-guid-6']),
    Row(key_addon="test-guid-2",
        coinstalled_addons=['test-guid-1'])
    ]

@mock_s3
def test_load_training_from_telemetry(spark):
    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=taar_utils.AMO_DUMP_BUCKET)

    # Store the data in the mocked bucket.
    conn.Object(taar_utils.AMO_DUMP_BUCKET, key=taar_utils.AMO_DUMP_KEY)\
        .put(Body=json.dumps(MOCK_TELEMETRY_SAMPLE))

    expected = {
        "it-IT": ["test-guid-0001"]
    }

    # Sanity check that mocking is happening correctly.
    assert taar_lite_guidguid.load_training_from_telemetry(spark) == MOCK_TELEMETRY_SAMPLE

    assert taar_lite_guidguid\
        .load_training_from_telemetry(spark)\
        .rdd\
        .flatMap(lambda x: taar_lite_guidguid
                 .key_all(x.installed_addons))\
        .toDF(['key_addon', "coinstalled_addons"]) == MOCK_KEYED_ADDONS


def test_addon_keying():
    assert taar_lite_guidguid.key_all(MOCK_KEYED_ADDONS) == MOCK_ADDON_INSTALLATIONS
