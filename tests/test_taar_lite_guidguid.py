"""Test suite for taar_lite_guidguid Job."""

import pytest  # noqa
import mock  # noqa
from moto import mock_s3  # noqa
from mozetl.taar import taar_lite_guidguid, taar_utils  # noqa
from pyspark.sql import Row  # noqa

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


@mock.patch('mozetl.taar.taar_lite_guidguid.load_training_from_telemetry',
            return_value=MOCK_TELEMETRY_SAMPLE)
@mock_s3
def test_load_training_from_telemetry(spark):
    # Sanity check that mocking is happening correctly.
    assert taar_lite_guidguid.load_training_from_telemetry(spark) == MOCK_TELEMETRY_SAMPLE

# Exercise the only part of the ETL job happening outside of spark.
def test_addon_keying():
    assert taar_lite_guidguid.key_all(MOCK_KEYED_ADDONS) == MOCK_ADDON_INSTALLATIONS
