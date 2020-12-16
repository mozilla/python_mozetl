"""Test suite for taar_lite_guidguid Job."""

import boto3
from moto import mock_s3
from mozetl.taar import taar_lite_guidguid
from pyspark.sql import Row
from mock import Mock

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
    Row(installed_addons=["test-guid-1", "test-guid-1"]),
]

EXPECTED_ADDON_INSTALLATIONS = [
    (  # noqa
        Row(
            key_addon="test-guid-1",
            coinstalled_addons=["test-guid-2", "test-guid-3", "test-guid-4"],
        ),
        [
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-3", "test-guid-4"],
            ),
            Row(
                key_addon="test-guid-2",
                coinstalled_addons=["test-guid-1", "test-guid-5", "test-guid-6"],
            ),
            Row(key_addon="test-guid-2", coinstalled_addons=["test-guid-1"]),
        ],
    ),
    (
        Row(key_addon="test-guid-1", coinstalled_addons=["test-guid-3", "test-guid-4"]),
        [
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-2", "test-guid-3", "test-guid-4"],
            ),
            Row(
                key_addon="test-guid-2",
                coinstalled_addons=["test-guid-1", "test-guid-5", "test-guid-6"],
            ),
            Row(key_addon="test-guid-2", coinstalled_addons=["test-guid-1"]),
        ],
    ),
    (
        Row(
            key_addon="test-guid-2",
            coinstalled_addons=["test-guid-1", "test-guid-5", "test-guid-6"],
        ),
        [
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-2", "test-guid-3", "test-guid-4"],
            ),
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-3", "test-guid-4"],
            ),
            Row(key_addon="test-guid-2", coinstalled_addons=["test-guid-1"]),
        ],
    ),
    (
        Row(key_addon="test-guid-2", coinstalled_addons=["test-guid-1"]),
        [
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-2", "test-guid-3", "test-guid-4"],
            ),
            Row(
                key_addon="test-guid-1",
                coinstalled_addons=["test-guid-3", "test-guid-4"],
            ),
            Row(
                key_addon="test-guid-2",
                coinstalled_addons=["test-guid-1", "test-guid-5", "test-guid-6"],
            ),
        ],
    ),
]

MOCK_KEYED_ADDONS = [
    Row(
        key_addon="test-guid-1",
        coinstalled_addons=["test-guid-2", "test-guid-3", "test-guid-4"],
    ),
    Row(key_addon="test-guid-1", coinstalled_addons=["test-guid-3", "test-guid-4"]),
    Row(
        key_addon="test-guid-2",
        coinstalled_addons=["test-guid-1", "test-guid-5", "test-guid-6"],
    ),
    Row(key_addon="test-guid-2", coinstalled_addons=["test-guid-1"]),
]


EXPECTED_GUID_GUID_DATA = [
    Row(
        key_addon="test-guid-2",
        coinstallation_counts=[
            Row(id="test-guid-6", n=1),
            Row(id="test-guid-5", n=1),
            Row(id="test-guid-3", n=1),
            Row(id="test-guid-1", n=1),
        ],
    ),
    Row(key_addon="test-guid-4", coinstallation_counts=[Row(id="test-guid-1", n=1)]),
    Row(
        key_addon="test-guid-3",
        coinstallation_counts=[Row(id="test-guid-2", n=1), Row(id="test-guid-1", n=2)],
    ),
    Row(
        key_addon="test-guid-5",
        coinstallation_counts=[Row(id="test-guid-6", n=1), Row(id="test-guid-2", n=1)],
    ),
    Row(
        key_addon="test-guid-1",
        coinstallation_counts=[
            Row(id="test-guid-2", n=1),
            Row(id="test-guid-1", n=2),
            Row(id="test-guid-3", n=2),
            Row(id="test-guid-4", n=1),
        ],
    ),
    Row(
        key_addon="test-guid-6",
        coinstallation_counts=[Row(id="test-guid-2", n=1), Row(id="test-guid-5", n=1)],
    ),
]


# Exercise the only part of the ETL job happening outside of spark.
def test_addon_keying():
    assert taar_lite_guidguid.key_all(MOCK_KEYED_ADDONS) == EXPECTED_ADDON_INSTALLATIONS


def test_get_addons_per_client(spark, df_equals):
    def make_addon_meta(addon_id):
        return Row(
            **dict(
                addon_id=addon_id,
                is_system=False,
                type="extension",
                user_disabled=False,
                app_disabled=False,
                foreign_install=False,
            )
        )

    MOCK_USERS_DF = [
        Row(
            client_id="client-1",
            active_addons=[
                make_addon_meta("test-guid-1"),
                make_addon_meta("test-guid-2"),
                make_addon_meta("test-guid-3"),
            ],
        ),
        Row(
            client_id="client_2",
            active_addons=[
                make_addon_meta("test-guid-1"),
                make_addon_meta("test-guid-3"),
            ],
        ),
        Row(
            client_id="client_3",
            active_addons=[
                make_addon_meta("test-guid-1"),
                make_addon_meta("test-guid-4"),
            ],
        ),
        Row(
            client_id="client_4",
            active_addons=[
                make_addon_meta("test-guid-2"),
                make_addon_meta("test-guid-5"),
                make_addon_meta("test-guid-6"),
            ],
        ),
        Row(client_id="client_5", active_addons=[make_addon_meta("test-guid-1")]),
    ]

    test_users_df = spark.createDataFrame(MOCK_USERS_DF)

    mock_broadcast_amo_whitelist = Mock()
    mock_broadcast_amo_whitelist.value = Mock()
    mock_broadcast_amo_whitelist.value.__contains__ = lambda self, x: True

    result = taar_lite_guidguid.get_addons_per_client(
        mock_broadcast_amo_whitelist, test_users_df
    )
    assert len(result.collect()) > 0


@mock_s3
def test_transform_is_valid(spark, df_equals):
    """
    Check that the contents of a sample transformation of extracted
    data
    """
    # Build a dataframe using the mocked telemetry data sample
    df = spark.createDataFrame(MOCK_TELEMETRY_SAMPLE)

    result_data = taar_lite_guidguid.transform(df)
    expected_data = spark.createDataFrame(EXPECTED_GUID_GUID_DATA)
    assert df_equals(result_data, expected_data)


@mock_s3
def test_load_s3(spark):
    BUCKET = taar_lite_guidguid.OUTPUT_BUCKET
    PREFIX = taar_lite_guidguid.OUTPUT_PREFIX
    dest_filename = taar_lite_guidguid.OUTPUT_BASE_FILENAME + ".json"

    # Create the bucket before we upload
    conn = boto3.resource("s3", region_name="us-west-2")
    bucket_obj = conn.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={
            "LocationConstraint": "us-west-2",
        },
    )

    load_df = spark.createDataFrame(EXPECTED_GUID_GUID_DATA)
    taar_lite_guidguid.load_s3(load_df, "20180301", PREFIX, BUCKET)

    # Now check that the file is there
    available_objects = list(bucket_obj.objects.filter(Prefix=PREFIX))
    full_s3_name = "{}{}".format(PREFIX, dest_filename)
    keys = [o.key for o in available_objects]
    assert full_s3_name in keys
