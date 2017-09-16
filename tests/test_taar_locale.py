"""Test suite for TAAR Locale Job."""

import boto3
import json
import functools
import pytest
from moto import mock_s3
from mozetl.taar import taar_locale
from pyspark.sql.types import (
    StructField, StructType, StringType,
    LongType, BooleanType, ArrayType, MapType
)

longitudinal_schema = StructType([
    StructField("client_id",             StringType(),  True),
    StructField("normalized_channel",    StringType(),  True),
    StructField(
        "build",
        ArrayType(
            StructType(
                [StructField("application_name",  StringType(),  True)]),
            True),
        True),
    StructField("settings",              ArrayType(
        StructType(
            [StructField("locale",        StringType(),  True)]), True),
        True),
    StructField("active_addons",         ArrayType(
        MapType(StringType(), StructType([
            StructField("blocklisted",   BooleanType(), True),
            StructField("type",          StringType(), True),
            StructField("signed_state",  LongType(), True),
            StructField("user_disabled", BooleanType(), True),
            StructField("app_disabled",  BooleanType(), True),
            StructField("is_system",     BooleanType(), True)
        ]), True), True))
    ])

default_sample = {
    "client_id":             "client-id",
    "normalized_channel":    "release",
    "build": [{
        "application_name":  "Firefox"
    }],
    "settings": [{
        "locale":            "en-US"
    }],
    "active_addons": [
      {
        "test-guid-0001": {
          "blocklisted":     False,
          "user_disabled":   False,
          "app_disabled":    False,
          "signed_state":    2,
          "type":            "extension",
          "foreign_install": False,
          "is_system":       False
        },
        "non-whitelisted-addon": {
          "blocklisted":     False,
          "user_disabled":   False,
          "app_disabled":    False,
          "signed_state":    2,
          "type":            "extension",
          "foreign_install": False,
          "is_system":       False
        }
      }
    ]
}

FAKE_AMO_DUMP = {
    "test-guid-0001": {
        "name": {
            "en-US": "test-amo-entry-1"
        },
        "default_locale": "en-US",
        "current_version": {
            "files": [{
                "status": "public",
                "platform": "all",
                "id": 1,
                "is_webextension": True
            }]
        },
        "guid": "test-guid-0001"
    },
    "test-guid-0002": {
        "name": {
            "en-US": "test-amo-entry-2"
        },
        "default_locale": "en-US",
        "current_version": {
            "files": [{
                "status": "public",
                "platform": "all",
                "id": 2,
                "is_webextension": False
            }]
        },
        "guid": "test-guid-0002"
    }
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=longitudinal_schema
    )


@pytest.fixture
def multi_locales_df(generate_data):
    LOCALE_COUNTS = {
        "en-US": 50,
        "en-GB": 60,
        "it-IT": 2500
    }

    sample_snippets = []
    counter = 0
    for locale, count in LOCALE_COUNTS.iteritems():
        for i in range(0, count):
            variation = {
                "settings": [{
                    "locale": locale,
                }],
                "client_id": "client-{}".format(counter)
            }
            sample_snippets.append(variation)
            counter = counter + 1

    return generate_data(sample_snippets)


@mock_s3
def test_load_amo_external_whitelist():
    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=taar_locale.AMO_DUMP_BUCKET)

    # Make sure that whitelist loading fails before mocking the S3 file.
    EXCEPTION_MSG = 'Empty AMO whitelist detected'
    with pytest.raises(RuntimeError) as excinfo:
        taar_locale.load_amo_external_whitelist()

    assert EXCEPTION_MSG in str(excinfo.value)

    # Store an empty file and verify that an exception is raised.
    conn.Object(taar_locale.AMO_DUMP_BUCKET, key=taar_locale.AMO_DUMP_KEY)\
        .put(Body=json.dumps({}))

    with pytest.raises(RuntimeError) as excinfo:
        taar_locale.load_amo_external_whitelist()

    assert EXCEPTION_MSG in str(excinfo.value)

    # Store the data in the mocked bucket.
    conn.Object(taar_locale.AMO_DUMP_BUCKET, key=taar_locale.AMO_DUMP_KEY)\
        .put(Body=json.dumps(FAKE_AMO_DUMP))

    # Check that the web_extension item is still present
    # and the legacy addon is absent.
    whitelist = taar_locale.load_amo_external_whitelist()
    assert 'this_guid_can_not_be_in_amo' not in whitelist

    # Verify that the legacy addon was removed while the
    # web_extension compatible addon is still present.
    assert 'test-guid-0001' in whitelist
    assert 'test-guid-0002' not in whitelist


@mock_s3
def test_generate_dictionary(spark, multi_locales_df):
    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=taar_locale.AMO_DUMP_BUCKET)

    # Store the data in the mocked bucket.
    conn.Object(taar_locale.AMO_DUMP_BUCKET, key=taar_locale.AMO_DUMP_KEY)\
        .put(Body=json.dumps(FAKE_AMO_DUMP))

    multi_locales_df.createOrReplaceTempView("longitudinal")

    # The "en-US" locale must not be reported: we set it to a low
    # frequency on |multi_locale_df|.
    expected = {
        "it-IT": ["test-guid-0001"]
    }

    assert taar_locale.generate_dictionary(spark, 5) == expected
