"""Test suite for TAAR Locale Job."""

import boto3
import json
import functools
import pytest
from moto import mock_s3
from mozetl.taar import taar_locale, taar_utils
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    LongType,
    BooleanType,
    ArrayType,
)

clientsdaily_schema = StructType(
    [
        StructField("client_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("city", StringType(), True),
        StructField("subsession_hours_sum", LongType(), True),
        StructField("os", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("locale", StringType(), True),
        StructField(
            "active_addons",
            # active_addons is a list of dictionaries holding all
            # metadata related to an addon
            ArrayType(
                StructType(
                    [
                        StructField("addon_id", StringType(), True),
                        StructField("app_disabled", BooleanType(), True),
                        StructField("blocklisted", BooleanType(), True),
                        StructField("foreign_install", BooleanType(), True),
                        StructField("has_binary_components", BooleanType(), True),
                        StructField("install_day", LongType(), True),
                        StructField("is_system", BooleanType(), True),
                        StructField("is_web_extension", BooleanType(), True),
                        StructField("multiprocess_compatible", BooleanType(), True),
                        StructField("name", StringType(), True),
                        StructField("scope", LongType(), True),
                        StructField("signed_state", LongType(), True),
                        StructField("type", StringType(), True),
                        StructField("update_day", LongType(), True),
                        StructField("user_disabled", BooleanType(), True),
                        StructField("version", StringType(), True),
                    ]
                ),
                True,
            ),
        ),
        StructField("places_bookmarks_count_mean", LongType(), True),
        StructField(
            "scalar_parent_browser_engagement_tab_open_event_count_sum",
            LongType(),
            True,
        ),
        StructField(
            "scalar_parent_browser_engagement_total_uri_count_sum", LongType(), True
        ),
        StructField(
            "scalar_parent_browser_engagement_unique_domains_count_mean",
            LongType(),
            True,
        ),
    ]
)

default_sample = {
    "client_id": "client-id",
    "channel": "release",
    "app_name": "Firefox",
    "locale": "en-US",
    "active_addons": [
        {
            "addon_id": "test-guid-0001",
            "app_disabled": False,
            "blocklisted": False,
            "foreign_install": False,
            "is_system": False,
            "signed_state": 2,
            "type": "extension",
            "user_disabled": False,
        },
        {
            "addon_id": "non-whitelisted-addon",
            "app_disabled": False,
            "blocklisted": False,
            "foreign_install": False,
            "is_system": False,
            "signed_state": 2,
            "type": "extension",
            "user_disabled": False,
        },
    ],
}

FAKE_AMO_DUMP = {
    "test-guid-0001": {
        "name": {"en-US": "test-amo-entry-1"},
        "default_locale": "en-US",
        "current_version": {
            "files": [
                {
                    "status": "public",
                    "platform": "all",
                    "id": 1,
                    "is_webextension": True,
                }
            ]
        },
        "guid": "test-guid-0001",
    },
    "test-guid-0002": {
        "name": {"en-US": "test-amo-entry-2"},
        "default_locale": "en-US",
        "current_version": {
            "files": [
                {
                    "status": "public",
                    "platform": "all",
                    "id": 2,
                    "is_webextension": False,
                }
            ]
        },
        "guid": "test-guid-0002",
    },
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=clientsdaily_schema,
    )


@pytest.fixture
def multi_locales_df(generate_data):
    LOCALE_COUNTS = {"en-US": 50, "en-GB": 60, "it-IT": 2500}

    sample_snippets = []
    counter = 0
    for locale, count in list(LOCALE_COUNTS.items()):
        for i in range(count):
            variation = {"locale": locale, "client_id": "client-{}".format(counter)}
            sample_snippets.append(variation)
            counter = counter + 1

    return generate_data(sample_snippets)


@mock_s3
def test_generate_dictionary(spark, multi_locales_df):
    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=taar_utils.AMO_DUMP_BUCKET)

    # Store the data in the mocked bucket.
    conn.Object(
        taar_utils.AMO_DUMP_BUCKET, key=taar_utils.AMO_CURATED_WHITELIST_KEY
    ).put(Body=json.dumps(FAKE_AMO_DUMP))

    multi_locales_df.createOrReplaceTempView("clients_daily")

    # The "en-US" locale must not be reported: we set it to a low
    # frequency on |multi_locale_df|.
    expected = {"it-IT": [["test-guid-0001", 1.0]]}

    actual = taar_locale.generate_dictionary(spark, 5)
    assert actual == expected
