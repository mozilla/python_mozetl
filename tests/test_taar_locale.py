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
        "firefox@getpocket.com": {
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

fake_amo_sample_web_extension = {
    'test-addon-key':
        {'ratings':
             {'bayesian_average': 0.0,
              'count': 0.0,
              'average': 0.0},
         'name':
             {'en-US': 'test-amo-entry-1'},
         'default_locale': 'en-US',
         'tags': [
             'test-tag-1',
             'test-tag-2',
             'test-tag-3'],
         'summary': {
             'en-US':
                 'helps test taar_locale moz_etl job'},
         'current_version': {
             'files': [
                 {'status': 'public',
                  'platform': 'all',
                  'id': 000001,
                  'is_webextension': True}]},
         'guid': 'test-guid-0001'}
}

fake_amo_sample_legacy = {
    'test-addon-key':
        {'ratings':
             {'bayesian_average': 0.0,
              'count': 0.0,
              'average': 0.0},
         'name':
             {'en-US': 'test-amo-entry-2'},
         'default_locale': 'en-US',
         'tags': [
             'test-tag-1',
             'test-tag-2',
             'test-tag-3'],
         'summary': {
             'en-US':
                 'helps test taar_locale moz_etl job'},
         'current_version': {
             'files': [
                 {'status': 'public',
                  'platform': 'all',
                  'id': 000002,
                  'is_webextension': False}]},
         'guid': 'test-guid-0002'}
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


def test_generate_dictionary(spark, multi_locales_df):
    multi_locales_df.createOrReplaceTempView("longitudinal")

    # The "en-US" locale must not be reported: we set it to a low
    # frequency on |multi_locale_df|.
    expected = {
        "it-IT": ["firefox@getpocket.com"]
    }

    assert taar_locale.generate_dictionary(spark, 5) == expected


@mock_s3
def test_write_output():
    bucket = 'test-bucket'
    prefix = 'test-prefix/'

    content = {
        "it-IT": ["firefox@getpocket.com"]
    }

    conn = boto3.resource('s3', region_name='us-west-2')
    bucket_obj = conn.create_bucket(Bucket=bucket)

    # Store the data in the mocked bucket.
    taar_locale.store(content, '20171106', prefix, bucket)

    # Get the content of the bucket.
    available_objects = list(bucket_obj.objects.filter(Prefix=prefix))
    assert len(available_objects) == 2

    # Get the list of keys.
    keys = [o.key for o in available_objects]
    assert "{}{}.json".format(prefix, taar_locale.LOCALE_FILE_NAME) in keys
    date_filename =\
        "{}{}20171106.json".format(prefix, taar_locale.LOCALE_FILE_NAME)
    assert date_filename in keys


@mock_s3
def test_load_amo_external_whitelist():
    amo_test = taar_locale.load_amo_external_whitelist()
    bucket = 'test-bucket'
    prefix = 'test-prefix/'

    content = [fake_amo_sample_web_extension, fake_amo_sample_legacy]

    conn = boto3.resource('s3', region_name='us-west-2')
    bucket_obj = conn.create_bucket(Bucket=bucket)

    # Store the data in the mocked bucket.
    taar_locale.store(json.dumps(content), prefix, bucket)

    # Get the content of the bucket.
    available_objects = list(bucket_obj.objects.filter(Prefix=prefix))
    assert len(available_objects) == 2

    # Check that the web_extension item is still present and the legacy addon is absent.
    white_listed_addons = taar_locale.load_amo_external_whitelist()
    assert 'test-guid-0002' in white_listed_addons
    assert 'test-guid-0001' not in white_listed_addons
