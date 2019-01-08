"""Test suite to exercise the creation of the editorial reviewed
whitelist of addons for whitelist Job."""

import json

from moto import mock_s3
import pytest
import boto3
from mozetl.taar import taar_update_whitelist

# from mozetl.taar import taar_utils

EDITORIAL_URL = "https://addons.mozilla.org/api/v4/discovery/editorial/"


@pytest.fixture
def mock_extract():
    jdata = {
        "results": [
            {
                "addon": {"guid": "guid1"},
                "custom_heading": "heading1",
                "custom_description": "description1",
            },
            {
                "addon": {"guid": "guid2"},
                "custom_heading": "heading2",
                "custom_description": "description2",
            },
        ]
    }

    return jdata


@pytest.fixture
def mock_transformed_data():
    return ["guid1", "guid2"]


def test_transform(mock_extract, mock_transformed_data):
    jdata = taar_update_whitelist.parse_json(mock_extract)
    assert jdata == mock_transformed_data


@mock_s3
def test_load(mock_transformed_data):
    bucket = "telemetry-parquet"
    prefix = "taar/locale/"
    date = "20190105"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    taar_update_whitelist.load_etl(mock_transformed_data, date, prefix, bucket)

    obj = conn.Object(bucket, key=prefix + "only_guids_top_200.json").get()
    json_txt = obj["Body"].read()
    assert sorted(json.loads(json_txt)) == mock_transformed_data
