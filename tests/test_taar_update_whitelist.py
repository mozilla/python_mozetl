"""Test suite to exercise the creation of the editorial reviewed
whitelist of addons for whitelist Job."""

import json

from moto import mock_s3
import pytest
import boto3
from mozetl.taar import taar_update_whitelist
from mozetl.taar.taar_update_whitelist import LoadError, ShortWhitelistError
import mock


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


# This method will be used by the mock to replace requests.get
def mocked_requests_get_404(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(None, 404)


@mock.patch("requests.get", side_effect=mocked_requests_get_404)
def test_amo_network_failure(mock_get):
    with pytest.raises(LoadError) as err:
        taar_update_whitelist.load_amo_editorial("http://some/editorial/url")  # noqa
    assert (
        err.value.args[0] == "HTTP 404 status loading JSON from AMO editorial endpoint."
    )


def test_transform(mock_extract, mock_transformed_data):
    jdata = taar_update_whitelist.parse_json(mock_extract, True, True)
    assert jdata == mock_transformed_data


def test_transform_with_short_guidlist_error(mock_extract, mock_transformed_data):
    with pytest.raises(ShortWhitelistError):
        taar_update_whitelist.parse_json(mock_extract, False)


@mock.patch("requests.get", side_effect=mocked_requests_get_404)
def test_transform_failed_guid_validation(
    mock_get, mock_extract, mock_transformed_data
):
    # test that we can trigger a GUIDError and the json parse will
    # immediately fail
    with pytest.raises(taar_update_whitelist.GUIDError):
        taar_update_whitelist.parse_json(mock_extract, True, True)


@mock_s3
def test_load(mock_transformed_data):
    bucket = "telemetry-parquet"
    prefix = "taar/locale/"
    date = "20190105"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(
        Bucket=bucket,
        CreateBucketConfiguration={
            "LocationConstraint": "us-west-2",
        },
    )

    taar_update_whitelist.load_etl(mock_transformed_data, date, prefix, bucket)

    obj = conn.Object(bucket, key=prefix + "only_guids_top_200.json").get()
    json_txt = obj["Body"].read()
    if type(json_txt) == bytes:
        json_txt = json_txt.decode("utf8")
    assert sorted(json.loads(json_txt)) == mock_transformed_data


def test_validate_row():

    r = {}
    assert not taar_update_whitelist.validate_row(r)

    r = {"addon": {}}
    assert not taar_update_whitelist.validate_row(r)

    r = {"addon": {"guid": "foo"}}
    assert taar_update_whitelist.validate_row(r)


#     run a test of the command cli
#     using the CliRunner, verify transformed data in the same way as
#     test_load using mock_s3
