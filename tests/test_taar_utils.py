import boto3
import json
import pytest

from tempfile import NamedTemporaryFile
from moto import mock_s3
from mozetl.taar import taar_utils

SAMPLE_DATA = {"test": "data"}

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


@mock_s3
def test_read_from_s3():
    # Write a JSON blob
    bucket = "test-bucket"
    prefix = "test-prefix/"
    s3_json_fname = "test.json"

    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=bucket)

    with NamedTemporaryFile("w") as json_file:
        json.dump(SAMPLE_DATA, json_file)
        # Seek to the beginning of the file to allow the tested
        # function to find the file content.
        json_file.seek(0)
        # Upload the temp file to S3.
        taar_utils.write_to_s3(json_file.name, s3_json_fname, prefix, bucket)

    data = taar_utils.read_from_s3(s3_json_fname, prefix, bucket)
    assert data == SAMPLE_DATA


@mock_s3
def test_write_to_s3():
    bucket = "test-bucket"
    prefix = "test-prefix/"
    dest_filename = "test.json"

    conn = boto3.resource("s3", region_name="us-west-2")
    bucket_obj = conn.create_bucket(Bucket=bucket)

    with NamedTemporaryFile("w") as json_file:
        json.dump(SAMPLE_DATA, json_file)
        # Seek to the beginning of the file to allow the tested
        # function to find the file content.
        json_file.seek(0)
        # Upload the temp file to S3.
        taar_utils.write_to_s3(json_file.name, dest_filename, prefix, bucket)

    available_objects = list(bucket_obj.objects.filter(Prefix=prefix))
    assert len(available_objects) == 1

    # Check that our file is there.
    full_s3_name = "{}{}".format(prefix, dest_filename)
    keys = [o.key for o in available_objects]
    assert full_s3_name in keys

    stored_data = taar_utils.read_from_s3(dest_filename, prefix, bucket)
    assert SAMPLE_DATA == stored_data


@mock_s3
def test_write_json_s3():
    bucket = "test-bucket"
    prefix = "test-prefix/"
    base_filename = "test"

    content = {"it-IT": ["firefox@getpocket.com"]}

    conn = boto3.resource("s3", region_name="us-west-2")
    bucket_obj = conn.create_bucket(Bucket=bucket)

    # Store the data in the mocked bucket.
    taar_utils.store_json_to_s3(
        json.dumps(content), base_filename, "20171106", prefix, bucket
    )

    # Get the content of the bucket.
    available_objects = list(bucket_obj.objects.filter(Prefix=prefix))
    assert len(available_objects) == 2

    # Get the list of keys.
    keys = [o.key for o in available_objects]
    assert "{}{}.json".format(prefix, base_filename) in keys
    date_filename = "{}{}20171106.json".format(prefix, base_filename)
    assert date_filename in keys


@mock_s3
def test_load_amo_external_whitelist():
    conn = boto3.resource("s3", region_name="us-west-2")
    conn.create_bucket(Bucket=taar_utils.AMO_DUMP_BUCKET)

    # Make sure that whitelist loading fails before mocking the S3 file.
    EXCEPTION_MSG = "Empty AMO whitelist detected"
    with pytest.raises(RuntimeError) as excinfo:
        taar_utils.load_amo_external_whitelist()

    assert EXCEPTION_MSG in str(excinfo.value)

    # Store an empty file and verify that an exception is raised.
    conn.Object(taar_utils.AMO_DUMP_BUCKET, key=taar_utils.AMO_WHITELIST_KEY).put(
        Body=json.dumps({})
    )

    with pytest.raises(RuntimeError) as excinfo:
        taar_utils.load_amo_external_whitelist()

    assert EXCEPTION_MSG in str(excinfo.value)

    # Store the data in the mocked bucket.
    conn.Object(taar_utils.AMO_DUMP_BUCKET, key=taar_utils.AMO_WHITELIST_KEY).put(
        Body=json.dumps(FAKE_AMO_DUMP)
    )

    # Check that the web_extension item is still present
    # and the legacy addon is absent.
    whitelist = taar_utils.load_amo_external_whitelist()
    assert "this_guid_can_not_be_in_amo" not in whitelist

    # Verify that the legacy addon was removed while the
    # web_extension compatible addon is still present.
    assert "test-guid-0001" in whitelist
    assert "test-guid-0002" not in whitelist


def test_telemetry_hash():
    """
    A JS snippet that will run in the Browser Toolbox is:

    let byteArr = new TextEncoder().encode("33c5c416-c57d-4eb7-bf58-beaf97a40332")
    const CryptoHash = Components.Constructor("@mozilla.org/security/hash;1",
                                              "nsICryptoHash",
                                              "initWithString");
    let hash = new CryptoHash("sha256");
    hash.update(byteArr, byteArr.length);
    let clientId = CommonUtils.bytesAsHex(hash.finish(false));
    "54e760dc799b24c6edc1a02b200db9b07d51a96b7dc7d4ebcd1d86ee8728f420"
    """

    uuid = "33c5c416-c57d-4eb7-bf58-beaf97a40332"
    hashed_id = taar_utils.hash_telemetry_id(uuid)
    assert (
        hashed_id == "54e760dc799b24c6edc1a02b200db9b07d51a96b7dc7d4ebcd1d86ee8728f420"
    )
