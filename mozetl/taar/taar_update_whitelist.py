# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import json
import requests
from taar_utils import store_json_to_s3


WHITELIST_FILENAME = "only_guids_top_200"
ADDON_META_URI = "https://addons.mozilla.org/api/v3/addons/search/?app=firefox&sort=created&type=extension&guid={}"  # noqa
EDITORIAL_URI = "https://addons.mozilla.org/api/v4/discovery/editorial/"


class GUIDError(BaseException):
    pass


def load_raw_json(url):
    r = requests.get(url)
    if 200 == r.status_code:
        # process stuff here
        json_data = json.loads(r.text)
        return json_data


def validate_row(row):
    if "addon" not in row:
        return False
    meta = row["addon"]
    if "guid" not in meta:
        return False
    if meta["guid"] in (None, "null", ""):
        return False
    return True


def check_guid(guid):
    full_uri = ADDON_META_URI.format(guid)
    r = requests.get(full_uri)
    return r.status_code == 200


def parse_json(json_data, validate_guids=False):
    guids = set()
    for row in json_data["results"]:
        if not validate_row(row):
            continue
        guids.add(row["addon"]["guid"])

    if validate_guids:
        for guid in guids:
            if not check_guid(guid):
                raise GUIDError("Can't validate GUID: {}".format(guid))
    return list(guids)


def load_etl(transformed_data, date, prefix, bucket):
    store_json_to_s3(
        json.dumps(transformed_data, indent=2), WHITELIST_FILENAME, date, prefix, bucket
    )


@click.command()
@click.option("--date", required=True)
@click.option("--url", default=EDITORIAL_URI)
@click.option("--bucket", default="telemetry-parquet")
@click.option("--prefix", default="taar/locale/")
def main(date, url, bucket, prefix):

    data_extract = load_raw_json(url)

    jdata = parse_json(data_extract, False)
    load_etl(jdata, date, prefix, bucket)
