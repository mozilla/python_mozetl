# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import json
import requests
from .taar_utils import store_json_to_s3


class LoadError(Exception):
    pass


class ShortWhitelistError(Exception):
    pass


WHITELIST_FILENAME = "only_guids_top_200"
ADDON_META_URI = "https://addons.mozilla.org/api/v3/addons/search/?app=firefox&sort=created&type=extension&guid={}"  # noqa
EDITORIAL_URI = "https://addons.mozilla.org/api/v4/discovery/editorial/"


class GUIDError(BaseException):
    pass


def load_amo_editorial(url, only_recommended=True):
    param_dict = {}

    if only_recommended:
        param_dict["recommended"] = "true"

    r = requests.get(url, params=param_dict)
    if 200 == r.status_code:
        # process stuff here
        json_data = json.loads(r.text)
        return json_data

    err_msg = "HTTP {} status loading JSON from AMO editorial endpoint.".format(
        r.status_code
    )
    raise LoadError(err_msg)


def validate_row(row):
    guid = row.get("addon", {}).get("guid", None)
    return guid not in (None, "null", "")


def check_guid(guid):
    full_uri = ADDON_META_URI.format(guid)
    r = requests.get(full_uri)
    return r.status_code == 200


def parse_json(json_data, allow_short_guidlist, validate_guids=False):
    guids = {row["addon"]["guid"] for row in json_data["results"] if validate_row(row)}

    if validate_guids:
        for guid in guids:
            if not check_guid(guid):
                raise GUIDError("Can't validate GUID: {}".format(guid))
    result = sorted(list(guids))

    if not allow_short_guidlist and len(result) < 100:
        raise ShortWhitelistError(
            "Only obtained {} editorial reviewed addons.".format(len(result))
        )
    return result


def load_etl(transformed_data, date, prefix, bucket):
    store_json_to_s3(
        json.dumps(transformed_data, indent=2), WHITELIST_FILENAME, date, prefix, bucket
    )


@click.command()
@click.option("--date", required=True)
@click.option("--url", default=EDITORIAL_URI)
@click.option("--only-recommended", default=True)
@click.option("--bucket", default="telemetry-parquet")
@click.option("--prefix", default="taar/locale/")
@click.option("--validate-guid", default=False)
@click.option("--allow-shortlist", default=True)
def main(date, url, only_recommended, bucket, prefix, validate_guid, allow_shortlist):
    data_extract = load_amo_editorial(url, only_recommended)
    jdata = parse_json(data_extract, allow_shortlist, validate_guid)
    load_etl(jdata, date, prefix, bucket)
