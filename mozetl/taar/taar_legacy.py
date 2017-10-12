"""
TAAR legacy addon replacements
This job pulls the latest curated substitute webestensions from the
AMO data sources. This ensures that TAAR recommendations for legacy
substitutions are consistent with AMO replacements.
Prototype notebook is here [1]

[1] https://gist.github.com/mlopatka/c6fe350dbcd8fe11ccf0929fc9f8c223
"""

import click
import json
import logging
import requests

from taar_utils import store_json_to_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AMO_LEGACY_RECS_URI =\
    "https://addons.mozilla.org/api/v3/addons/replacement-addon/"
EXPORT_FILE_NAME = 'legacy_substitutes.json'


def parse_from_amo_api(api_uri, storage_dict):
    while api_uri is not None:
        logger.info("Fetching " + api_uri)
        # Request the data and get it in JSON format.
        r = requests.get(api_uri)
        page_blob = r.json()
        # Re-map as {guid : [guid-1, guid-2, ... guid-n]}
        for addon in page_blob.get('results'):
            storage_dict[addon.get('guid')] = \
                addon.get('replacement')
        api_uri = page_blob.get('next')
    return storage_dict


def fetch_legacy_replacement_masterlist():
    # Set the initial URI to the REST API entry point.
    legacy_replacement_dict = {}
    legacy_replacement_dict = parse_from_amo_api(AMO_LEGACY_RECS_URI, legacy_replacement_dict)

    # Remove invalid and empty value in the dict.
    legacy_replacement_dict_valid = \
        {k: v for k, v in legacy_replacement_dict.items() if v}

    # Log any instance of removed (bad) data from the AMO API.
    if len(legacy_replacement_dict) > len(legacy_replacement_dict_valid):
        logger.info("Some invalid data purged from AMO provided JSON")

    return legacy_replacement_dict_valid


@click.command()
@click.option('--date', required=True)
@click.option('--bucket', default='telemetry-private-analysis-2')
@click.option('--prefix', default='taar/legacy/')
def main(date, bucket, prefix):
    logger.info("Retreiving AMO legacy addon replacements list")
    legacy_dict = fetch_legacy_replacement_masterlist()

    if len(legacy_dict) > 0:
        logger.info("Updating active legacy addon replacements list in s3")
        store_json_to_s3(json.dumps(legacy_dict, indent=2), EXPORT_FILE_NAME,
                         date, prefix, bucket)
    else:
        logger.info("EMPTY list retrieved from AMO legacy recs API")
