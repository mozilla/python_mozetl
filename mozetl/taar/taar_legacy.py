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


def fetch_legacy_replacement_masterlist():
    # Set the initial URI to the REST API entry point.
    request_uri = AMO_LEGACY_RECS_URI

    # Put all the mappings in this dictionary, keyed on legacy guid.
    legacy_replacement_dict = {}

    while request_uri is not None:
        print("Fetching {}\n".format(request_uri))
        # Request the data and get it in JSON format.
        r = requests.get(request_uri)
        page_blob = r.json()
        # Re map as {guid : [guid-1, guid-2,... guid-n]}
        for addon in page_blob.get('results'):
            legacy_replacement_dict[addon.get('guid')] =\
                addon.get('replacement')

        request_uri = page_blob.get('next')

    return legacy_replacement_dict


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
