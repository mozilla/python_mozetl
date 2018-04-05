#!/bin/env python

import click
import json
import logging
import logging.config
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from dateutil.parser import parse
import datetime

from taar_utils import read_from_s3, store_json_to_s3

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_PREFIX = 'telemetry-ml/addon_recommender/'

AMO_DUMP_BASE_FILENAME = 'extended_addons_database'
FILTERED_AMO_BASE_FILENAME = 'whitelist_addons_database'

AMO_DUMP_FILENAME = AMO_DUMP_BASE_FILENAME + '.json'
FILTERED_AMO_FILENAME = FILTERED_AMO_BASE_FILENAME + '.json'

MIN_RATING = 3.0
MIN_AGE = 60

logger = logging.getLogger('amo_whitelist')


class AMOTransformer:
    """
    This class transforms the raw AMO addon JSON dump
    by filtering out addons that do not meet the minimum requirements
    for 'whitelisted' addons.  See the documentation in the transform
    method for details.
    """
    def __init__(self, bucket, prefix, fname, min_rating, min_age):
        self._s3_bucket = bucket
        self._s3_prefix = prefix
        self._s3_fname = fname
        self._min_rating = min_rating
        self._min_age = min_age

    def extract(self):
        return read_from_s3(self._s3_fname, self._s3_prefix, self._s3_bucket)

    def transform(self, json_data):
        """
        We currently whitelist addons which meet a minimum critieria
        of:

        * At least 3.0 average rating or higher
        * At least 60 days old as computed using the
          'first_create_date' field in the addon JSON

        Criteria are discussed over at :
          https://github.com/mozilla/taar-lite/issues/1
        """

        latest_create_date = datetime.datetime.today() - datetime.timedelta(days=self._min_age)
        latest_create_date = latest_create_date.replace(tzinfo=None)

        new_data = {}
        for k in json_data.keys():
            rating = json_data[k]['ratings']['average']
            create_date = parse(json_data[k]['first_create_date']).replace(tzinfo=None)

            if rating >= self._min_rating and create_date <= latest_create_date:
                new_data[k] = json_data[k]

        return new_data

    def load(self, jdata):
        date = datetime.date.today().strftime("%Y%m%d")
        store_json_to_s3(json.dumps(jdata),
                         FILTERED_AMO_BASE_FILENAME,
                         date,
                         AMO_DUMP_PREFIX,
                         AMO_DUMP_BUCKET)


@click.command()
@click.option("--s3-prefix", default=AMO_DUMP_PREFIX)
@click.option("--s3-bucket", default=AMO_DUMP_BUCKET)
@click.option("--input_filename", default=AMO_DUMP_FILENAME)
@click.option("--min_rating", default=MIN_RATING)
@click.option("--min_age", default=MIN_AGE)
def main(s3_prefix, s3_bucket, input_filename, min_rating, min_age):
    etl = AMOTransformer(s3_bucket,
                         s3_prefix,
                         input_filename,
                         float(min_rating),
                         int(min_age))
    jdata = etl.extract()
    final_jdata = etl.transform(jdata)
    etl.load(final_jdata)

if __name__ == '__main__':
    main()
