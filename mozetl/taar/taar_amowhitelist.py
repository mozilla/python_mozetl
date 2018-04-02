#!/bin/env python

import click
import json
import logging
import logging.config
from dateutil.parser import parse
from tempfile import NamedTemporaryFile
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
    def __init__(self, bucket, prefix, fname, min_rating, min_age):
        self._s3_bucket = bucket
        self._s3_prefix = prefix
        self._s3_fname = fname
        self._min_rating = min_rating
        self._min_age = min_age

    def extract(self):
        with NamedTemporaryFile() as dst_json_file:
            read_from_s3(dst_json_file.name,
                         self._s3_fname,
                         self._s3_prefix,
                         self._s3_bucket)

            data = open(dst_json_file.name).read()
            return json.loads(data)

    def transform(self, json_data):
        age = datetime.datetime.today() - datetime.timedelta(days=self._min_age)
        age = age.replace(tzinfo=None)

        new_data = {}
        for k in json_data.keys():
            rating = json_data[k]['ratings']['average']
            create_date = parse(json_data[k]['first_create_date']).replace(tzinfo=None)

            if rating >= self._min_rating and create_date < age:
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
@click.option("--s3-filename", default=AMO_DUMP_FILENAME)
@click.option("--min_rating", default=MIN_RATING)
@click.option("--min_age", default=MIN_AGE)
def main(s3_prefix, s3_bucket, s3_filename, min_rating, min_age):
    etl = AMOTransformer(s3_bucket,
                         s3_prefix,
                         s3_filename,
                         float(min_rating),
                         int(min_age))
    jdata = etl.extract()
    final_jdata = etl.transform(jdata)
    etl.load(final_jdata)

if __name__ == '__main__':
    main()
