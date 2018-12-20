#!/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import json
import logging
import logging.config
from abc import abstractmethod

from dateutil.parser import parse
import datetime

from .taar_utils import read_from_s3, store_json_to_s3

AMO_DUMP_BUCKET = "telemetry-parquet"
AMO_DUMP_PREFIX = "telemetry-ml/addon_recommender/"

# Input file
AMO_DUMP_BASE_FILENAME = "extended_addons_database"
AMO_DUMP_FILENAME = AMO_DUMP_BASE_FILENAME + ".json"

# Output files
FILTERED_AMO_BASE_FILENAME = "whitelist_addons_database"
FEATURED_BASE_FILENAME = "featured_addons_database"
FEATURED_WHITELIST_BASE_FILENAME = "featured_whitelist_addons"

FILTERED_AMO_FILENAME = FILTERED_AMO_BASE_FILENAME + ".json"
FEATURED_FILENAME = FEATURED_BASE_FILENAME + ".json"
FEATURED_WHITELIST_FILENAME = FEATURED_WHITELIST_BASE_FILENAME + ".json"

MIN_RATING = 3.0
MIN_AGE = 60

logger = logging.getLogger("amo_whitelist")


class AbstractAccumulator:
    def __init__(self):
        self._results = {}

    @abstractmethod
    def process_record(self, guid, addon_data):
        pass

    def get_results(self):
        return self._results


class FeaturedAccumulator(AbstractAccumulator):
    def __init__(self):
        AbstractAccumulator.__init__(self)

    def process_record(self, guid, addon_data):

        featured = addon_data.get("is_featured", False)

        if featured:
            self._results[guid] = addon_data


class WhitelistAccumulator(AbstractAccumulator):
    def __init__(self, min_age, min_rating):
        AbstractAccumulator.__init__(self)

        self._min_age = min_age
        self._min_rating = min_rating

        self._latest_create_date = datetime.datetime.today() - datetime.timedelta(
            days=self._min_age
        )
        self._latest_create_date = self._latest_create_date.replace(tzinfo=None)

    def process_record(self, guid, addon_data):
        if guid == "pioneer-opt-in@mozilla.org":
            # Firefox Pioneer is explicitly excluded
            return

        current_version_files = addon_data.get("current_version", {}).get("files", [])
        if len(current_version_files) == 0:
            # Only allow addons that files in the latest version.
            # Yes - that's as weird as it sounds.  Sometimes addons
            # have no files.
            return

        if current_version_files[0].get("is_webextension", False) is False:
            # Only allow webextensions
            return

        rating = addon_data.get("ratings", {}).get("average", 0)
        create_date = parse(addon_data.get("first_create_date", None)).replace(
            tzinfo=None
        )

        if rating >= self._min_rating and create_date <= self._latest_create_date:
            self._results[guid] = addon_data


class WhitelistFeaturedAccumulator(WhitelistAccumulator):
    def __init__(self, min_age, min_rating):
        WhitelistAccumulator.__init__(self, min_age, min_rating)

    def process_record(self, guid, addon_data):
        if not addon_data.get("is_featured", False):
            return
        return WhitelistAccumulator.process_record(self, guid, addon_data)

    def get_results(self):
        return WhitelistAccumulator.get_results(self)


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

        self._accumulators = {
            "whitelist": WhitelistAccumulator(self._min_age, self._min_rating),
            "featured": FeaturedAccumulator(),
            "featured_whitelist": WhitelistFeaturedAccumulator(
                self._min_age, self._min_rating
            ),
        }

    def extract(self):
        return read_from_s3(self._s3_fname, self._s3_prefix, self._s3_bucket)

    def transform(self, json_data):
        """
        We currently whitelist addons which meet a minimum critieria
        of:

        * At least 3.0 average rating or higher
        * At least 60 days old as computed using the
          'first_create_date' field in the addon JSON
        * Not the Firefox Pioneer addon

        Criteria are discussed over at :
          https://github.com/mozilla/taar-lite/issues/1
        """

        for guid, addon_data in list(json_data.items()):
            for acc in list(self._accumulators.values()):
                acc.process_record(guid, addon_data)

        return self.get_whitelist()

    def get_featuredlist(self):
        return self._accumulators["featured"].get_results()

    def get_featuredwhitelist(self):
        return self._accumulators["featured_whitelist"].get_results()

    def get_whitelist(self):
        return self._accumulators["whitelist"].get_results()

    def _load_s3_data(self, jdata, fname):
        date = datetime.date.today().strftime("%Y%m%d")
        store_json_to_s3(
            json.dumps(jdata), fname, date, AMO_DUMP_PREFIX, AMO_DUMP_BUCKET
        )

    def load_whitelist(self, jdata):
        self._load_s3_data(jdata, FILTERED_AMO_BASE_FILENAME)

    def load_featuredlist(self, jdata):
        self._load_s3_data(jdata, FEATURED_BASE_FILENAME)

    def load_featuredwhitelist(self, jdata):
        self._load_s3_data(jdata, FEATURED_WHITELIST_BASE_FILENAME)

    def load(self):
        self.load_whitelist(self.get_whitelist())
        self.load_featuredlist(self.get_featuredlist())
        self.load_featuredwhitelist(self.get_featuredwhitelist())


@click.command()
@click.option("--s3-prefix", default=AMO_DUMP_PREFIX)
@click.option("--s3-bucket", default=AMO_DUMP_BUCKET)
@click.option("--input_filename", default=AMO_DUMP_FILENAME)
@click.option("--min_rating", default=MIN_RATING)
@click.option("--min_age", default=MIN_AGE)
def main(s3_prefix, s3_bucket, input_filename, min_rating, min_age):
    etl = AMOTransformer(
        s3_bucket, s3_prefix, input_filename, float(min_rating), int(min_age)
    )
    jdata = etl.extract()
    etl.transform(jdata)
    etl.load()


if __name__ == "__main__":
    main()
