#!/bin/env python

import click
import json
import logging
import logging.config
import typing
from six.moves import urllib
import queue as queue

from .taar_utils import store_json_to_s3

import requests
from requests_toolbelt.threaded import pool

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_PREFIX = 'telemetry-ml/addon_recommender/'
AMO_DUMP_FILENAME = 'extended_addons_database'

DEFAULT_AMO_REQUEST_URI = "https://addons.mozilla.org/api/v3/addons/search/"
QUERY_PARAMS = "?app=firefox&sort=created&type=extension"

logger = logging.getLogger('amo_database')


"""
JSON from addons.mozilla.org server are parsed using subclasses of
JSONSchema to declare the types that we want to extract.

The top level object that we are parsing is `AMOAddonInfo`.

All classes of type JSONSchema have a `meta` dictionary attribute
which define keys which we are interested in extracting.

The key is the name of the attribute which we are interested in
retrieving.

The value defines how we want to coerce the inbound data.  There are 3
general cases:
    1) subclasses of JSONSchema are nested objects which are
       represented as dictionaries
    2) List<T> or Dict<T,T> types where values are coerced
       recursively using the marshal function.
    3) Everything else. These are callable type defintions. Usually
       Python built-ins like str or bool. It is possible to define
       custom callables if you want to do custom data conversion.

"""


class JSONSchema:
    pass


class AMOAddonFile(JSONSchema):
    meta = {'id': int,
            'platform': str,
            'status': str,
            'is_webextension': bool}


class AMOAddonVersion(JSONSchema):
    meta = {'files': typing.List[AMOAddonFile]}


class AMOAddonInfo(JSONSchema):
    meta = {'guid': str,
            'categories': typing.Dict[str, typing.List[str]],
            'default_locale': str,
            'description': typing.Dict[str, str],
            'name': typing.Dict[str, str],
            'current_version': AMOAddonVersion,
            'ratings': typing.Dict[str, float],
            'summary': typing.Dict[str, str],
            'tags': typing.List[str],
            'weekly_downloads': int}


class AMODatabase:
    def __init__(self, worker_count):
        """
        Just setup the page_count
        """
        self._max_processes = worker_count

        uri = DEFAULT_AMO_REQUEST_URI + QUERY_PARAMS
        response = requests.get(uri)
        jdata = json.loads(response.content.decode('utf8'))

        self._page_count = jdata['page_count']

    def fetch_addons(self):
        addon_map = self._fetch_pages()
        self._fetch_versions(addon_map)
        final_result = {}
        for k, v in list(addon_map.items()):
            if 'first_create_date' in v:
                final_result[k] = v
        logger.info("Final addon set includes %d addons." % len(final_result))
        return final_result

    def _fetch_pages(self):
        addon_map = {}

        urls = []
        for i in range(1, self._page_count+1):
            url = "{0}{1}&page={2}".format(DEFAULT_AMO_REQUEST_URI, QUERY_PARAMS, i)
            urls.append(url)
        logger.info("Processing AMO urls")
        p = pool.Pool.from_urls(urls, num_processes=self._max_processes)
        p.join_all()

        self._handle_responses(p, addon_map)

        # Try failed requests
        exceptions = p.exceptions()
        p = pool.Pool.from_exceptions(exceptions, num_processes=self._max_processes)
        p.join_all()
        self._handle_responses(p, addon_map)
        return addon_map

    def _fetch_versions(self, addon_map):

        logger.info("Processing Version urls")

        q = queue.Queue()
        logger.info("Filling initial verson page queue")

        def iterFactory(guid_map):
            for guid in list(guid_map.keys()):
                yield "https://addons.mozilla.org/api/v3/addons/addon/%s/versions/" % guid

        def chunker(seq, size):
            collector = []
            for term in seq:
                collector.append(term)
                if len(collector) == size:
                    yield collector
                    collector = []

            # Yield any dangling records we collected
            if len(collector) > 0:
                yield collector

        total_processed_addons = 0
        for chunk in chunker(iterFactory(addon_map), 500):
            for i, url in enumerate(chunk):
                q.put({'method': 'GET',
                       'url': url,
                       'timeout': 2.0})
            logger.info("Queue setup - processing initial version page requests")
            logger.info("%d requests to process" % q.qsize())

            p = pool.Pool(q, num_processes=self._max_processes)
            p.join_all()
            logger.info("Pool completed - processing responses")
            last_page_urls = self._handle_version_responses(p)
            logger.info("Captured %d last page urls" % len(last_page_urls))
            total_processed_addons += len(last_page_urls)

            # Try processing the exceptions once
            p = pool.Pool.from_exceptions(p.exceptions(), num_processes=self._max_processes)
            p.join_all()
            last_page_urls.extend(self._handle_version_responses(p))

            # Now fetch the last version of each addon
            logger.info("Processing last page urls: %d" % len(last_page_urls))
            p = pool.Pool.from_urls(last_page_urls,
                                    num_processes=self._max_processes)
            p.join_all()

            self._handle_last_version_responses(p, addon_map)

            # Try processing exceptions once
            p = pool.Pool.from_exceptions(p.exceptions(), num_processes=self._max_processes)
            p.join_all()
            self._handle_last_version_responses(p, addon_map)

            logger.info("Processed %d addons with version info" %
                        total_processed_addons)

    def _handle_last_version_responses(self, p, addon_map):
        for i, resp in enumerate(p.responses()):
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']

                    raw_guid = resp.url.split("addon/")[1].split("/versions")[0]
                    guid = urllib.parse.unquote(raw_guid)
                    create_date = results[-1]['files'][0]['created']

                    record = addon_map.get(guid, None)
                    if record is not None:
                        record['first_create_date'] = create_date
            except Exception as e:
                # Skip this record
                logger.error(e)
        return addon_map

    def _handle_responses(self, p, addon_map):
        i = 0
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']
                    for record in results:
                        if i % 500 == 0:
                            logger.info("Still parsing  addons...")
                        guid = record['guid']
                        addon_map[guid] = record
                    i += 1
            except Exception as e:
                # Skip this record
                logger.error(e)

    def _handle_version_responses(self, p):
        page_urls = []
        for i, resp in enumerate(p.responses()):
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    page_count = int(jdata['page_count'])
                    if page_count > 1:
                        url = resp.url+"?page=%d" % page_count
                    else:
                        url = resp.url
                    page_urls.append(url)
            except Exception as e:
                # Skip this record
                logger.error(e)
        return page_urls


class Undefined:
    """
    This value is used to disambiguate None vs a non-existant value on
    dict.get() lookups
    """
    pass


def marshal(value, name, type_def):
    serializers = {typing.List: list,
                   typing.Dict: dict,
                   str: unicode,
                   unicode: unicode,
                   int: int,
                   float: float,
                   bool: bool}

    if issubclass(type_def, JSONSchema):
        obj = {}
        for attr_name, attr_type_def in list(type_def.meta.items()):
            attr_value = value.get(attr_name, Undefined)
            if attr_value is not Undefined:
                # Try marshalling the value
                obj[attr_name] = marshal(attr_value,
                                         attr_name,
                                         attr_type_def)
        return obj
    elif issubclass(type_def, typing.Container) and type_def not in [str, bytes]:
        if issubclass(type_def, typing.List):
            item_type = type_def.__args__[0]
            return [marshal(j, name, item_type) for j in value]
        elif issubclass(type_def, typing.Dict):
            if value is None:
                return None
            k_cast, v_cast = type_def.__args__
            dict_vals = [(marshal(k, name, k_cast),
                          marshal(v, name, v_cast))
                         for k, v in list(value.items())]
            return dict(dict_vals)
    else:
        return serializers[type_def](value)


@click.command()
@click.option("--date", required=True)
@click.option("--workers", default=100)
@click.option("--s3-prefix", default=AMO_DUMP_PREFIX)
@click.option("--s3-bucket", default=AMO_DUMP_BUCKET)
def main(date, workers, s3_prefix, s3_bucket):
    amodb = AMODatabase(int(workers))

    addon_map = amodb.fetch_addons()

    try:
        store_json_to_s3(json.dumps(addon_map),
                         AMO_DUMP_FILENAME,
                         date,
                         s3_prefix,
                         s3_bucket)
        logger.info("Completed uploading s3://%s/%s%s.json" % (s3_bucket,
                                                               s3_prefix,
                                                               AMO_DUMP_FILENAME))
    except Exception as e:
        logger.error("Error uploading data to S3", e)


if __name__ == '__main__':
    main()
