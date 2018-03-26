#!/bin/env python

from contextlib import contextmanager
import click
import json
import logging
import logging.config
import os
import shutil
import sys
import tempfile
import typing
import urllib
import Queue as queue

from taar_utils import store_json_to_s3

import requests
from requests_toolbelt.threaded import pool

AMO_DUMP_BUCKET = 'telemetry-parquet'
AMO_DUMP_PREFIX = 'telemetry-ml/addon_recommender/'
AMO_DUMP_FILENAME = 'extended_addons_database'

DEFAULT_AMO_REQUEST_URI = "https://addons.mozilla.org/api/v3/addons/search/"
QUERY_PARAMS = "?app=firefox&sort=created&type=extension"

logger = logging.getLogger('amo_database')


@contextmanager
def guid_cache(fpath):
    """
    This contextmanager cache uses the filesystem to store
    JSON blobs that contain a 'guid' key.  Each file is stored simply
    with the GUID as the filename and '.json' as the extension.
    """
    class Cache:
        def __init__(self, fpath):
            self._fpath = fpath
            shutil.rmtree(self._fpath, ignore_errors=True)
            os.makedirs(self._fpath)

        def put(self, data):
            guid = data['guid']
            fname = os.path.join(self._fpath, "%s.json" % guid)
            with open(fname, 'w') as fout:
                fout.write(json.dumps(data))
    yield Cache(fpath)


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
    meta = {
            'id': int,
            'platform': str,
            'status': str,
            'is_webextension': bool
    }


class AMOAddonVersion(JSONSchema):
    meta = {
            'files': typing.List[AMOAddonFile]
    }


class AMOAddonInfo(JSONSchema):
    meta = {
            'guid': str,
            'categories': typing.Dict[str, typing.List[str]],
            'default_locale': str,
            'description': typing.Dict[str, str],
            'name': typing.Dict[str, str],
            'current_version': AMOAddonVersion,
            'ratings': typing.Dict[str, float],
            'summary': typing.Dict[str, str],
            'tags': typing.List[str],
            'weekly_downloads': int
    }


class URLIteratorFactory(object):
    BATCH_SIZE = 500

    def __init__(self, cache_dir):
        self._fnames = os.listdir(cache_dir)
        logger.info("Processing %d GUIDs" % len(self._fnames))

    def generatorFactory(self):
        data_slice, self._fnames = self._fnames[:self.BATCH_SIZE], self._fnames[self.BATCH_SIZE:]
        logger.info("%d in current batch.  %d remaining URLs" % (len(data_slice), len(self._fnames)))
        for i, fname in enumerate(data_slice):
            guid = fname.split(".json")[0]
            url = "https://addons.mozilla.org/api/v3/addons/addon/%s/versions/" % guid
            yield url


class AMODatabase:
    def __init__(self, root_path, worker_count):
        """
        Just setup the page_count
        """

        self._root_path = root_path
        logger.info("Writing temp files to : %s" % self._root_path)
        self._max_processes = worker_count

        self._amo_cache_dir = os.path.join(self._root_path, "amo_cache")
        self._amo_date_cache_dir = os.path.join(self._root_path, "amo_cache_dates")

        uri = DEFAULT_AMO_REQUEST_URI + QUERY_PARAMS
        response = requests.get(uri)
        jdata = json.loads(response.content.decode('utf8'))

        self._page_count = jdata['page_count']

    def fetch_pages(self):
        with guid_cache(self._amo_cache_dir) as db:
            urls = []
            for i in range(1, self._page_count+1):
                url = "%s%s%s" % (DEFAULT_AMO_REQUEST_URI, QUERY_PARAMS, ("&page=%d" % i))
                urls.append(url)
            logger.info("Processing AMO urls")
            p = pool.Pool.from_urls(urls, num_processes=self._max_processes)
            p.join_all()

            self._handle_responses(p, db)

            # Try failed requests
            exceptions = p.exceptions()
            p = pool.Pool.from_exceptions(exceptions, num_processes=self._max_processes)
            p.join_all()
            self._handle_responses(p, db)

    def parse_file(self, guid):
        try:
            tmpl = os.path.join(self._amo_cache_dir, '%s.json')
            jbdata = open(tmpl % guid, 'rb').read()
            data = json.loads(jbdata.decode('utf8'))
            result = marshal(data, None, AMOAddonInfo)
        except Exception as exc:
            logger.error("Error parsing addon json. GUID: %s, %s" % (guid, str(exc)))
            return None

        try:
            tmpl = os.path.join(self._amo_date_cache_dir, '%s.json')
            jbdata = open(tmpl % guid, 'rb').read()
            data = json.loads(jbdata.decode('utf8'))
            result['first_create_date'] = data['create_date']
            return result
        except:
            logger.warn("Error parsing version json. GUID: %s" % guid)
            return None

    def fetch_versions(self):

        with guid_cache(self._amo_date_cache_dir) as date_db:
            logger.info("Processing Version urls")

            q = queue.Queue()
            logger.info("Filling initial verson page queue")

            iterFactory = URLIteratorFactory(self._amo_cache_dir)
            while True:
                url_gen = iterFactory.generatorFactory()
                for i, url in enumerate(url_gen):
                    q.put({'method': 'GET',
                           'url': url,
                           'timeout': 2.0})
                logger.info("Queue setup - processing initial version page requests")
                logger.info("%d requests to process" % q.qsize())
                if q.qsize() == 0:
                    logger.info("Finished processing version URLs")
                    break
                p = pool.Pool(q, num_processes=self._max_processes)
                p.join_all()
                logger.info("Pool completed - processing responses")
                last_page_urls = self._handle_version_responses(p)
                logger.info("Captured %d last page urls" % len(last_page_urls))

                # Try processing the exceptions once
                p = pool.Pool.from_exceptions(p.exceptions(), num_processes=self._max_processes)
                p.join_all()
                last_page_urls.extend(self._handle_version_responses(p))

                # Now fetch the last version of each addon
                logger.info("Processing Last page urls: %d" % len(last_page_urls))
                p = pool.Pool.from_urls(last_page_urls,
                                        num_processes=self._max_processes)
                p.join_all()

                logger.info("Writing create dates")
                last_versions = self._handle_last_version_responses(p, date_db)

                # Try processing exceptions once
                p = pool.Pool.from_exceptions(p.exceptions(), num_processes=self._max_processes)
                p.join_all()
                last_versions.extend(self._handle_last_version_responses(p, date_db))

    def _handle_last_version_responses(self, p, db):
        guids = []
        for i, resp in enumerate(p.responses()):
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']

                    raw_guid = resp.url.split("addon/")[1].split("/versions")[0]
                    guid = urllib.unquote(raw_guid)
                    create_date = results[-1]['files'][0]['created']

                    record = {'guid': guid, 'create_date': create_date}
                    db.put(record)
                    guids.append(guid)
            except Exception as e:
                # Skip this record
                logger.error(e)
        return guids

    def _handle_responses(self, p, db):
        guids = []
        for resp in p.responses():
            try:
                if resp.status_code == 200:
                    jdata = json.loads(resp.content.decode('utf8'))
                    results = jdata['results']
                    for record in results:
                        guid = record['guid']
                        guids.append(guid)
                        db.put(record)
            except Exception as e:
                # Skip this record
                logger.error(e)
        return guids

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
        for attr_name, attr_type_def in type_def.meta.items():
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
                         for k, v in value.items()]
            return dict(dict_vals)
    else:
        return serializers[type_def](value)


@click.command()
@click.option("--path", default=tempfile.mkdtemp())
@click.option("--date", required=True)
@click.option("--workers", default=100)
@click.option("--s3-prefix", default=AMO_DUMP_PREFIX)
@click.option("--s3-bucket", default=AMO_DUMP_BUCKET)
def main(path, date, workers, s3_prefix, s3_bucket):
    amodb = AMODatabase(path, int(workers))
    amodb.fetch_pages()
    amodb.fetch_versions()
    try:
        large_blob = {}

        successful = 0
        for i, fname in enumerate(os.listdir(amodb._amo_cache_dir)):
            guid = fname.split(".json")[0]
            client_blob = amodb.parse_file(guid)

            if client_blob is None:
                continue

            large_blob[guid] = client_blob
            successful += 1

            if i > 0 and i % 200 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()

        fail_rate = 1.0 - (successful * 1.0 / len(os.listdir(amodb._amo_cache_dir)))
        if fail_rate >= 0.01:
            logger.error("Unexpectedly high failure rate for merging versions with addon JSON.  %0.4f" % (fail_rate))
            sys.exit(1)
        else:
            logger.info("Version merging failure rate: %0.4f" % fail_rate)

        store_json_to_s3(json.dumps(large_blob),
                         AMO_DUMP_FILENAME,
                         date,
                         s3_prefix,
                         s3_bucket)
    finally:
        logger.info("Cleaning up and removing : %s" % path)
        shutil.rmtree(path, ignore_errors=True)


if __name__ == '__main__':
    main()
