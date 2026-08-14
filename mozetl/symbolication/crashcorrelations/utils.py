import os
import errno
import json
import gzip
import shutil
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def utc_today():
    return datetime.utcnow().date()


def get_day(days):
    return utc_today() - timedelta(days)


def get_with_retries(url, params=None, headers=None):
    retries = Retry(total=16, backoff_factor=1, status_forcelist=[429])

    s = requests.Session()
    http_adapter = HTTPAdapter(max_retries=retries)
    s.mount("https://", http_adapter)

    return s.get(url, params=params, headers=headers)


def query_searchfox(q):
    r = get_with_retries('https://searchfox.org/mozilla-central/search', params={
        'q': q,
        'limit': 1000
    }, headers={
        'Accept': 'application/json'
    })

    if r.status_code != 200:
        print(r.text)
        raise Exception(r)

    return sum((result for result in r.json()["normal"].values()), [])


def mkdir(path):
    try:
        os.mkdir(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e


def rmdir(path):
    try:
        shutil.rmtree(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e


def write_json(path, obj):
    with gzip.open(path, 'wt') as f:
        json.dump(obj, f)
