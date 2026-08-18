import os
import errno
import json
import gzip
import shutil
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import boto3


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


def upload_results(job_name, directory):
    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)

    for root, dirs, files in os.walk(directory):
        for name in files:
            full_path = os.path.join(root, name)

            transfer.upload_file(full_path, 'telemetry-public-analysis-2', '{}/data/{}'.format(job_name, full_path[len(directory) + 1:]), extra_args={'ContentType': 'application/json', 'ContentEncoding': 'gzip'})


def remove_results(job_name):
    bucket = boto3.resource('s3').Bucket('telemetry-public-analysis-2')

    for key in bucket.objects.filter(Prefix=job_name + '/data/'):
        key.delete()
