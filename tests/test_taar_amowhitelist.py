import boto3
import json
import pytest
from mozetl.taar import taar_amowhitelist
from mozetl.taar import taar_utils
from moto import mock_s3
from dateutil.parser import parse
import datetime

# This SAMPLE_DATA blob is a copy of some sample data that was
# extracted from the AMO JSON API.
SAMPLE_DATA = {
    "gnome-download-notify@ion201": {
        "categories": {
            "firefox": [
                "alerts-updates"
            ]
        },
        "current_version": {
            "files": [
                {
                    "id": 844358,
                    "is_webextension": True,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "en-US",
        "description": {
            "en-US": "Native notification integration for completed downloads."
        },
        "first_create_date": "2015-06-20T11:21:58Z",
        "guid": "gnome-download-notify@ion201",
        "name": {
            "en-US": "Download Notifications"
        },
        "ratings": {
            "average": 5.0,
            "bayesian_average": 4.58373,
            "count": 11.0,
            "text_count": 6.0
        },
        "summary": {
            "en-US": "Native notification integration for completed downloads."
        },
        "tags": [
            "cinnamon",
            "download",
            "firefox57",
            "gnome",
            "kde",
            "libnotify",
            "linux",
            "mate",
            "notification",
            "notify-send",
            "unity",
            "xfce"
        ],
        "weekly_downloads": 105
    },
    "jid0-ujiop74nNc447DlWVPSGIlzMRqY@jetpack": {
        "categories": {},
        "current_version": {
            "files": [
                {
                    "id": 149005,
                    "is_webextension": False,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "en-US",
        "description": None,
        "first_create_date": "2012-04-13T08:55:39Z",
        "guid": "jid0-ujiop74nNc447DlWVPSGIlzMRqY@jetpack",
        "name": {
            "en-US": "Luffi"
        },
        "ratings": {
            "average": 1.0,
            "bayesian_average": 0.698215,
            "count": 1.0,
            "text_count": 1.0
        },
        "summary": {
            "en-US": "Luffi - Let\u00b4s use freeware! fun included:)"
        },
        "tags": [],
        "weekly_downloads": 0
    },
    "jid1-tpeRu7ABM810Fw@jetpack": {
        "categories": {
            "firefox": [
                "photos-music-videos"
            ]
        },
        "current_version": {
            "files": [
                {
                    "id": 216360,
                    "is_webextension": False,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "zh-CN",
        "description": None,
        "first_create_date": "2011-10-13T04:55:06Z",
        "guid": "jid1-tpeRu7ABM810Fw@jetpack",
        "name": {
            "zh-CN": "\u8c46\u74e3\u7535\u53f0OSD Lyrics\u63d2\u4ef6"
        },
        "ratings": {
            "average": 3.0,
            "bayesian_average": 2.38892,
            "count": 3.0,
            "text_count": 0.0
        },
        "summary": {
            "zh-CN": "\u4f7f\u7528OSD Lyrics\u663e\u793a\u8c46\u74e3\u7535\u53f0\u6b4c\u8bcd"
        },
        "tags": [
            "douban",
            "osd-lyrics",
            "osdlyrics",
            "\u8c46\u74e3"
        ],
        "weekly_downloads": 1
    },
    "nellyfurtado@browsernation.com": {
        "categories": {
            "firefox": [
                "feeds-news-blogging"
            ]
        },
        "current_version": {
            "files": [
                {
                    "id": 114512,
                    "is_webextension": False,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "en-US",
        "description": {
            "en-US": "What am I downloading? An .xpi file that only takes seconds to install."
        },
        "first_create_date": "2010-11-03T09:38:20Z",
        "guid": "nellyfurtado@browsernation.com",
        "name": {
            "en-US": "The Official Nelly Furtado Add-on for Firefox"
        },
        "ratings": {
            "average": 0.0,
            "bayesian_average": 0.0,
            "count": 0.0,
            "text_count": 0.0
        },
        "summary": {
            "en-US": "Stay connected to Nelly Furtado and download her new browser application."
        },
        "tags": [
            "artist",
            "browsernation",
            "music",
            "nelly furtado",
            "pop",
            "rss reader",
            "theme",
            "toolbar"
        ],
        "weekly_downloads": 0
    }
}

EXPECTED_FINAL_JDATA = {
    "gnome-download-notify@ion201": {
        "categories": {
            "firefox": [
                "alerts-updates"
            ]
        },
        "current_version": {
            "files": [
                {
                    "id": 844358,
                    "is_webextension": True,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "en-US",
        "description": {
            "en-US": "Native notification integration for completed downloads."
        },
        "first_create_date": "2015-06-20T11:21:58Z",
        "guid": "gnome-download-notify@ion201",
        "name": {
            "en-US": "Download Notifications"
        },
        "ratings": {
            "average": 5.0,
            "bayesian_average": 4.58373,
            "count": 11.0,
            "text_count": 6.0
        },
        "summary": {
            "en-US": "Native notification integration for completed downloads."
        },
        "tags": [
            "cinnamon",
            "download",
            "firefox57",
            "gnome",
            "kde",
            "libnotify",
            "linux",
            "mate",
            "notification",
            "notify-send",
            "unity",
            "xfce"
        ],
        "weekly_downloads": 105
    },
    "jid1-tpeRu7ABM810Fw@jetpack": {
        "categories": {
            "firefox": [
                "photos-music-videos"
            ]
        },
        "current_version": {
            "files": [
                {
                    "id": 216360,
                    "is_webextension": False,
                    "platform": "all",
                    "status": "public"
                }
            ]
        },
        "default_locale": "zh-CN",
        "description": None,
        "first_create_date": "2011-10-13T04:55:06Z",
        "guid": "jid1-tpeRu7ABM810Fw@jetpack",
        "name": {
            "zh-CN": "\\u8c46\\u74e3\\u7535\\u53f0OSD Lyrics\\u63d2\\u4ef6"
        },
        "ratings": {
            "average": 3.0,
            "bayesian_average": 2.38892,
            "count": 3.0,
            "text_count": 0.0
        },
        "summary": {
            "zh-CN":
                "\\u4f7f\\u7528OSD Lyrics\\u663e\\u793a\\u8c46\\u74e3\\u7535\\u53f0\\u6b4c\\u8bcd"
        },
        "tags": [
            "douban",
            "osd-lyrics",
            "osdlyrics",
            "\\u8c46\\u74e3"
        ],
        "weekly_downloads": 1
    }
}


@pytest.yield_fixture(scope="function")
def s3_fixture():
    mock_s3().start()

    conn = boto3.resource('s3', region_name='us-west-2')
    conn.create_bucket(Bucket=taar_amowhitelist.AMO_DUMP_BUCKET)
    taar_utils.store_json_to_s3(json.dumps(SAMPLE_DATA),
                                taar_amowhitelist.AMO_DUMP_BASE_FILENAME,
                                '20171106',
                                taar_amowhitelist.AMO_DUMP_PREFIX,
                                taar_amowhitelist.AMO_DUMP_BUCKET)
    yield conn, SAMPLE_DATA
    mock_s3().stop()


def test_extract(s3_fixture):
    '''
    The transform for the AMOTransformer is just filtering by
    age using `first_create_date` and using the ratings.average
    with a minimum of 3.0
    '''

    etl = taar_amowhitelist.AMOTransformer(taar_amowhitelist.AMO_DUMP_BUCKET,
                                           taar_amowhitelist.AMO_DUMP_PREFIX,
                                           taar_amowhitelist.AMO_DUMP_FILENAME,
                                           taar_amowhitelist.MIN_RATING,
                                           taar_amowhitelist.MIN_AGE)
    jdata = etl.extract()
    assert jdata == SAMPLE_DATA


def test_transform(s3_fixture):
    '''
    The transform for the AMOTransformer is just filtering by
    age using `first_create_date` and using the ratings.average
    with a minimum of 3.0
    '''

    conn, data = s3_fixture
    etl = taar_amowhitelist.AMOTransformer(taar_amowhitelist.AMO_DUMP_BUCKET,
                                           taar_amowhitelist.AMO_DUMP_PREFIX,
                                           taar_amowhitelist.AMO_DUMP_FILENAME,
                                           taar_amowhitelist.MIN_RATING,
                                           taar_amowhitelist.MIN_AGE)
    final_jdata = etl.transform(data)
    assert len(final_jdata) == 1

    today = datetime.datetime.today().replace(tzinfo=None)
    for client_data in final_jdata.values():
        assert client_data['current_version']['files'][0]['is_webextension']
        assert client_data['ratings']['average'] >= taar_amowhitelist.MIN_RATING
        create_datetime = parse(client_data['first_create_date']).replace(tzinfo=None)
        assert create_datetime + datetime.timedelta(days=taar_amowhitelist.MIN_AGE) < today


def test_load(s3_fixture):
    conn, data = s3_fixture

    etl = taar_amowhitelist.AMOTransformer(taar_amowhitelist.AMO_DUMP_BUCKET,
                                           taar_amowhitelist.AMO_DUMP_PREFIX,
                                           taar_amowhitelist.AMO_DUMP_FILENAME,
                                           taar_amowhitelist.MIN_RATING,
                                           taar_amowhitelist.MIN_AGE)
    etl.load(EXPECTED_FINAL_JDATA)

    s3 = boto3.resource('s3', region_name='us-west-2')
    bucket_obj = s3.Bucket(taar_amowhitelist.AMO_DUMP_BUCKET)

    available_objects = list(bucket_obj.objects.filter(Prefix=taar_amowhitelist.AMO_DUMP_PREFIX))
    # Check that our file is there.
    full_s3_name = '{}{}'.format(taar_amowhitelist.AMO_DUMP_PREFIX,
                                 taar_amowhitelist.FILTERED_AMO_FILENAME)
    keys = [o.key for o in available_objects]
    assert full_s3_name in keys
