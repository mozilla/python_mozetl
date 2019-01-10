import pytest
import json
from pyspark.sql import SQLContext, Row

from mozetl.testpilot.pulse import transform_pings


def create_row():
    with open("tests/pulse_ping.json") as infile:
        return json.load(infile)


@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize([create_row()])


def test_simple_transform(row_to_dict, simple_rdd, spark_context):
    actual = transform_pings(SQLContext(spark_context), simple_rdd).take(1)[0]

    empty_request_keys = [
        "sub_frame",
        "stylesheet",
        "script",
        "image",
        "object",
        "xmlhttprequest",
        "xbl",
        "xslt",
        "ping",
        "beacon",
        "xml_dtd",
        "font",
        "media",
        "websocket",
        "csp_report",
        "imageset",
        "web_manifest",
        "other",
    ]
    empty_request = Row(cached=None, cdn=None, num=None, time=None)

    requests = dict(
        [(key, empty_request) for key in empty_request_keys]
        + [
            ("all", Row(num=12, time=4978, cached=0.08333333333333333, cdn=0)),
            ("main_frame", Row(num=1, time=726, cached=0, cdn=0)),
        ]
    )

    expected = {
        "method": None,
        "id": u"19e0cf07-8145-4666-938d-811397db85dc",
        "type": None,
        "object": None,
        "category": None,
        "variant": None,
        "details": u"test",
        "sentiment": 5,
        "reason": u"like",
        "adBlocker": True,
        "addons": [
            u"pulse@mozilla.com",
            u"inspector@mozilla.org",
            u"DevPrefs@jetpack",
            u"@min-vid",
        ],
        "channel": u"developer",
        "hostname": u"github.com",
        "language": u"en-US",
        "openTabs": 7,
        "openWindows": 2,
        "platform": u"darwin",
        "protocol": u"https:",
        "telemetryId": u"549a6456-32c2-e048-8310-d22ccfa9fee1",
        "timerContentLoaded": 589,
        "timerFirstInteraction": None,
        "timerFirstPaint": 41,
        "timerWindowLoad": 1005,
        "inner_timestamp": 1487862372503,
        "fx_version": None,
        "creation_date": None,
        "test": u"pulse@mozilla.com",
        "variants": None,
        "timestamp": 76543,
        "version": u"1.0.2",
        "requests": requests,
        "disconnectRequests": 1,
        "consoleErrors": 0,
        "e10sStatus": 1,
        "e10sProcessCount": 4,
        "trackingProtection": False,
    }

    assert row_to_dict(actual, recursive=False) == expected
