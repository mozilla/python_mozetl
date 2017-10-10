"""Test suite for TAAR Legacy Job."""

import pytest
import requests

AMO_LEGACY_RECS_URI =\
    "https://addons-dev.allizom.org/api/v3/addons/replacement-addon/"


@pytest.fixture
def is_api_up():
    request_uri = AMO_LEGACY_RECS_URI
    while request_uri is not None:
        # Request some data from the AMO API.
        r = requests.get(request_uri)
    return r.ok

assert is_api_up()
