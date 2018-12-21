from mozetl.testpilot.txp_mau_dau import get_experiments
from .utils import get_resource
import requests
import json


class MockRequest():
    def raise_for_status(self):
        pass

    def json(self):
        return json.loads(get_resource('txp_experiments.json'))


def test_get_experiments(monkeypatch):
    def mock_get(url):
        return MockRequest()
    monkeypatch.setattr(requests, 'get', mock_get)
    # There are 14 total experiments in the file but one should be filtered out
    # due to not having an addon_id -- we base this mau/dau on the addons dataset
    assert len(get_experiments()) == 13
