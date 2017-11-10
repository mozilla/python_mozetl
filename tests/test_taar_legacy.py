"""Test suite for TAAR Legacy Job."""

import pytest
from mozetl.taar import taar_legacy

FAKE_LEGACY_DATA = {
    "guid-01":
        ["guid-02"],
    "guid-03":
        ["guid-05"],
    "guid-05":
        ["guid-06"],
    "guid-07": ["guid-08-1", "guid-09-2", "guid-10-3", "guid-11-4"],
    "guid-12": ["guid-13-1", "guid-14-2", "guid-15-3", "guid-16-4",
                "guid-17-5", "guid-18-6", "guid-19-7", "guid-20-8",
                "guid-21-9", "guid-22-10"],
    "next": None
}

FAKE_BROKEN_LEGACY_DATA = {
    "guid-23": [],
    "guid-24": {},
    "guid-25": None
}

FAKE_MIXED_LEGACY_DATA = {
    "guid-26": [],
    "guid-27": {},
    "guid-28": None,
    "guid-29": ["guid-30"]
}


@pytest.fixture
def mock_amo_api_response(monkeypatch):
    monkeypatch.setattr('mozetl.taar.taar_legacy.parse_from_amo_api',
                        lambda x, y: FAKE_LEGACY_DATA)


@pytest.fixture
def mock_amo_broke(monkeypatch):
    monkeypatch.setattr('mozetl.taar.taar_legacy.parse_from_amo_api',
                        lambda x, y: FAKE_BROKEN_LEGACY_DATA)


@pytest.fixture
def mock_amo_mixed(monkeypatch):
    monkeypatch.setattr('mozetl.taar.taar_legacy.parse_from_amo_api',
                        lambda x, y: FAKE_MIXED_LEGACY_DATA)


def test_fetch_legacy_replacement_masterlist(mock_amo_api_response):
    # Test with mocked properly-formed data.
    test_data = taar_legacy.fetch_legacy_replacement_masterlist()

    # Test for expected legacy addon guids as keys.
    for i in ["guid-01", "guid-03", "guid-05", "guid-07", "guid-12"]:
        assert i in test_data.keys()

    # Test for removeal of pagination structure in output.
    assert "next" not in test_data.keys()

    # Test for values in specific legacy replacement.
    expected = ["guid-13-1", "guid-14-2", "guid-15-3", "guid-16-4",
                "guid-17-5", "guid-18-6", "guid-19-7", "guid-20-8",
                "guid-21-9", "guid-22-10"]

    assert test_data["guid-12"] == expected


def test_fetch_broken_legacy(mock_amo_broke):
    # Test with mocked malformed data.
    test_data = taar_legacy.fetch_legacy_replacement_masterlist()

    # This should be empty as no valid entries exist in mocked data.
    assert not test_data


def test_fetch_mixed_legacy(mock_amo_mixed):
    # Test with mocked malformed data.
    test_data = taar_legacy.fetch_legacy_replacement_masterlist()

    # This contain only the single valid entry in the mocked data.
    assert "guid-29" in test_data.keys()
    assert test_data["guid-29"] == ["guid-30"]
    assert len(test_data) == 1


def test_parse_from_amo_api():
    # Test api reading function with None URL.
    expected = {'test-key': 'test-val'}
    new_dict = taar_legacy.parse_from_amo_api(None, expected)
    assert expected == new_dict
