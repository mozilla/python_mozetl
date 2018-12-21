"""Test suite for validating hwsurvey.json"""
import ujson
import os

from mozetl.hardware_report.check_output import (
    _check_most_recent_change as check_most_recent_change,
    _get_data as get_data,
    _make_report as make_report)


def test_check_most_recent_change_min_change():
    test_data = {
        20170701: {
            "nochange": 1.0,
            "somechange": 1.0,
            "bigchange": 1.0
        },
        20170702: {
            "nochange": 1.0,
            "somechange": 1.1,
            "bigchange": 1.4
        }
    }

    assert check_most_recent_change(test_data, min_change=0.5).keys()\
        == set()
    assert check_most_recent_change(test_data, min_change=0.3).keys()\
        == {"bigchange"}
    assert check_most_recent_change(test_data, min_change=0.05).keys()\
        == {"bigchange", "somechange"}


def test_check_most_recent_change_min_value():
    test_data = {
        20170701: {
            "somechange1": 1.5,
            "somechange2": 0.5
        },
        20170702: {
            "somechange1": 1.0,
            "somechange2": 0.0
        }
    }

    assert check_most_recent_change(test_data, min_change=0.1, min_value=2.0).keys()\
        == set()
    assert check_most_recent_change(test_data, min_change=0.1, min_value=1.0).keys()\
        == {"somechange1"}
    assert check_most_recent_change(test_data, min_change=0.1, min_value=0.0).keys()\
        == {"somechange1", "somechange2"}


def test_check_most_recent_change_missing_val():
    test_data = {
        20170701: {
            "change": 0.5,
        },
        20170702: {}
    }

    assert check_most_recent_change(test_data, min_change=0.1, min_value=0.0, missing_val=0.5).keys() == set()
    assert check_most_recent_change(test_data, min_change=0.1, min_value=0.0, missing_val=0.01).keys() == {"change"}
    assert check_most_recent_change(test_data, min_change=0.1, min_value=0.0, missing_val=1.0).keys() == {"change"}


def test_get_data():
    test_data = [
        {
            "date": "20170701",
            "nochange": 0.5,
            "somechange": 0.5,
            "bigchange": 0.5
        },
        {
            "date": "20170702",
            "nochange": 0.5,
            "somechange": 0.4,
            "bigchange": 0.1
        }
    ]

    with open('hwsurvey-weekly.json', 'w') as f:
        f.write(ujson.dumps(test_data))

    expected = {
        20170701: {
            "nochange": 0.5,
            "somechange": 0.5,
            "bigchange": 0.5
        },
        20170702: {
            "nochange": 0.5,
            "somechange": 0.4,
            "bigchange": 0.1
        }
    }

    assert get_data() == expected
    os.remove('hwsurvey-weekly.json')


def test_report_missing_data():
    test_data = {
        20170701: {
            "change": 0.5,
        },
        20170702: {}
    }

    changes = check_most_recent_change(test_data, min_change=0.1, min_value=0.0, missing_val=1.0)
    assert make_report(changes) == "change: Last week = 50.00%, This week = 100.00%"
