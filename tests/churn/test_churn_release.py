from datetime import datetime

import pytest
from pyspark.sql import functions as F

from mozetl.churn import release

"""
For reference:

```
"major": {
     "52.0": "2017-03-07"
},
"minor": {
    "51.0.1": "2017-01-26",
    "52.0.1": "2017-03-17",
    "52.0.2": "2017-03-29"
}
```
"""


def test_date_to_version_range(release_info):
    result = release.create_date_to_version(release_info)

    start = datetime.strptime(
        release_info['minor']['51.0.1'], '%Y-%m-%d')
    end = datetime.strptime(
        release_info['minor']['52.0.2'], '%Y-%m-%d')

    assert len(result) == (end - start).days + 1
    assert result['2017-03-08'] == '52.0'
    assert result['2017-03-17'] == '52.0.1'


@pytest.fixture()
def usage_dates(dataframe_factory):
    sample = {"uid": 0, "date": "2017-01-01", "channel": "release"}
    snippets = [
        {"uid": 1, "date": "2014-01-02"},  # older
        {"uid": 2, "date": None},  # older
        {"uid": 3, "date": "2017-03-07"},
        {"uid": 4, "date": "2017-03-08"},
        {"uid": 5, "date": "2017-03-07", "channel": "beta"},
        {"uid": 6, "date": "2017-03-07", "channel": None},

    ]
    return dataframe_factory.create_dataframe(snippets, sample)


def test_with_effective_version(usage_dates, effective_version):
    result = (
        release.with_effective_version(usage_dates, effective_version, "date")
    )

    def get_case(uid):
        return result.where(F.col("uid") == uid).collect()[0].start_version

    assert get_case(1) == "older"
    assert get_case(2) == "older"
    assert get_case(3) == "52.0"
    assert get_case(4) == "52.0"
    assert get_case(5) == "53.0"
    assert get_case(6) == "52.0"
