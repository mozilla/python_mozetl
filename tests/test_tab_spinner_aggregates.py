import pytest
import json
import pandas as pd

from mozetl.tab_spinner.utils import get_short_and_long_spinners


def create_row():
    with open("tests/tab_spinner_ping.json") as infile:
        return json.load(infile)


@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize([create_row()])


def test_simple_transform(simple_rdd, spark_context):
    results = get_short_and_long_spinners(simple_rdd)

    # get rid of pd index
    result = {
        k: {build_id: series.values for build_id, series in v}
        for k, v in results.items()
    }

    expected = {
        "short": {"20170101": pd.Series([0, 1.0, 0, 0, 0, 0, 0, 0])},
        "long": {"20170101": pd.Series([0, 0.0, 1.0, 0, 0, 0, 0, 0])},
    }

    for k, v in expected.items():
        for build_id, series in v.items():
            assert all(result[k][build_id] == series)
