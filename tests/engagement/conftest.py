import functools

import pytest

from . import data
from mozetl.engagement.churn import release


@pytest.fixture()
def generate_main_summary_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=data.main_summary_sample,
        schema=data.main_summary_schema,
    )


@pytest.fixture()
def generate_new_profile_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=data.new_profile_sample,
        schema=data.new_profile_schema,
    )


@pytest.fixture()
def release_info():
    return {
        "major": {"52.0": "2017-03-07"},
        "minor": {
            "51.0.1": "2017-01-26",
            "52.0.1": "2017-03-17",
            "52.0.2": "2017-03-29",
        },
    }


@pytest.fixture(autouse=True)
def no_get_release_info(release_info, monkeypatch):
    """ Disable get_release_info because of requests change over time. """

    def mock_get_release_info():
        return release_info

    monkeypatch.setattr(release, "get_release_info", mock_get_release_info)

    # disable fetch_json to cover all the bases
    monkeypatch.delattr(release, "fetch_json")


@pytest.fixture()
def effective_version(spark):
    return release.create_effective_version_table(spark)
