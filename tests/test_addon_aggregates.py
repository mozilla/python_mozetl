from mozetl.addon_aggregates import addon_aggregates
import datetime as dt
import pytest


@pytest.fixture
def load_data(spark):
    """
    Create synthetic main_summary dataframe
    with only relevant fields
    """
    return spark.read.json("./tests/ms-test-data.json")


@pytest.fixture
def explode_data(load_data):
    return addon_aggregates.ms_explode_addons(load_data)


@pytest.fixture
def add_columns(explode_data):
    return addon_aggregates.add_addon_columns(explode_data)


@pytest.fixture
def aggregate_data(add_columns):
    return addon_aggregates.aggregate_addons(add_columns)


# tests for helper/non-spark functions ###


def test_get_dest():
    """
    Ensure proper contruction of input/output s3 paths
    """
    s1 = "s3://bucket/prefix/version/"
    s2 = s1 + "submission_date_s3=20170101/"
    s3 = s2 + "sample_id=42/"

    gd = addon_aggregates.get_dest
    assert gd("bucket", "prefix", "version") == s1
    assert gd("bucket", "prefix", "version", "20170101") == s2
    assert gd("bucket", "prefix", "version", "20170101", "42") == s3


# tests for pyspark functions ###


def test_explode_data(explode_data):
    assert explode_data.count() == 11


def test_add_columns(add_columns):
    columns = [
        "addon_id",
        "app_version",
        "client_id",
        "foreign_install",
        "install_day",
        "is_foreign_install",
        "is_self_install",
        "is_shield_addon",
        "is_system",
        "is_web_extension",
        "locale",
        "normalized_channel",
        "profile_creation_date",
        "sample_id",
    ]

    # ensure proper schema
    assert sorted(add_columns.columns) == columns

    # ensure addon_ids equal to None register no
    # non-zero values in the added columns
    none_addon_id = add_columns.filter("addon_id is null").collect()[0]

    # ensure null addon_ids
    # have no non-zero values
    # in the additional columns
    assert (
        none_addon_id.is_system
        == none_addon_id.is_web_extension
        == none_addon_id.is_shield_addon
        == none_addon_id.is_foreign_install
        == none_addon_id.is_self_install
        == 0
    )


def test_addon_counts(aggregate_data):
    """
    Test the aggregation across add-on types
    """

    # true values as defined in ms-test-data.json
    true_client_counts = {
        1: {
            "n_self_installed_addons": 1,
            "n_foreign_installed_addons": 1,
            "n_web_extensions": 1,
            "n_system_addons": 1,
            "n_shield_addons": 0,
        },
        2: {
            "n_self_installed_addons": 0,
            "n_foreign_installed_addons": 0,
            "n_web_extensions": 0,
            "n_system_addons": 0,
            "n_shield_addons": 1,
        },
        3: {
            "n_self_installed_addons": 1,
            "n_foreign_installed_addons": 0,
            "n_web_extensions": 1,
            "n_system_addons": 0,
            "n_shield_addons": 0,
        },
        4: {
            "n_self_installed_addons": 0,
            "n_foreign_installed_addons": 0,
            "n_web_extensions": 0,
            "n_system_addons": 2,
            "n_shield_addons": 1,
        },
    }

    for client_id in true_client_counts:
        data = aggregate_data.filter(aggregate_data.client_id == client_id).collect()[0]
        for key, value in true_client_counts[client_id].iteritems():
            assert data[key] == value


def test_unix_dates(aggregate_data):
    """
    Test profile_creation_date and install_date
    are properly reformatted relative to their
    previous values
    """

    # true values as defined in ms-test-data.json
    # min install_day - pcd
    true_client_days_to_install = {
        1: {"min_install_day": 16890 - 15000},
        2: {"min_install_day": None},
        3: {"min_install_day": 16800 - 15002},
        4: {"min_install_day": None},
    }

    for client_id, value in true_client_days_to_install.iteritems():
        data = aggregate_data.filter(aggregate_data.client_id == client_id).collect()[0]
        first_install = data.first_addon_install_date
        pcd = data.profile_creation_date
        print(first_install, pcd)
        fmt = "%Y%m%d"
        days_to_install = (
            (
                dt.datetime.strptime(first_install, fmt)
                - dt.datetime.strptime(pcd, fmt)
            ).days
            if first_install is not None
            else None
        )

        assert (
            true_client_days_to_install[client_id]["min_install_day"] == days_to_install
        )


def test_agg_by_channel_locale_and_version(aggregate_data):
    """
    Test the aggregation by channel, locale and app_version.
    """

    # true values as defined in ms-test-data.json
    true_values = {
        "normalized_channel": {"release": 1, "beta": 2, "nightly": 1},
        "locale": {"en-US": 2, "de": 1, "ru": 1},
        "app_version": {"57": 2, "56": 1, "58": 1},
    }

    for grouping_field in ("normalized_channel", "locale", "app_version"):
        counts = aggregate_data.groupBy(grouping_field).count().collect()
        for i in counts:
            assert true_values[grouping_field][i[grouping_field]] == i["count"]
