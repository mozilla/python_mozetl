"""Test suite for TAAR Locale Job."""

import copy
import functools
import numpy as np
import pytest

from mozetl.taar import taar_similarity
from pyspark.sql.types import (
    StructField, StructType, StringType,
    LongType, BooleanType, ArrayType, MapType, Row
)

longitudinal_schema = StructType([
    StructField("client_id",             StringType(),  True),
    StructField("normalized_channel",    StringType(),  True),
    StructField("geo_city",              ArrayType(StringType(),  True)),
    StructField("subsession_length",     ArrayType(LongType(),  True)),
    StructField("os",                    StringType(),  True),
    StructField(
        "build",
        ArrayType(
            StructType(
                [StructField("application_name",  StringType(),  True)]),
            True),
        True),
    StructField("settings",              ArrayType(
        StructType(
            [StructField("locale",        StringType(),  True)]), True),
        True),
    StructField("active_addons",         ArrayType(
        MapType(StringType(), StructType([
            StructField("blocklisted",      BooleanType(), True),
            StructField("type",             StringType(), True),
            StructField("signed_state",     LongType(), True),
            StructField("user_disabled",    BooleanType(), True),
            StructField("app_disabled",     BooleanType(), True),
            StructField("is_system",        BooleanType(), True),
            StructField("foreign_install",  BooleanType(), True)
        ]), True), True)),
    StructField(
        "places_bookmarks_count",
        ArrayType(
            StructType(
                [StructField("sum",  LongType(),  True)]),
            True),
        True),
    StructField(
        "scalar_parent_browser_engagement_tab_open_event_count",
        ArrayType(
            StructType(
                [StructField("value",  LongType(),  True)]),
            True),
        True),
    StructField(
        "scalar_parent_browser_engagement_total_uri_count",
        ArrayType(
            StructType(
                [StructField("value",  LongType(),  True)]),
            True),
        True),
    StructField(
        "scalar_parent_browser_engagement_unique_domains_count",
        ArrayType(
            StructType(
                [StructField("value",  LongType(),  True)]),
            True),
        True)
    ])

default_sample = {
    "client_id":             "client-id",
    "normalized_channel":    "release",
    "geo_city": ["Boston"],
    "subsession_length": [10],
    "os": "Windows",
    "build": [{
        "application_name":  "Firefox"
    }],
    "settings": [{
        "locale":            "en-US"
    }],
    "active_addons": [],
    "places_bookmarks_count": [{"sum": 1}],
    "scalar_parent_browser_engagement_tab_open_event_count": [{"value": 2}],
    "scalar_parent_browser_engagement_total_uri_count": [{"value": 3}],
    "scalar_parent_browser_engagement_unique_domains_count": [{"value": 4}]
}


@pytest.fixture()
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=default_sample,
        schema=longitudinal_schema
    )


@pytest.fixture
def multi_clusters_df(generate_data):
    # Stub a different set of addons for each variation
    # so that we get 3 clusters out.
    CLUSTER_VARIATIONS = [
        {
            "count": 50,
            "variation": {
                "geo_city": ["Rome"],
                "subsession_length": [3785],
                "os": "Linux",
                "settings": [{
                    "locale": "it-IT"
                }],
            }
        },
        {
            "count": 50,
            "variation": {
                "geo_city": ["London"],
                "subsession_length": [1107],
                "os": "MacOS",
                "settings": [{
                    "locale": "en-UK"
                }],
            }
        },
        {
            "count": 50,
            "variation": {
                "geo_city": ["Boston"],
                "subsession_length": [201507],
                "os": "Windows",
                "settings": [{
                    "locale": "en-US"
                }],
            }
        }
    ]

    sample_snippets = []
    counter = 0
    for variation_id, cluster in enumerate(CLUSTER_VARIATIONS):
        for i in range(0, cluster["count"]):
            variation = copy.deepcopy(cluster["variation"])
            variation["client_id"] = "client-{}".format(counter)

            # Generate some valid addons for each client. Each stub
            # addon as a specific GUID that contains both the variation
            # id and the addon id for simplified debugging.
            variation["active_addons"] = [{}]
            for addon_id in range(0, 3):
                addon_name = "var-{}-guid-{}".format(variation_id, addon_id)
                variation["active_addons"][0][addon_name] = {
                    "blocklisted":     False,
                    "user_disabled":   False,
                    "app_disabled":    False,
                    "signed_state":    2,
                    "type":            "extension",
                    "foreign_install": False,
                    "is_system":       False
                }

            # Additionally add a system addon.
            variation["active_addons"][0]["system-addon-guid"] = {
                "blocklisted":     False,
                "user_disabled":   False,
                "app_disabled":    False,
                "signed_state":    2,
                "type":            "extension",
                "foreign_install": False,
                "is_system":       True
            }

            # Additionally add an AMO unlisted addon.
            variation["active_addons"][0]["unlisted-addon-guid"] = {
                "blocklisted":     False,
                "user_disabled":   False,
                "app_disabled":    False,
                "signed_state":    2,
                "type":            "extension",
                "foreign_install": False,
                "is_system":       False
            }

            sample_snippets.append(variation)
            counter = counter + 1

    return generate_data(sample_snippets)


@pytest.fixture
def addon_whitelist():
    addon_whitelist = ["system-addon-guid"]
    for cluster_id in range(0, 3):
        for addon_id in range(0, 3):
            addon_whitelist.append("var-{}-guid-{}".format(cluster_id, addon_id))
    return addon_whitelist


def test_non_cartesian_pairs(spark):
    TEST_DATA_1 = [1, 2, 3, 4]
    TEST_DATA_2 = ['a', 'b', 'c', 'd']

    rdd1 = spark.sparkContext.parallelize(TEST_DATA_1)
    rdd2 = spark.sparkContext.parallelize(TEST_DATA_2)

    # Make sure we get the first element from rdd1 and the second
    # from rdd2.
    pairs = taar_similarity.generate_non_cartesian_pairs(rdd1, rdd2)
    for p in pairs.collect():
        assert p[0] in TEST_DATA_1
        assert p[1] in TEST_DATA_2


def test_similarity():
    UserDataRow = Row(
        "normalized_channel", "geo_city", "subsession_length", "os", "locale",
        "active_addons", "bookmark_count", "tab_open_count", "total_uri", "unique_tlds"
    )

    test_user_1 = UserDataRow("release", "Boston", 10, "Windows", "en-US", [], 1, 2, 3, 4)
    test_user_2 = UserDataRow("release", "notsoB", 10, "swodniW", "SU-ne", [], 1, 2, 3, 4)
    test_user_3 = UserDataRow("release", "Boston", 0, "Windows", "en-US", [], 0, 0, 0, 0)
    test_user_4 = UserDataRow("release", "notsoB", 0, "swodniW", "SU-ne", [], 0, 0, 0, 0)
    # The following user contains a None value for "total_uri" and geo_city
    # (categorical feature). The latter should never be possible, but let's be cautious.
    test_user_5 = UserDataRow("release", None, 10, "swodniW", "SU-ne", [], 1, None, 3, 4)

    taar_similarity.similarity_function(test_user_1, test_user_4)

    # Identical users should be very close (0 distance).
    assert np.isclose(taar_similarity.similarity_function(test_user_1, test_user_1), 0.0)
    # Users with completely different categorical features but identical
    # continuous features should be slightly different.
    assert np.isclose(taar_similarity.similarity_function(test_user_1, test_user_2), 0.001)
    # Users with completely different continuous features but identical
    # categorical features should be very close.
    assert np.isclose(taar_similarity.similarity_function(test_user_1, test_user_3), 0.0)
    # Completely different users should be far away.
    assert taar_similarity.similarity_function(test_user_1, test_user_4) >= 1.0
    # Partial user information should not break the similarity function.
    assert taar_similarity.similarity_function(test_user_1, test_user_5)


def test_get_addons(spark, addon_whitelist, multi_clusters_df):
    multi_clusters_df.createOrReplaceTempView("longitudinal")

    samples_df = taar_similarity.get_samples(spark)
    addons_df = taar_similarity.get_addons_per_client(samples_df, addon_whitelist, 2)

    # We should have one row per client and that row should contain
    # addons as an array.
    assert samples_df.count() == addons_df.count()
    assert isinstance(addons_df.schema.fields[1].dataType, ArrayType)


def test_compute_donors(spark, addon_whitelist, multi_clusters_df):
    multi_clusters_df.createOrReplaceTempView("longitudinal")

    # Perform the clustering on our test data. We expect
    # 3 clusters out of this and 10 donors.
    _, donors_df = taar_similarity.get_donors(spark, 3, 10, addon_whitelist, random_seed=42)
    donors = taar_similarity.format_donors_dictionary(donors_df)

    # Even if we requested 10 donors, it doesn't mean we will receive
    # precisely that number. All we can do is check that the number of
    # donors should always be >= 2 * num_clusters. Since we're fixing
    # the seed for this test, we can just assert that we receive 14 donors.
    assert len(donors) == 14

    # Our artificial clusters should report different cities.
    for cluster_id, city in enumerate(["Rome", "London", "Boston"]):
        # We should see at least one item for the "Rome" cluster.
        cluster_donors = [d for d in donors if d["geo_city"] == city]
        assert len(cluster_donors) >= 1

        # The generated data must have all the required fields.
        REQUIRED_FIELDS = [
            "geo_city", "subsession_length", "locale", "os", "bookmark_count",
            "tab_open_count", "total_uri", "unique_tlds", "active_addons"
        ]
        donor = cluster_donors[0]
        for f in REQUIRED_FIELDS:
            assert f in donor.keys()

        # Verify that no system addon or unlisted addons are reported.
        assert "system-addon-guid" not in donor["active_addons"]
        assert "unlisted-addon-guid" not in donor["active_addons"]

        # Verify that all the donor addons come from the relative
        # cluster.
        expected_initial = "var-{}-guid".format(cluster_id)
        for addon_id in donor["active_addons"]:
            assert addon_id.startswith(expected_initial)
