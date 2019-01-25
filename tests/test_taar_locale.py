"""Test suite for TAAR Locale Job."""

import functools
import pytest
from datetime import date, datetime
from numpy import repeat as np_repeat
from pandas import DataFrame
from mozetl.taar import taar_locale
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import substring_index


CLIENTS_DAILY_SCHEMA = StructType(
    [
        StructField("client_id", StringType(), True),
        StructField("submission_date_s3", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("locale", StringType(), True),
        StructField(
            "active_addons",
            # active_addons is a list of dictionaries holding all
            # metadata related to an addon
            ArrayType(
                StructType(
                    [
                        StructField("addon_id", StringType(), True),
                        StructField("app_disabled", BooleanType(), True),
                        StructField("blocklisted", BooleanType(), True),
                        StructField("foreign_install", BooleanType(), True),
                        StructField("is_system", BooleanType(), True),
                        StructField("signed_state", LongType(), True),
                        StructField("type", StringType(), True),
                        StructField("user_disabled", BooleanType(), True),
                    ]
                ),
                True,
            ),
        ),
    ]
)

# Templates for a single `active_addons` entry.
ADDON_TEMPLATES = {
    "good": {
        "addon_id": "<guid>",
        "app_disabled": False,
        "blocklisted": False,
        "foreign_install": False,
        "is_system": False,
        "signed_state": 2,
        "type": "extension",
        "user_disabled": False,
    },
    "bad1": {
        "addon_id": "<guid>",
        "app_disabled": False,
        "blocklisted": False,
        "foreign_install": False,
        "is_system": True,
        "signed_state": 2,
        "type": "extension",
        "user_disabled": False,
    },
    "bad2": {
        "addon_id": "<guid>",
        "app_disabled": False,
        "blocklisted": False,
        "foreign_install": False,
        "is_system": False,
        "signed_state": 0,
        "type": "extension",
        "user_disabled": True,
    },
}

# Template for a clients_daily row.
ROW_TEMPLATE = {
    "client_id": "<client_id>",
    "submission_date_s3": "<yyyymmdd>",
    "channel": "release",
    "app_name": "Firefox",
    "locale": "en-US",
    "active_addons": "<addons_list>",
}

# Condensed client data.
# Generate snippets using `generate_rows_for_client()`.
SAMPLE_CLIENT_DATA = {
    "client-1": {
        "20190115": ["guid-1", "guid-5", "guid-bad1"],
        "20190113": ["guid-1", "guid-4", "guid-bad1"],
        "20190112": ["guid-1", "guid-3", "guid-bad1"],
        "20190110": ["guid-1", "guid-bad1"],
    },
    "client-2": {
        "20190114": ["guid-2", "guid-bad2"],
        "20190112": ["guid-1", "guid-bad2"],
    },
    "client-3": {"20190109": ["guid-1"]},
    "client-4": {"20190112": ["guid-1", "guid-2"]},
    "client-5": {"20190114": [], "20190113": []},
    "client-6": {"20190114": ["guid-1"], "20190112": ["guid-1"]},
    "client-7": {
        "20190114": [
            "guid-1",
            "guid-2",
            "guid-3",
            "guid-4",
            "guid-5",
            "guid-not-whitelisted",
        ]
    },
}

# Per-locale add-on count records.
SAMPLE_ADDON_COUNTS = [
    ("en-US", "guid-1", 5),
    ("en-US", "guid-2", 2),
    ("en-US", "guid-3", 1),
    ("de", "guid-1", 3),
    ("de", "guid-2", 2),
    ("de", "guid-3", 4),
]

# Boundary dates to use when querying the mocked `clients_daily`.
DATE_RANGE = {"start": "20190112", "end": "20190114"}

LOCALE_LIMITS = {"en-US": 1, "de": 3, "pl": 7}

AMO_WHITELIST = ["guid-1", "guid-2", "guid-3", "guid-4", "guid-5", "guid-not-installed"]


def generate_addon_entry(guid):
    """Generate an `active_addons` entry with the given GUID.

    The desired type ('good'/'badN') can optionally be tagged on to the GUID
    as a suffix delimited by '-'.
    """
    guid_suffix = guid.split("-")[-1]
    addon_type = guid_suffix if guid_suffix.startswith("bad") else "good"
    entry = dict(ADDON_TEMPLATES[addon_type])
    entry["addon_id"] = guid
    return entry


def generate_rows_for_client(client_id, client_data):
    """Generate dataframe_factory snippets with the given client ID from the
    condensed format
    {
        '<date>': [ <addon1>, ... ],
        ...
    },
    where <addon> will be passed to `generate_addon_entry()`. Ordering of the
    rows is not guaranteed.
    """
    snippets = []
    for subm_date, addons in client_data.items():
        snippets.append(
            {
                "client_id": client_id,
                "submission_date_s3": subm_date,
                "active_addons": [generate_addon_entry(a) for a in addons],
            }
        )
    return snippets


@pytest.fixture
def generate_data(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=ROW_TEMPLATE,
        schema=CLIENTS_DAILY_SCHEMA,
    )


@pytest.fixture
def locale_limits():
    return dict(LOCALE_LIMITS)


@pytest.fixture
def whitelist():
    return list(AMO_WHITELIST)


@pytest.fixture
def short_locale_limits(locale_limits):
    """A condensed version of the locale limits."""
    del locale_limits["pl"]
    return locale_limits


@pytest.fixture
def short_whitelist(whitelist):
    """A condensed version of the whitelist."""
    for x in ["guid-4", "guid-5"]:
        whitelist.remove(x)
    return whitelist


@pytest.fixture
def addon_counts():
    return DataFrame.from_records(
        SAMPLE_ADDON_COUNTS, columns=["locale", "addon", "count"]
    )


@pytest.fixture
def addon_count_neg(addon_counts):
    """Add-on counts with a negative count, as could occur from adding noise."""
    replace_row = (addon_counts["locale"] == "de") & (addon_counts["addon"] == "guid-1")
    addon_counts.loc[replace_row, "count"] = -1
    return addon_counts


@pytest.fixture
def client_addons_df(generate_data, locale_limits):
    """Returns a mock `clients_daily` Spark DF."""
    snippets = []
    for locale, maxn in locale_limits.items():
        # Copy all the clients for each locale, tagging the client ID with
        # the locale.
        for cid, cdata in SAMPLE_CLIENT_DATA.items():
            tagged_cid = "{}_{}".format(locale, cid)
            client_snippets = generate_rows_for_client(tagged_cid, cdata)
            for s in client_snippets:
                s["locale"] = locale
            snippets.extend(client_snippets)

    # Add a dummy locale that should get dropped in processing.
    client_snippets = generate_rows_for_client(
        "client_fr", SAMPLE_CLIENT_DATA["client-4"]
    )
    for s in client_snippets:
        s["locale"] = "fr"
    snippets.extend(client_snippets)

    df = generate_data(snippets)
    df.createOrReplaceTempView("clients_daily")
    return df


@pytest.fixture
def short_client_df(client_addons_df):
    """A condensed version of the `clients_daily` data."""
    df = client_addons_df.where("locale in ('en-US', 'de')")
    df.createOrReplaceTempView("clients_daily")
    return df


@pytest.fixture
def multi_day_client_df(client_addons_df):
    """A single-locale version of the `clients_daily` data."""
    df = (
        client_addons_df.where("locale = 'en-US'")
        .withColumn("client_id", substring_index("client_id", "_", -1))
        .where(substring_index("client_id", "-", -1).isin([1, 2, 3, 4, 5]))
    )
    df.createOrReplaceTempView("clients_daily")
    return df


@pytest.fixture
def mock_sql_rand(spark):
    """Patch Spark SQL `RAND` to return a constant value.

    This will ensure deterministic selection of the add-on subsets, assuming
    that sorting maintains the ordering of `active_addons`.
    """

    def mock_rand():
        return 1

    spark.udf.register("rand", mock_rand, IntegerType())
    yield
    # Afterwards, reset all temporary catalog items to undo the patching.
    spark.catalog._reset()


@pytest.fixture()
def mock_rlaplace(monkeypatch):
    """Patch the Laplace noise generation to make results deterministic."""

    def mock_noise(scale, size):
        """Return `size` copies of the `scale` parameter."""
        return np_repeat(scale, size)

    monkeypatch.setattr(taar_locale, "rlaplace", mock_noise)


@pytest.fixture()
def mock_amo_whitelist(monkeypatch, short_whitelist):
    """Patch the whitelist loader to just use the one defined here."""
    monkeypatch.setattr(
        taar_locale, "load_amo_curated_whitelist", lambda: short_whitelist
    )


@pytest.fixture()
def mock_today(monkeypatch):
    """Patch `date.today()` to 2019-01-15, the latest date in the dataset."""

    class MockDate:
        @classmethod
        def today(cls):
            return date(2019, 1, 15)

    monkeypatch.setattr(taar_locale, "date", MockDate)


def spark_df_to_records(df):
    """Dump a Spark DF as a list of tuples representing rows ('records')."""
    return [tuple(r) for r in df.collect()]


def pandas_df_to_records(df):
    """Convert a Pandas DF to a list of tuples representing rows ('records')."""
    return df.to_records(index=False).tolist()


def same_rows(rows_list_1, rows_list_2):
    """Compare DF rows represented as lists of tuples ('records').

    Checks that the lists contain the same tuples (possibly in different orders).
    """
    return sorted(rows_list_1) == sorted(rows_list_2)


# =========================================================================== #


def test_get_client_addons(spark, multi_day_client_df):
    # Test date range limited on both ends.
    client_df = taar_locale.get_client_addons(
        spark, DATE_RANGE["start"], DATE_RANGE["end"]
    )
    actual = spark_df_to_records(client_df)
    expected = [
        # client-1 retains the 20190113 record, dropping the bad add-on
        ("en-US", "client-1", "guid-1"),
        ("en-US", "client-1", "guid-4"),
        # client-2 retains the 20190114 record, dropping the bad add-on
        ("en-US", "client-2", "guid-2"),
        # client-3 is dropped (outside date range)
        # client-4 retains the 20190112 record
        ("en-US", "client-4", "guid-1"),
        ("en-US", "client-4", "guid-2")
        # client-5 is dropped (no add-ons)
    ]
    assert same_rows(actual, expected)

    # Test date range limited by earliest only.
    client_df_no_end = taar_locale.get_client_addons(spark, DATE_RANGE["start"])
    actual = spark_df_to_records(client_df_no_end)
    expected = [
        # client-1 retains the 20190115 record, dropping the bad add-on
        ("en-US", "client-1", "guid-1"),
        ("en-US", "client-1", "guid-5"),
        # client-2 retains the 20190114 record, dropping the bad add-on
        ("en-US", "client-2", "guid-2"),
        # client-3 is dropped (outside date range)
        # client-4 retains the 20190112 record
        ("en-US", "client-4", "guid-1"),
        ("en-US", "client-4", "guid-2")
        # client-5 is dropped (no add-ons)
    ]
    assert same_rows(actual, expected)


def test_limit_client_addons(
    spark, client_addons_df, locale_limits, whitelist, mock_sql_rand
):
    client_df = taar_locale.get_client_addons(spark, DATE_RANGE["start"])
    limited_df = taar_locale.limit_client_addons(
        spark, client_df, locale_limits, whitelist
    )
    actual = spark_df_to_records(limited_df)
    expected = [
        # en-US clients keep 1 add-on
        # client-1 retains the 20190115 record
        ("en-US_client-1", "en-US", "guid-1"),
        # client-2 retains the 20190114 record
        ("en-US_client-2", "en-US", "guid-2"),
        # client-4 retains the 20190112 record
        ("en-US_client-4", "en-US", "guid-1"),
        # client-6 retains the 20190114 record
        ("en-US_client-6", "en-US", "guid-1"),
        # client-7 retains the 20190114 record
        ("en-US_client-7", "en-US", "guid-1"),
        # de clients keep 3 add-ons
        # client-1 retains the 20190115 record
        ("de_client-1", "de", "guid-1"),
        ("de_client-1", "de", "guid-5"),
        # client-2 retains the 20190114 record
        ("de_client-2", "de", "guid-2"),
        # client-4 retains the 20190112 record
        ("de_client-4", "de", "guid-1"),
        ("de_client-4", "de", "guid-2"),
        # client-6 retains the 20190114 record
        ("de_client-6", "de", "guid-1"),
        # client-7 retains the 20190114 record
        ("de_client-7", "de", "guid-1"),
        ("de_client-7", "de", "guid-2"),
        ("de_client-7", "de", "guid-3"),
        # pl clients keep 7 add-ons
        # client-1 retains the 20190115 record
        ("pl_client-1", "pl", "guid-1"),
        ("pl_client-1", "pl", "guid-5"),
        # client-2 retains the 20190114 record
        ("pl_client-2", "pl", "guid-2"),
        # client-4 retains the 20190112 record
        ("pl_client-4", "pl", "guid-1"),
        ("pl_client-4", "pl", "guid-2"),
        # client-6 retains the 20190114 record
        ("pl_client-6", "pl", "guid-1"),
        # client-7 retains the 20190114 record,
        # dropping the non-whitelisted add-on
        ("pl_client-7", "pl", "guid-1"),
        ("pl_client-7", "pl", "guid-2"),
        ("pl_client-7", "pl", "guid-3"),
        ("pl_client-7", "pl", "guid-4"),
        ("pl_client-7", "pl", "guid-5")
        # fr locale is dropped because it is not in the locale dict
    ]
    assert same_rows(actual, expected)


def test_compute_noisy_counts(
    addon_counts, short_locale_limits, short_whitelist, mock_rlaplace
):
    noisy_df = taar_locale.compute_noisy_counts(
        addon_counts, short_locale_limits, short_whitelist
    )
    actual = pandas_df_to_records(noisy_df)
    expected = [
        # The mock noisification adds the Laplace scale parameter
        # to the count directly.
        # The function is using taar_locale.EPSILON = 0.4.
        # For en-US, it is 1.0 / 0.4 = 2.5.
        ("en-US", "guid-1", 7.5),
        ("en-US", "guid-2", 4.5),
        ("en-US", "guid-3", 3.5),
        # The whitelist add-on which was not installed in any profile
        # gets added to the raw count DF with a count of 0.
        ("en-US", "guid-not-installed", 2.5),
        # For de, the scale parameter is 3.0 / 0.4 = 7.5.
        ("de", "guid-1", 10.5),
        ("de", "guid-2", 9.5),
        ("de", "guid-3", 11.5),
        ("de", "guid-not-installed", 7.5),
    ]
    assert same_rows(actual, expected)


def test_get_protected_and_dictionary(
    spark, short_client_df, mock_sql_rand, mock_rlaplace, mock_amo_whitelist, mock_today
):
    client_df = taar_locale.get_client_addons(spark, DATE_RANGE["start"])
    noisy_df = taar_locale.get_protected_locale_addon_counts(spark, client_df)
    actual = pandas_df_to_records(noisy_df)
    expected = [
        # The mock noisification adds the Laplace scale parameter
        # to the count directly.
        # The function is using taar_locale.EPSILON = 0.4 and setting each
        # locale limit to 1.
        # The scale parameters are 1.0 / 0.4 = 2.5.
        ("en-US", "guid-1", 6.5),
        ("en-US", "guid-2", 3.5),
        # The next two add-ons are not installed in the sample profiles,
        # so they have a raw count of 0.
        ("en-US", "guid-3", 2.5),
        ("en-US", "guid-not-installed", 2.5),
        ("de", "guid-1", 6.5),
        ("de", "guid-2", 3.5),
        ("de", "guid-3", 2.5),
        ("de", "guid-not-installed", 2.5),
    ]
    assert same_rows(actual, expected)

    # To test `generate_dictionary()`, we need to mock `date.today()`.
    # Make sure that the `dataset_num_days` arg works out to give us
    # `DATE_RANGE["start"] as the earliest date.
    start_date = datetime.strptime(DATE_RANGE["start"], "%Y%m%d").date()
    num_days = (taar_locale.date.today() - start_date).days
    # `generate_dictionary()` adds 1 to the number of days.
    num_days -= 1

    actual = taar_locale.generate_dictionary(spark, 3, num_days)
    # Noisy counts will be the same as listed in `expected` above.
    # To generate the dictionary, the min value (2.5) is subtracted in each locale,
    # counts are converted to relative proportions, and the list is ordered by
    # decreasing value.
    expected = {
        "en-US": [("guid-1", 0.8), ("guid-2", 0.2), ("guid-not-installed", 0.0)],
        "de": [("guid-1", 0.8), ("guid-2", 0.2), ("guid-not-installed", 0.0)],
    }
    assert actual == expected


def test_get_top_addons_by_locale(addon_count_neg):
    actual = taar_locale.get_top_addons_by_locale(addon_count_neg, 3)
    # Weights should sum to 1 within each locale.
    for addons in actual.values():
        assert sum([w for a, w in addons]) == 1.0

    expected = {
        "en-US": [("guid-1", 0.8), ("guid-2", 0.2), ("guid-3", 0.0)],
        "de": [("guid-3", 0.625), ("guid-2", 0.375), ("guid-1", 0.0)],
    }
    assert actual == expected
