import functools
import arrow
import pytest

from pyspark.sql.types import (
    ArrayType,
    StructField,
    StructType,
    StringType,
    LongType,
    MapType,
    IntegerType,
    BooleanType,
)

from mozetl.health_check.normandy import job

# Fix the tests around a specific time to ensure reproducibility. We
# choose a period in January 2017 because the examples I'm copying
# from engagement are all situated there.
SUBSESSION_START = arrow.get(2017, 1, 15).replace(tzinfo="utc")
ONE_DAY_BEFORE_SUBSESSION_START = arrow.get(2017, 1, 14).replace(tzinfo="utc")

SAMPLE_EVENT = {
    "app_name": "Firefox",
    "app_version": "57.0.0",
    "client_id": "client-id",
    "country": "US",
    "doc_type": "event",
    "document_id": "document-id",
    "event_category": "uptake.remotecontent.result",
    "event_map_values": {"source": "normandy/recipe/123"},
    "event_method": "uptake",
    "event_object": "component",
    "event_string_value": "success",
    "event_timestamp": SUBSESSION_START.timestamp * 10 ** 9,  # nanoseconds
    "locale": "en-US",
    "normalized_channel": "release",
    "os": "windows",
    "os_version": "6.1",
    "sample_id": "sample-id",
    "submission_date_s3": SUBSESSION_START.format("YYYYMMDD"),
    "subsession_length": 3600,
    "subsession_start_date": str(SUBSESSION_START),
    "sync_configured": False,
    "sync_count_desktop": 1,
    "sync_count_mobile": 1,
    "timestamp": SUBSESSION_START.timestamp * 10 ** 9,  # nanoseconds
}

EVENTS_SCHEMA = StructType(
    [
        StructField("app_name", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("doc_type", StringType(), True),
        StructField("document_id", StringType(), True),
        StructField("event_category", StringType(), True),
        StructField("event_map_values", MapType(StringType(), StringType()), True),
        StructField("event_method", StringType(), True),
        StructField("event_object", StringType(), True),
        StructField("event_string_value", StringType(), True),
        StructField("event_timestamp", LongType(), True),
        StructField("locale", StringType(), True),
        StructField("normalized_channel", StringType(), True),
        StructField("os", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("sample_id", StringType(), True),
        StructField("submission_date_s3", StringType(), False),
        StructField("subsession_length", LongType(), True),
        StructField("subsession_start_date", StringType(), True),
        StructField("sync_configured", BooleanType(), True),
        StructField("sync_count_desktop", IntegerType(), True),
        StructField("sync_count_mobile", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

SAMPLE_LONGITUDINAL = {
    "client_id": "1",
    "submission_date": [
        # My testing indicated that they appear reverse-chronologically
        "2017-01-14T00:00:00Z",
        "2017-01-13T00:00:00Z",
        "2017-01-12T00:00:00Z",
    ],
    "scalar_parent_normandy_recipe_freshness": {
        "1": [102, None, 100],
        "2": [None, None, None],
    },
}

LONGITUDINAL_SCHEMA = StructType(
    [
        StructField("client_id", StringType(), False),
        StructField(
            "scalar_parent_normandy_recipe_freshness",
            MapType(StringType(), ArrayType(LongType())),
            True,
        ),
        StructField("submission_date", ArrayType(StringType()), False),
    ]
)


@pytest.fixture
def generate_events(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe, base=SAMPLE_EVENT, schema=EVENTS_SCHEMA
    )


@pytest.fixture
def single_run_events(generate_events):
    recipe_1_success = {
        "event_map_values": {"source": "normandy/recipe/1"},
        "event_string_value": "success",
    }

    recipe_1_backoff = {
        "event_map_values": {"source": "normandy/recipe/1"},
        "event_string_value": "backoff",
    }

    recipe_2_success = {
        "event_map_values": {"source": "normandy/recipe/2"},
        "event_string_value": "success",
    }

    action_abc_success = {
        "event_map_values": {"source": "normandy/action/abc"},
        "event_string_value": "success",
    }

    action_abc_pre_execution_error = {
        "event_map_values": {"source": "normandy/action/abc"},
        "event_string_value": "pre_execution_error",
    }

    runner_success = {
        "event_map_values": {"source": "normandy/runner"},
        "event_string_value": "success",
    }

    return generate_events(
        [
            recipe_1_success,
            recipe_1_success,
            recipe_1_backoff,
            recipe_2_success,
            action_abc_success,
            action_abc_pre_execution_error,
            runner_success,
        ]
    )


@pytest.fixture
def generate_longitudinal(dataframe_factory):
    return functools.partial(
        dataframe_factory.create_dataframe,
        base=SAMPLE_LONGITUDINAL,
        schema=LONGITUDINAL_SCHEMA,
    )


@pytest.fixture
def some_clients_longitudinal(generate_longitudinal):
    # recipe 3 is absent; recipe 2 is all None
    sample_client = {}
    client_with_no_recipes = {
        "client_id": "2",
        "scalar_parent_normandy_recipe_freshness": {},
    }
    client_with_recipe_2_disappeared = {
        "client_id": "3",
        "scalar_parent_normandy_recipe_freshness": {"2": [None, None, 201]},
    }
    client_with_recipe_2_present = {
        "client_id": "4",
        "scalar_parent_normandy_recipe_freshness": {"2": [205, 204, 203]},
    }
    client_with_old_recipe_1 = {
        "client_id": "5",
        "scalar_parent_normandy_recipe_freshness": {"1": [100, 100, 100]},
    }
    client_with_recipe_3_and_1 = {
        "client_id": "5",
        "scalar_parent_normandy_recipe_freshness": {
            "1": [102, 102, 102],
            "3": [303, 304, 305],
        },
    }

    return generate_longitudinal(
        [
            sample_client,
            client_with_old_recipe_1,
            client_with_recipe_2_present,
            client_with_recipe_2_disappeared,
            client_with_no_recipes,
            client_with_recipe_3_and_1,
        ]
    )


def test_extract_tabulates(spark, single_run_events, some_clients_longitudinal):
    now = arrow.get(2017, 1, 15, 6, 0, 0, 0)
    res = job.extract(
        now,
        single_run_events,
        SUBSESSION_START,
        some_clients_longitudinal,
        ONE_DAY_BEFORE_SUBSESSION_START,
    )
    assert isinstance(res, job.NormandyHealthCheckResult)
    assert res.written_at == "2017-01-15T06:00:00+00:00"
    assert res.recipe_health == {
        "1": {"success": 2, "backoff": 1},
        "2": {"success": 1, "backoff": 0},
    }
    assert res.action_health == {"abc": {"success": 1, "pre_execution_error": 1}}
    assert res.runner_health == {"success": 1}
    assert res.status_totals == {"success": 5, "backoff": 1, "pre_execution_error": 1}
    assert res.recipe_versions == {"1": {102: 2, 100: 1}, "2": {205: 1}, "3": {303: 1}}
