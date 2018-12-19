from pyspark.sql import SQLContext
from mozetl.testpilot.containers import transform_testpilot_pings


def create_ping_rdd(sc, payload):
    return sc.parallelize(
        [
            {
                "payload": {
                    "test": "@testpilot-containers",
                    "other-ignored-field": "who cares",
                    "payload": payload,
                }
            }
        ]
    )


def create_row(overrides):
    keys = [
        "uuid",
        "userContextId",
        "clickedContainerTabCount",
        "eventSource",
        "event",
        "hiddenContainersCount",
        "shownContainersCount",
        "totalContainersCount",
        "totalContainerTabsCount",
        "totalNonContainerTabsCount",
        "pageRequestCount",
        "test",
    ]

    overrides["test"] = "@testpilot-containers"

    return {key: overrides.get(key, None) for key in keys}


def test_open_container_ping(row_to_dict, spark_context):
    input_payload = {
        "uuid": "a",
        "userContextId": 10,
        "clickedContainerTabCount": 20,
        "event": "open-tab",
        "eventSource": "tab-bar",
    }

    result_payload = input_payload
    result_payload["userContextId"] = "10"

    actual = transform_testpilot_pings(
        SQLContext(spark_context), create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_edit_container_ping(row_to_dict, spark_context):
    input_payload = {"uuid": "b", "event": "edit-containers"}

    actual = transform_testpilot_pings(
        SQLContext(spark_context), create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(input_payload)


def test_hide_container_ping(row_to_dict, spark_context):
    input_payload = {
        "uuid": "a",
        "userContextId": "firefox-default",
        "clickedContainerTabCount": 5,
        "event": "hide-tabs",
        "hiddenContainersCount": 2,
        "shownContainersCount": 3,
        "totalContainersCount": 5,
    }

    actual = transform_testpilot_pings(
        SQLContext(spark_context), create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(input_payload)


def test_close_container_tab_ping(row_to_dict, spark_context):
    input_payload = {
        "uuid": "a",
        "userContextId": "firefox-default",
        "event": "page-requests-completed-per-tab",
        "pageRequestCount": 2,
    }

    actual = transform_testpilot_pings(
        SQLContext(spark_context), create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(input_payload)
