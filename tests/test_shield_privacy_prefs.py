from pyspark.sql import SQLContext
from mozetl.shield.privacy_prefs import (transform_state_pings,
                                         transform_event_pings)


def create_ping_rdd(sc, payload):
    return sc.parallelize([
        {
            'clientId': 'aa',
            'other-ignored-field': 'who cares',
            'payload': payload
        }
    ])


def create_row(overrides):
    keys = ["client_id", "branch", "event", "originDomain", "breakage",
            "notes", "study", "study_state"]

    # simulate transforuming study_name into study field
    if 'study_name' in overrides:
        del overrides['study_name']
        overrides['study'] = '@shield-study-privacy'
    overrides['client_id'] = 'aa'

    return {key: overrides.get(key, None) for key in keys}


def test_study_state_value(row_to_dict, spark_context):
    input_payload = {
      "study_name": "@shield-study-privacy",
      "branch": "firstPartyIsolationOpenerAccess",
      "study_state": "running",
      "study_version": "0.0.4",
      "about": {
        "_src": "shield",
        "_v": 2
      }
    }

    result_payload = input_payload

    actual = transform_state_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_event_value_problem(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.paypal.com',
        'event': 'page-problem',
        'breakage': None,
        'notes': None,
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_event_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_page_breakage(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.paypal.com',
        'event': 'breakage',
        'breakage': 'other',
        'notes': None,
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_event_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_notes(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.paypal.com',
        'event': 'notes',
        'breakage': 'other',
        'notes': 'Paypal prompted me for Reader Mode. WTF?',
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_event_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_combine_state_and_event(row_to_dict, spark_context):
    state_payload = {
      "study_name": "@shield-study-privacy",
      "branch": "firstPartyIsolationOpenerAccess",
      "study_state": "running",
      "study_version": "0.0.4",
      "about": {
        "_src": "shield",
        "_v": 2
      }
    }
    event_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.paypal.com',
        'event': 'disable',
        'breakage': None,
        'notes': None,
        'study_version': '0.0.4',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }
    transformed_event_ping = transform_event_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, event_payload)
    )
    transformed_state_ping = transform_state_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, state_payload)
    )

    transformed_pings = transformed_event_ping.union(transformed_state_ping)

    actual_rows = transformed_pings.take(2)

    for actual_row in actual_rows:
        assert (row_to_dict(actual_row) == create_row(state_payload) or
                row_to_dict(actual_row) == create_row(event_payload))
