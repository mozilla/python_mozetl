from pyspark.sql import SQLContext
from mozetl.shield.privacy_prefs import transform_shield_pings


def create_ping_rdd(sc, payload):
    return sc.parallelize([
        {'payload': {
            'study': '@shield-study-privacy',
            'other-ignored-field': 'who cares',
            'payload': payload
        }}
    ])


def create_row(overrides):
    keys = ["branch", "event", "originDomain", "breakage", "notes", "study"]

    overrides['study'] = '@shield-study-privacy'

    return {key: overrides.get(key, None) for key in keys}


def test_report_problem(row_to_dict, spark_context):
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

    actual = transform_shield_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_page_works(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.mozilla.org',
        'event': 'page-works',
        'breakage': None,
        'notes': None,
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_shield_pings(
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

    actual = transform_shield_pings(
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

    actual = transform_shield_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_disable(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'originDomain': 'www.paypal.com',
        'event': 'disable',
        'breakage': None,
        'notes': None,
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_shield_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)


def test_user_ended_study(row_to_dict, spark_context):
    input_payload = {
        'study': '@shield-study-privacy',
        'branch': 'thirdPartyCookiesOnlyFromVisited',
        'study_state': 'user-ended-study',
        'study_version': '0.0.1',
        'about': {
            '_src': 'addon',
            '_v': 2
        }
    }

    result_payload = input_payload

    actual = transform_shield_pings(
        SQLContext(spark_context),
        create_ping_rdd(spark_context, input_payload)
    ).take(1)[0]

    assert row_to_dict(actual) == create_row(result_payload)
