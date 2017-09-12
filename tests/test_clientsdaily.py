import pytest
import os
from mozetl.schemas import MAIN_SUMMARY_SCHEMA


EXPECTED_INTEGER_VALUES = {
    'active_addons_count_mean': 3613,
    'crashes_detected_content_sum': 9,
    'first_paint_mean': 12802105,
    'pings_aggregated_by_this_row': 1122,
    'search_count_all_sum': 1043
}


@pytest.fixture
def make_frame(spark):
    root = os.path.dirname(__file__)
    path = os.path.join(root, 'resources',
                        'main_summary-late-may-1123-rows-anonymized.json')
    frame = spark.read.json(path, MAIN_SUMMARY_SCHEMA)
    return frame


@pytest.fixture
def make_frame_with_extracts(spark):
    from mozetl.clientsdaily import rollup
    frame = make_frame(spark)
    return rollup.extract_search_counts(frame)


def test_extract_search_counts(spark):
    from mozetl.clientsdaily import rollup

    frame = make_frame(spark)
    extracted = rollup.extract_search_counts(frame)
    row = extracted.agg({'search_count_all': 'sum'}).collect()[0]
    total = row.asDict().values()[0]
    assert total == EXPECTED_INTEGER_VALUES['search_count_all_sum']


def test_to_profile_day_aggregates(spark):
    from mozetl.clientsdaily import rollup

    frame = make_frame_with_extracts(spark)
    clients_daily = rollup.to_profile_day_aggregates(frame)
    # Sum up the means and sums as calculated over 1123 rows,
    # one of which is a duplicate.
    aggd = dict([(k, 'sum') for k in EXPECTED_INTEGER_VALUES])
    result = clients_daily.agg(aggd).collect()[0]

    for k, expected in EXPECTED_INTEGER_VALUES.items():
        actual = int(result['sum({})'.format(k)])
        assert actual == expected


def test_profile_creation_date_fields(spark):
    from mozetl.clientsdaily import rollup

    frame = make_frame_with_extracts(spark)
    clients_daily = rollup.to_profile_day_aggregates(frame)

    # Spark's from_unixtime() is apparently sensitive to environment TZ
    # See https://issues.apache.org/jira/browse/SPARK-17971
    # There are therefore three possible expected results, depending on
    # the TZ setting of the system on which the tests run.
    expected_back = set([
        u'2014-12-16', u'2016-09-07',
        u'2016-05-12', u'2017-02-16',
        u'2012-11-17', u'2013-09-08',
        u'2017-02-12', u'2016-04-04',
        u'2017-04-25', u'2015-06-17'
    ])
    expected_utc = set([
        u'2014-12-17', u'2016-09-08',
        u'2016-05-13', u'2017-02-17',
        u'2012-11-18', u'2013-09-09',
        u'2017-02-13', u'2016-04-05',
        u'2017-04-26', u'2015-06-18'
    ])
    expected_forward = set([
        u'2014-12-18', u'2016-09-09',
        u'2016-05-14', u'2017-02-18',
        u'2012-11-19', u'2013-09-10',
        u'2017-02-14', u'2016-04-06',
        u'2017-04-27', u'2015-06-19'
    ])
    ten_pcds = clients_daily.select("profile_creation_date").take(10)
    actual1 = set([r.asDict().values()[0][:10] for r in ten_pcds])
    assert actual1 in (expected_back, expected_utc, expected_forward)

    expected2_back = [
        378, 894, 261, 1361, 101, 1656, 415, 29, 703, 102
    ]
    expected2_utc = [
        377, 893, 260, 1360, 100, 1655, 414, 28, 702, 101
    ]
    expected2_forward = [
        376, 892, 259, 1359, 99, 1654, 413, 27, 701, 100
    ]
    ten_pdas = clients_daily.select("profile_age_in_days").take(10)
    actual2 = [r.asDict().values()[0] for r in ten_pdas]
    assert actual2 in (expected2_back, expected2_utc, expected2_forward)


def test_sessions_started_on_this_day(spark):
    from mozetl.clientsdaily import rollup

    frame = make_frame_with_extracts(spark)
    clients_daily = rollup.to_profile_day_aggregates(frame)
    expected = [2, 0, 3, 2, 1, 0, 1, 0, 0, 3]
    ten_ssotds = clients_daily.select("sessions_started_on_this_day").take(10)
    actual = [r.asDict().values()[0] for r in ten_ssotds]
    assert actual == expected
