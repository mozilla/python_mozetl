import datetime as DT
import pytest
import os
from mozetl.schemas import MAIN_SUMMARY_SCHEMA


EXPECTED_INTEGER_VALUES = {
    'active_addons_count_mean': 3613,
    'crashes_detected_content_sum': 9,
    'first_paint_mean': 12802105,
    'pings_aggregated_by_this_row': 1122,
    'search_count_sum': 1043
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
    row = extracted.agg({'search_count': 'sum'}).collect()[0]
    total = row.asDict().values()[0]
    assert total == EXPECTED_INTEGER_VALUES['search_count_sum']


def test_extract_month(spark):
    from mozetl.clientsdaily import rollup

    frame = make_frame(spark)
    month_frame0 = rollup.extract_month(DT.date(2000, 1, 1), frame)
    count0 = month_frame0.count()
    assert count0 == 0
    month_frame1 = rollup.extract_month(DT.date(2017, 6, 1), frame)
    count1 = month_frame1.count()
    assert count1 == 68


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
