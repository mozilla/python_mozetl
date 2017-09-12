import json
import os
import pytest
from pyspark.sql.types import StructType


# XXX Use the actual numbers.
EXPECTED_INTEGER_VALUES = {
    'active_addons_count_mean': 0,
    'crashes_detected_content_sum': 0,
    'first_paint_mean': 0,
    'pings_aggregated_by_this_row': 0,
    'search_count_all_sum': 0
}


@pytest.fixture
def make_frame(spark):
    root = os.path.dirname(__file__)

    schema_path = os.path.join(root, 'resources',
                               'experiments-summary.schema.json')
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = os.path.join(root, 'resources',
                             'experiments-summary-190-rows.json')
    frame = spark.read.json(rows_path, schema)
    return frame


@pytest.mark.skip(reason="extract_search_counts broken locally")
def test_rollup(spark):
    from mozetl.experimentsdaily import rollup
    from mozetl.clientsdaily.rollup import extract_search_counts
    frame = make_frame(spark)
    searches_frame = extract_search_counts(frame)

    # It's actually 0 due to something in local PyPspark
    assert searches_frame.count() == 100
    results = rollup.to_experiment_profile_day_aggregates(searches_frame)

    aggers = []
    for k in EXPECTED_INTEGER_VALUES:
        aggers.append("sum({})".format(k))
    agged = results.selectExpr(*aggers).collect()
    assert agged.toDict() == results
