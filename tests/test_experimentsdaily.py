import json
import os
import pytest
from pyspark.sql.types import StructType


EXPECTED_INTEGER_VALUES = {
    'active_addons_count_mean': 1227,
    'crashes_detected_content_sum': 7,
    'first_paint_mean': 816743,
    'pings_aggregated_by_this_row': 98,
    'search_count_all_sum': 49
}


@pytest.fixture
def sample_data(spark):
    root = os.path.dirname(__file__)
    schema_path = os.path.join(root, 'resources',
                               'experiments-summary.schema.json')
    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)
    rows_path = os.path.join(root, 'resources',
                             'experiments-summary-190-rows.json')
    # FAILFAST causes us to abort early if the data doesn't match
    # the given schema. Without this there was as very annoying
    # problem where dataframe.collect() would return an empty set.
    frame = spark.read.json(rows_path, schema, mode="FAILFAST")
    return frame


def test_rollup(sample_data):
    from mozetl.experimentsdaily import rollup
    from mozetl.clientsdaily.rollup import extract_search_counts
    assert sample_data.count() == 100

    client_id_count = sample_data.where("client_id is not null").count()
    assert client_id_count == sample_data.count()

    search_count_count = sample_data.where("search_counts is not null").count()
    assert search_count_count == 23

    searches_frame = extract_search_counts(sample_data)

    # Two rows are skipped for containing only unknown SAPs:
    #  833c2828-e84d-42d4-b245-8ea5783fdace
    #  1a3c4318-a5d9-42a4-9cae-7a68eec6e1eb
    assert searches_frame.count() == 98

    filtered = searches_frame.where("subsession_start_date LIKE '2017-09-08%'") \
                             .where("experiment_id = 'pref-flip-searchcomp1-pref3-1390584'") \
                             .where("search_count_all > 0") \
                             .orderBy("client_id")

    assert filtered.count() == 4
    f_collected = filtered.collect()
    assert len(f_collected) == 4
    assert f_collected[0].client_id == "259b7010-90b0-4edb-a0fb-510330e172ea"
    assert f_collected[0].search_count_all == 1
    assert f_collected[1].client_id == "ae7e68d4-7fb6-4f2b-b195-2095dea77bec"
    assert f_collected[1].search_count_all == 1
    assert f_collected[2].client_id == "bc627386-e4a8-448f-bbc5-f9dcf716cc1f"
    assert f_collected[2].search_count_all == 9
    assert f_collected[3].client_id == "e5d896cd-97a7-4b96-8944-3ec74c2fede3"
    assert f_collected[3].search_count_all == 1

    results = rollup.to_experiment_profile_day_aggregates(searches_frame)
    aggers = []
    for k in EXPECTED_INTEGER_VALUES:
        aggers.append("sum({})".format(k))
    agged = results.selectExpr(*aggers).collect()
    row = agged[0].asDict()
    for k in sorted(EXPECTED_INTEGER_VALUES.keys()):
        assert EXPECTED_INTEGER_VALUES[k] == row["sum({})".format(k)]
