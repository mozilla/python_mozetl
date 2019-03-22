from functools import partial
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
import pytest

try:
    from unittest.mock import patch
    from unittest.mock import MagicMock
except Exception:
    from mock import patch
    from mock import MagicMock

TRAINING_SCHEMA = StructType(
    [
        StructField("client_id", StringType(), True),
        StructField("addon_ids", ArrayType(StringType()), True),
        StructField("geo_city", StringType(), True),
        StructField("subsession_length", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("os", StringType(), True),
        StructField("tab_open_count", StringType(), True),
        StructField("total_uri", StringType(), True),
        StructField("unique_tlds", StringType(), True),
    ]
)


client_row = {
    "client_id": "some_client",
    "addon_ids": [
        "addon_guid_1",
        "addon_guid_2",
        "addon_guid_3",
        "addon_guid_4",
        "addon_guid_5",
        "addon_guid_6",
        "addon_guid_7",
    ],
    "geo_city": "a_city",
    "subsession_length": 10.741944,
    "locale": "en",
    "os": "Windows_NT",
    "tab_open_count": 8,
    "total_uri": 25,
    "unique_tlds": 2,
}


@pytest.fixture
def base_document():
    return client_row


@pytest.fixture
def generate_df(dataframe_factory, base_document):
    return partial(
        dataframe_factory.create_dataframe, base=base_document, schema=TRAINING_SCHEMA
    )


def mock_load_recommenders():
    from mozetl.taar.taar_ensemble import COLLABORATIVE, SIMILARITY, LOCALE

    rec_map = {}

    class MockRecommender:
        def recommend(self, client_info, limit):
            return client_info["addon_ids"][:limit]

    rec_map[COLLABORATIVE] = MockRecommender()
    rec_map[SIMILARITY] = MockRecommender()
    rec_map[LOCALE] = MockRecommender()
    return rec_map


def mock_compute_regression(spark, rdd_list, regParam, elasticNetParam):
    """
    The compute_regression function in the taar_ensemble job
    doesn't seem to be able to execute properly from within the
    context of the pytest runner.  Conversion for RDD.toDF() seems to
    be the problem.
    """

    # There should be 4 items in this to match the number of folds
    assert len(rdd_list) == 4

    result = MagicMock()
    result.coefficients = [0.5, 0.7, 0.8]
    return result


@patch("mozetl.taar.taar_ensemble.cross_validation_split")
@patch("mozetl.taar.taar_ensemble.compute_regression", new=mock_compute_regression)
@patch("mozetl.taar.taar_ensemble.load_recommenders", new=mock_load_recommenders)
def test_transform(cross_split_mock, generate_df, spark):
    """
    This exercises the transformation function in the taar_ensemble
    job.

    The cross_validation split and the compute_regression functions
    have been mocked out as pyspark seems to crash under test when
    converting from RDD back to DataFrames


    The load_recommenders function has also been mocked out so that
    test cases do not rely upon TAAR to be installed for tests to run.
    """
    fold_1 = generate_df(
        [
            {"client_id": "client_1"},
            {"client_id": "client_2"},
            {"client_id": "client_3"},
            {"client_id": "client_4"},
        ]
    )
    fold_2 = generate_df(
        [
            {"client_id": "client_5"},
            {"client_id": "client_6"},
            {"client_id": "client_7"},
            {"client_id": "client_8"},
        ]
    )
    fold_3 = generate_df(
        [
            {"client_id": "client_9"},
            {"client_id": "client_10"},
            {"client_id": "client_11"},
            {"client_id": "client_12"},
        ]
    )
    fold_4 = generate_df(
        [
            {"client_id": "client_13"},
            {"client_id": "client_14"},
            {"client_id": "client_15"},
            {"client_id": "client_16"},
        ]
    )

    cross_split_mock.return_value = [fold_1, fold_2, fold_3, fold_4]

    taar_training = fold_1.union(fold_2).union(fold_3).union(fold_4)

    from mozetl.taar.taar_ensemble import transform
    from mozetl.taar.taar_ensemble import COLLABORATIVE, SIMILARITY, LOCALE

    coefs = transform(spark, taar_training, 0.1, 0.01)
    assert "ensemble_weights" in coefs
    weights = coefs["ensemble_weights"]
    assert len(weights) == 3
    assert weights[COLLABORATIVE] == 0.5
    assert weights[LOCALE] == 0.8
    assert weights[SIMILARITY] == 0.7
