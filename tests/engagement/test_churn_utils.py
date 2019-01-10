import pytest
from pyspark.sql import functions as F

from mozetl.engagement.churn import utils


def test_to_datetime(spark):
    df = spark.createDataFrame([{"date": "20000101"}])
    expect = "2000-01-01 00:00:00"

    expr = utils.to_datetime("date", "yyyyMMdd")
    actual = df.select(expr).collect()[0][0]

    assert actual == expect


@pytest.fixture()
def test_df(dataframe_factory):
    return dataframe_factory.create_dataframe(
        [None] * 2, {"alpha": "hello", "omega": "world", "delta": "!"}
    )


@pytest.fixture()
def select_expr_dict():
    return {
        "alpha": None,  # key is the column name
        "beta": "omega",  # reference to another column
        "gamma": "upper(alpha)",  # string expression
        "delta": F.col("delta"),  # column expression
    }


def test_preprocess_col_expr(select_expr_dict, test_df, row_to_dict):
    expect = {"alpha": "hello", "beta": "world", "gamma": "HELLO", "delta": "!"}

    actual = utils.preprocess_col_expr(select_expr_dict)

    assert (
        row_to_dict(
            test_df.select([v.alias(k) for k, v in list(actual.items())]).first()
        )
        == expect
    )


def test_build_col_expr(select_expr_dict, test_df, row_to_dict):
    expect = {"alpha": "hello", "beta": "world", "gamma": "HELLO", "delta": "!"}

    actual = utils.build_col_expr(select_expr_dict)

    assert row_to_dict(test_df.select(actual).first()) == expect
