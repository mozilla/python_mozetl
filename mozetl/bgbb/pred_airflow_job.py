from typing import List

import click
from pyspark.sql import SparkSession

from bgbb import BGBB
from bgbb.sql.bgbb_udfs import mk_n_returns_udf, mk_p_alive_udf
from bgbb.sql.sql_utils import run_rec_freq_spk

from mozetl.bgbb.bgbb_utils import PythonLiteralOption


def extract(spark, ho_start, model_win=90, sample_ids: List[int] = []):
    "TODO: increase ho_win to evaluate model performance"
    df, q = run_rec_freq_spk(
        ho_win=1,
        model_win=model_win,
        ho_start=ho_start,
        sample_ids=sample_ids,
        spark=spark,
    )
    return df


def transform(df, bgbb_params=[0.825, 0.68, 0.0876, 1.385], return_preds=[14]):
    """
    @return_preds: for each integer value `n`, make predictions
    for how many times a client is expected to return in the next `n`
    days.
    """
    bgbb = BGBB(params=bgbb_params)

    # Create/Apply UDFs
    p_alive = mk_p_alive_udf(bgbb, params=bgbb_params, alive_n_days_later=0)
    n_returns_udfs = [
        (
            "P{}".format(days),
            mk_n_returns_udf(
                bgbb, params=bgbb_params, return_in_next_n_days=days
            ),
        )
        for days in return_preds
    ]

    df2 = df.withColumn("P_alive", p_alive(df.Frequency, df.Recency, df.N))
    for days, udf in n_returns_udfs:
        df2 = df2.withColumn(days, udf(df.Frequency, df.Recency, df.N))
    return df2


def save(submission_date, bucket, prefix, df):
    path = "s3://{}/{}/submission_date_s3={}".format(
        bucket, prefix, submission_date
    )
    (
        df.write
        # .partitionBy("namespace", "doc_type", "doc_version")
        .parquet(path, mode="overwrite")
    )


@click.command("bgbb_pred")
@click.option("--submission-date", type=str, required=True)
@click.option("--model-win", type=int, default=120)
@click.option(
    "--sample-ids",
    cls=PythonLiteralOption,
    default="[]",
    help="List of integer sample ids or None",
)
@click.option(
    "--model-params",
    cls=PythonLiteralOption,
    # default='[0.825, 0.68, 0.0876, 1.385]'
)
@click.option(
    "--bucket", type=str, default="net-mozaws-prod-us-west-2-pipeline-analysis"
)
@click.option("--prefix", type=str, default="wbeard/bgbb_preds")
def main(submission_date, model_win, sample_ids, model_params, bucket, prefix):
    spark = SparkSession.builder.getOrCreate()

    df = extract(
        spark, submission_date, model_win=model_win, sample_ids=sample_ids
    )
    df2 = transform(df, model_params, return_preds=[7, 14, 21, 30])
    save(submission_date, bucket, prefix, df2)
    print("Success!")
