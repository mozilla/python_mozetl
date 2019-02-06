import datetime as dt
from functools import partial
from itertools import count


import numpy.random as nr
import pandas as pd
from pytest import fixture

from mozetl.bgbb import fit_airflow_job as fit_job, pred_airflow_job as pred_job


from bgbb.sql.sql_utils import S3_DAY_FMT, S3_DAY_FMT_DASH
from pyspark.sql.types import StringType, StructField, StructType

MODEL_WINDOW = 90
HO_WINDOW = 10
MODEL_START = pd.to_datetime("2018-10-10")
HO_START = MODEL_START + dt.timedelta(days=MODEL_WINDOW)
HO_ENDp1 = HO_START + dt.timedelta(days=HO_WINDOW + 1)
day_range = pd.date_range(MODEL_START, HO_ENDp1)

N_CLIENTS_IN_SAMPLE = 10
N_CLIENTS_ALL = 2 * N_CLIENTS_IN_SAMPLE


@fixture()
def create_clients_daily_table(spark, dataframe_factory):
    clientsdaily_schema = StructType(
        [
            StructField("app_name", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("client_id", StringType(), True),
            StructField("sample_id", StringType(), True),
            StructField("submission_date_s3", StringType(), True),
        ]
    )

    default_sample = {
        "app_name": "Firefox",
        "channel": "release",
        "client_id": "client-id",
        "sample_id": "1",
        "submission_date_s3": "20181220",
    }

    def generate_data(dataframe_factory):
        return partial(
            dataframe_factory.create_dataframe,
            base=default_sample,
            schema=clientsdaily_schema,
        )

    def coin_flip(p):
        return nr.binomial(1, p) == 1

    def gen_coins(n_coins, abgd=[1, 3, 4, 10]):
        a, b, g, d = abgd
        p = nr.beta(a, b, size=n_coins)
        th = nr.beta(g, d, size=n_coins)
        return p, th

    def client_2_daily_pings(client, days):
        client_days = []
        for day in days:
            client.update(submission_date_s3=day.strftime(S3_DAY_FMT))
            client_days.append(client.copy())
        return client_days

    def gen_client_days(
        client: dict, day_range, p: float, th: float, ensure_first=True
    ):
        """If `ensure_first`, add 1st day of day_range to their history
        so that every client will show up in `rfn`.
        """
        days_used_browser = []
        for day in day_range:
            # die coin
            if coin_flip(th):
                break
            return_today = coin_flip(p)
            if return_today:
                days_used_browser.append(day)
        if ensure_first and not days_used_browser:
            days_used_browser = [day_range[0]]
        return client_2_daily_pings(client, days_used_browser)

    def gen_client_dicts(n_clients_in_sample, abgd=[1, 1, 1, 10]):
        samples = ["1"] * n_clients_in_sample + ["2"] * n_clients_in_sample
        ps, θs = gen_coins(abgd=abgd, n_coins=len(samples))
        ps[0], θs[0] = 1, 0  # at least someone returns every day

        cids_rows = []
        for cid, samp, p, th in zip(count(), samples, ps, θs):
            row = default_sample.copy()
            row.update(dict(client_id=cid, sample_id=samp))

            cid_rows = gen_client_days(
                client=row, day_range=day_range, p=p, th=th
            )
            cids_rows.extend(cid_rows)
        return cids_rows

    cdaily_factory = generate_data(dataframe_factory)

    def gen_clients_daily(n_clients_in_sample, abgd=[1, 3, 1, 10], seed=0):
        nr.seed(seed)
        table_data = gen_client_dicts(
            n_clients_in_sample=n_clients_in_sample, abgd=abgd
        )

        dataframe = cdaily_factory(table_data)
        dataframe.createOrReplaceTempView("clients_daily")
        dataframe.cache()
        return dataframe

    gen_clients_daily(N_CLIENTS_IN_SAMPLE)


@fixture
def rfn(spark, create_clients_daily_table):
    create_clients_daily_table
    rfn = pred_job.extract(
        spark, model_win=MODEL_WINDOW, ho_start=HO_START.date(), sample_ids=[1]
    )
    rfn2 = pred_job.transform(rfn, return_preds=[7, 14])
    return rfn2


@fixture
def rfn_pd(rfn):
    return rfn.toPandas().set_index("client_id").sort_index()


def test_max_preds(rfn_pd):
    """
    Coins for client 1 were set for immortality:
    these should have highest predict prob(alive)
    and # of returns.
    """
    pred_cols = ["P7", "P14", "P_alive"]
    first_client_preds = rfn_pd.loc["0"][pred_cols]

    max_min = rfn_pd[pred_cols].apply(["max", "min"])

    assert (first_client_preds == max_min.loc["max"]).all()
    assert (first_client_preds > max_min.loc["min"]).all()
    assert len(rfn_pd) == N_CLIENTS_IN_SAMPLE


def test_get_params(spark, create_clients_daily_table):
    "TODO: test multiple preds"
    create_clients_daily_table
    ho_start = HO_START.strftime(S3_DAY_FMT_DASH)
    rfn, n_users = fit_job.extract(
        ho_start,
        spark,
        ho_win=HO_WINDOW,
        model_win=MODEL_WINDOW,
        samp_fraction=1.0,
        check_min_users=1,
        sample_ids=range(100),
    )
    assert n_users == N_CLIENTS_ALL, "Windows should contain all created users"
    params = fit_job.transform(rfn, spark, penalizer_coef=0.01)
    assert params.count() == 1, "Returns single row"
    return params
