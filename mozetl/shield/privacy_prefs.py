# coding: utf-8

# Started from mashing up:
#  https://gist.githubusercontent.com/ilanasegall/b3ce1aa0d3cc8c117a35b4a4fb9d4681/raw/c8a96e823cd56072e896e4c2d94c496306b59c8c/blok_df.py
# with:
# https://github.com/mozilla/python_mozetl/blob/689afa3d23229ca717422314c5a56abd83a85a0d/mozetl/testpilot/containers.py

from datetime import date, timedelta

from pyspark.sql.types import StringType
from moztelemetry.dataset import Dataset

from ..basic import convert_pings, DataFrameConfig


SHIELD_ADDON_ID = '@shield-study-privacy'

# Note that the JSON paths in this config are unusual for Shield pings.
# See https://bugzil.la/1387236 for more details and discussion

COMMON_COLUMN_CONFIGS = [
    ("client_id", "clientId", None, StringType()),
    ("branch", "payload/branch", None, StringType()),
    ("study_state", "payload/study_state", None, StringType()),
    ("event", "payload/event", None, StringType()),
    ("originDomain", "payload/originDomain", None, StringType()),
    ("breakage", "payload/breakage", None, StringType()),
    ("notes", "payload/notes", None, StringType()),
]

STUDY_STATE_DATAFRAME_COLUMN_CONFIGS = COMMON_COLUMN_CONFIGS + [
    ("study", "payload/study_name", None, StringType()),
]

STUDY_EVENT_DATAFRAME_COLUMN_CONFIGS = COMMON_COLUMN_CONFIGS + [
    ("study", "payload/study", None, StringType()),
]


def transform_state_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(STUDY_STATE_DATAFRAME_COLUMN_CONFIGS,
                        include_state_pings)
    )


def transform_event_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(STUDY_EVENT_DATAFRAME_COLUMN_CONFIGS,
                        include_event_pings)
    )


def include_event_pings(ping):
    return ping['payload/study'] == SHIELD_ADDON_ID


def include_state_pings(ping):
    return ping['payload/study_name'] == SHIELD_ADDON_ID


def etl_job(sc, sqlContext, submission_date=None, save=True):
    s3_path = 's3n://telemetry-parquet/harter/privacy_prefs_shield/v1'
    if submission_date is None:
        submission_date = (date.today() - timedelta(1)).strftime("%Y%m%d")

    pings = Dataset.from_source(
        "telemetry"
    ).where(
        docType="shield-study",
        submissionDate=submission_date,
        appName="Firefox",
    ).records(sc)

    transformed_event_pings = transform_event_pings(sqlContext, pings)

    transformed_state_pings = transform_state_pings(sqlContext, pings)

    transformed_pings = transformed_event_pings.union(transformed_state_pings)

    if save:
        path = s3_path + '/submission_date={}'.format(submission_date)
        transformed_pings.repartition(1).write.mode('overwrite').parquet(path)

    return transformed_pings
