# coding: utf-8

# Started from mashing up:
#  https://gist.githubusercontent.com/ilanasegall/b3ce1aa0d3cc8c117a35b4a4fb9d4681/raw/c8a96e823cd56072e896e4c2d94c496306b59c8c/blok_df.py
# with:
# https://github.com/mozilla/python_mozetl/blob/689afa3d23229ca717422314c5a56abd83a85a0d/mozetl/testpilot/containers.py

from pyspark.sql.types import StringType

from ..basic import convert_pings, DataFrameConfig
from .utils import shield_etl_boilerplate


SHIELD_ADDON_ID = '@shield-study-privacy'
DATAFRAME_COLUMN_CONFIGS = [
    ("branch", "payload/payload/branch", None, StringType()),
    ("event", "payload/payload/event", None, StringType()),
    ("originDomain", "payload/payload/originDomain", None, StringType()),
    ("breakage", "payload/payload/breakage", None, StringType()),
    ("notes", "payload/payload/notes", None, StringType()),
    ("study", "payload/payload/study", None, StringType()),
]


def transform_shield_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(DATAFRAME_COLUMN_CONFIGS, include_shield_pings)
    )


def include_shield_pings(ping):
    return ping['payload/payload/study'] == SHIELD_ADDON_ID


def etl_job(sc, sqlContext, **kwargs):
    return shield_etl_boilerplate(
        transform_shield_pings,
        's3n://telemetry-parquet/harter/privacy_prefs_shield/v1'
    )
