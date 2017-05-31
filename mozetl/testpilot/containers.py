from pyspark.sql.types import StringType, LongType

from ..basic import convert_pings, DataFrameConfig
from .utils import testpilot_etl_boilerplate


TESTPILOT_ADDON_ID = "@testpilot-containers"
SHIELD_ADDON_ID = "@shield-study-containers"
DATAFRAME_COLUMN_CONFIGS = [
    ("uuid", "payload/payload/uuid", None, StringType()),
    ("userContextId", "payload/payload/userContextId", None, StringType()),
    ("clickedContainerTabCount",
     "payload/payload/clickedContainerTabCount", None, LongType()),
    ("eventSource", "payload/payload/eventSource", None, StringType()),
    ("event", "payload/payload/event", None, StringType()),
    ("hiddenContainersCount", "payload/payload/hiddenContainersCount", None, LongType()),
    ("shownContainersCount", "payload/payload/shownContainersCount", None, LongType()),
    ("totalContainersCount", "payload/payload/totalContainersCount", None, LongType()),
    ("totalContainerTabsCount",
     "payload/payload/totalContainerTabsCount", None, LongType()),
    ("totalNonContainerTabsCount",
     "payload/payload/totalNonContainerTabsCount", None, LongType()),
    ("pageRequestCount", "payload/payload/pageRequestCount", None, LongType()),
    ("test", "payload/test", None, StringType()),
]


def transform_testpilot_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(DATAFRAME_COLUMN_CONFIGS, include_testpilot_pings)
    )


def transform_shield_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(DATAFRAME_COLUMN_CONFIGS, include_testpilot_and_shield_pings)
    )


def include_testpilot_pings(ping):
    return ping['payload/test'] == TESTPILOT_ADDON_ID


def include_testpilot_and_shield_pings(ping):
    return ping['payload/test'] in [TESTPILOT_ADDON_ID, SHIELD_ADDON_ID]


testpilot_etl_job = testpilot_etl_boilerplate(
    transform_testpilot_pings,
    's3n://telemetry-parquet/harter/containers_testpilottest/v2'
)

shield_etl_job = testpilot_etl_boilerplate(
    transform_shield_pings,
    's3n://telemetry-parquet/harter/containers_shield/v1'
)


def etl_job(sc, sqlContext, **kwargs):
    testpilot_etl_job(sc, sqlContext, **kwargs)
    shield_etl_job(sc, sqlContext, **kwargs)
