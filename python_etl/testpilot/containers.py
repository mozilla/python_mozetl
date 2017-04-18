from python_etl.basic_etl import convert_pings, DataFrameConfig
from utils import testpilot_etl_boilerplate

from pyspark.sql.types import StringType, LongType


def transform_pings(sqlContext, pings):
    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig([
            ("uuid", "payload/payload/uuid", None, StringType()),
            ("userContextId", "payload/payload/userContextId", None, StringType()),
            ("clickedContainerTabCount", "payload/payload/clickedContainerTabCount", None, LongType()),
            ("eventSource", "payload/payload/eventSource", None, StringType()),
            ("event", "payload/payload/event", None, StringType()),
            ("hiddenContainersCount", "payload/payload/hiddenContainersCount", None, LongType()),
            ("shownContainersCount", "payload/payload/shownContainersCount", None, LongType()),
            ("totalContainersCount", "payload/payload/totalContainersCount", None, LongType()),
            ("totalContainerTabsCount", "payload/payload/totalContainerTabsCount", None, LongType()),
            ("totalNonContainerTabsCount", "payload/payload/totalNonContainerTabsCount", None, LongType()),
            ("test", "payload/test", None, StringType()),
        ], lambda ping: ping['payload/test'] == "@testpilot-containers")
    )


etl_job = testpilot_etl_boilerplate(
    transform_pings,
    's3n://telemetry-parquet/harter/containers_testpilottest/v1'
)
