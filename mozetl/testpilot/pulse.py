import dateutil.parser
from pyspark.sql.types import (
    LongType,
    DoubleType,
    BooleanType,
    StringType,
    TimestampType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)
from pyspark.sql import Row

from ..basic import DataFrameConfig, convert_pings
from .utils import testpilot_etl_boilerplate


def _option_or_none(func):
    return lambda x: func(x) if x is not None else None


class Request(object):
    int_type = (_option_or_none(int), LongType())
    float_type = (_option_or_none(float), DoubleType())

    field_types = {
        "num": int_type,
        "cached": float_type,
        "cdn": float_type,
        "time": int_type,
    }

    # python3 work-around to keep field_types in scope since class variables
    # are not in the local scope of list comprehensions.
    _keys = sorted(field_types.keys())
    _dtypes = map(field_types.get, _keys)

    StructType = StructType(
        [StructField(x[0], x[1][1], True) for x in zip(_keys, _dtypes)]
    )

    def __init__(self, request_dict):
        args = {
            field: conversion(request_dict.get(field))
            for field, (conversion, sql_type) in Request.field_types.items()
        }
        self.row = Row(**args)


def _requests_to_rows(requests):
    out = {k: Request(v).row for k, v in requests.items()}
    return out


def transform_pings(sqlContext, pings):

    RequestsType = MapType(StringType(), Request.StructType)

    return convert_pings(
        sqlContext,
        pings,
        DataFrameConfig(
            [
                ("method", "payload/payload/method", None, StringType()),
                ("id", "payload/payload/id", None, StringType()),
                ("type", "payload/payload/type", None, StringType()),
                ("object", "payload/payload/object", None, StringType()),
                ("category", "payload/payload/category", None, StringType()),
                ("variant", "payload/payload/variant", None, StringType()),
                ("details", "payload/payload/details", None, StringType()),
                ("sentiment", "payload/payload/sentiment", None, LongType()),
                ("reason", "payload/payload/reason", None, StringType()),
                ("adBlocker", "payload/payload/adBlocker", None, BooleanType()),
                ("addons", "payload/payload/addons", None, ArrayType(StringType())),
                ("channel", "payload/payload/channel", None, StringType()),
                ("hostname", "payload/payload/hostname", None, StringType()),
                ("language", "payload/payload/language", None, StringType()),
                ("openTabs", "payload/payload/openTabs", None, LongType()),
                ("openWindows", "payload/payload/openWindows", None, LongType()),
                ("platform", "payload/payload/platform", None, StringType()),
                ("protocol", "payload/payload/protocol", None, StringType()),
                ("telemetryId", "payload/payload/telemetryId", None, StringType()),
                (
                    "timerContentLoaded",
                    "payload/payload/timerContentLoaded",
                    None,
                    LongType(),
                ),
                (
                    "timerFirstInteraction",
                    "payload/payload/timerFirstInteraction",
                    None,
                    LongType(),
                ),
                (
                    "timerFirstPaint",
                    "payload/payload/timerFirstPaint",
                    None,
                    LongType(),
                ),
                (
                    "timerWindowLoad",
                    "payload/payload/timerWindowLoad",
                    None,
                    LongType(),
                ),
                ("inner_timestamp", "payload/payload/timestamp", None, LongType()),
                ("fx_version", "payload/payload/fx_version", None, StringType()),
                (
                    "creation_date",
                    "creationDate",
                    dateutil.parser.parse,
                    TimestampType(),
                ),
                ("test", "payload/test", None, StringType()),
                ("variants", "payload/variants", None, StringType()),
                ("timestamp", "payload/timestamp", None, LongType()),
                ("version", "payload/version", None, StringType()),
                (
                    "requests",
                    "payload/payload/requests",
                    _requests_to_rows,
                    RequestsType,
                ),
                (
                    "disconnectRequests",
                    "payload/payload/disconnectRequests",
                    None,
                    LongType(),
                ),
                ("consoleErrors", "payload/payload/consoleErrors", None, LongType()),
                ("e10sStatus", "payload/payload/e10sStatus", None, LongType()),
                (
                    "e10sProcessCount",
                    "payload/payload/e10sProcessCount",
                    None,
                    LongType(),
                ),
                (
                    "trackingProtection",
                    "payload/payload/trackingProtection",
                    None,
                    BooleanType(),
                ),
            ],
            lambda ping: ping["payload/test"] == "pulse@mozilla.com",
        ),
    )


etl_job = testpilot_etl_boilerplate(
    transform_pings, "s3://telemetry-parquet/testpilot/txp_pulse/v1"
)
