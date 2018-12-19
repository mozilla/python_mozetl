import logging
from collections import namedtuple

from moztelemetry import get_pings_properties
from pyspark.sql.types import StructType, StructField


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


ColumnConfig = namedtuple(
    "ColumnConfig", ["name", "path", "cleaning_func", "struct_type"]
)


class DataFrameConfig(object):
    def __init__(self, col_configs, ping_filter):
        self.columns = [ColumnConfig(*col) for col in col_configs]
        self.ping_filter = ping_filter

    def toStructType(self):
        return StructType(
            map(lambda col: StructField(col.name, col.struct_type, True), self.columns)
        )

    def get_paths(self):
        return map(lambda col: col.path, self.columns)


def convert_pings(sqlContext, pings, data_frame_config):
    """Performs basic data pipelining on raw telemetry pings """
    filtered_pings = get_pings_properties(pings, data_frame_config.get_paths()).filter(
        data_frame_config.ping_filter
    )

    return convert_rdd(sqlContext, filtered_pings, data_frame_config)


def _build_cell(ping, column_config):
    """Take a json ping and a ColumnConfig and return a cleaned cell"""
    raw_value = ping[column_config.path]
    func = column_config.cleaning_func
    if func is not None:
        try:
            return func(raw_value)
        except Exception as e:
            logger.warning("Unable to apply transform on data '%s': %s", raw_value, e)
            return None
    else:
        return raw_value


def convert_rdd(sqlContext, raw_data, data_frame_config):
    """Performs basic data pipelining over an RDD, returning a DataFrame

    raw_data must have a map method.
    """

    def ping_to_row(ping):
        return [_build_cell(ping, col) for col in data_frame_config.columns]

    return sqlContext.createDataFrame(
        raw_data.map(ping_to_row), schema=data_frame_config.toStructType()
    )
