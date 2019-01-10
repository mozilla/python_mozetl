import ujson as json
import os
import pkg_resources

from pyspark.sql.types import StructType

import mozetl

SCHEMA_DIR = "json"
MAIN_SUMMARY_SCHEMA_BASENAME = "main_summary.v4.schema.json"
main_summary_path = os.path.join(SCHEMA_DIR, MAIN_SUMMARY_SCHEMA_BASENAME)

with pkg_resources.resource_stream(mozetl.__name__, main_summary_path) as f:
    d = json.load(f)
    MAIN_SUMMARY_SCHEMA = StructType.fromJson(d)
