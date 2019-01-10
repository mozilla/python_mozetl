from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    BooleanType,
)

retention_schema = StructType(
    [
        StructField("client_id", StringType(), True),
        StructField("subsession_start", StringType(), True),
        StructField("profile_creation", StringType(), True),
        StructField("days_since_creation", LongType(), True),
        StructField("channel", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("geo", StringType(), True),
        StructField("distribution_id", StringType(), True),
        StructField("is_funnelcake", BooleanType(), True),
        StructField("source", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("campaign", StringType(), True),
        StructField("content", StringType(), True),
        StructField("sync_usage", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("usage_hours", DoubleType(), True),
        StructField("sum_squared_usage_hours", DoubleType(), True),
        StructField("total_uri_count", LongType(), True),
        StructField("unique_domains_count", LongType(), True),
    ]
)
