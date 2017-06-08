import logging
import operator
from datetime import datetime

import arrow
import click
from pyspark.sql import types, SparkSession, functions as F
from pyspark.sql.window import Window

from mozetl.topline.schema import topline_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

countries = {
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS",
    "AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF", "BG",
    "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS", "BT",
    "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI",
    "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY",
    "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH",
    "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB",
    "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ",
    "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM", "HN", "HR", "HT",
    "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT",
    "JE", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP",
    "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS",
    "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH",
    "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU",
    "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF", "NG", "NI",
    "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA",
    "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG",
    "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST",
    "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK",
    "TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ", "UA", "UG",
    "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU",
    "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW"
}

seconds_per_hour = 60 * 60
seconds_per_day = seconds_per_hour * 24


def column_like(name, patterns, default):
    """ patterns: dict[label, list[match_expr]]"""
    # start with the pyspark.sql.functions
    op = F
    for label in patterns:
        cond = reduce(
            operator.__or__,
            [F.col(name).like(pat) for pat in patterns[label]]
        )
        op = op.when(cond, label)
    return op.otherwise(default)


def clean_input(dataframe, start, end):
    input_columns = [
        "client_id",
        "timestamp",
        "is_default_browser",
        "search_counts",
        "country",
        "profile_creation_date",
        "channel",
        "os",
        "hours",
    ]
    columns = {col: F.col(col) for col in input_columns}

    # normalize countries against a whitelist
    columns["country"] = (
        F.when(F.col("country").isin(countries), F.col("country"))
        .otherwise("Other")
        .alias("country")
    )

    # clean operating system based on CEP naming scheme
    pattern = {
        "Windows": ["Windows%", "WINNT%"],
        "Mac": ["Darwin%"],
        "Linux": ["%Linux%", "%BSD%", "%SunOS%"],
    }
    columns["os"] = column_like("os", pattern, "Other")

    # rename normalized channel to channel
    columns["channel"] = F.col("normalized_channel")

    # convert profile creation date into seconds (day -> seconds)
    columns["profile_creation_date"] = (
        F.when(F.col("profile_creation_date") >= 0,
               F.col("profile_creation_date") * seconds_per_day)
        .otherwise(0.0)
        .cast(types.DoubleType())
    )

    # generate hours of usage from subsession length (seconds -> hours)
    columns["hours"] = (
        F.when((F.col("subsession_length") >= 0) &
               (F.col("subsession_length") < 180 * seconds_per_day),
               F.col("subsession_length") / seconds_per_hour)
        .otherwise(0.0)
        .cast(types.DoubleType())
    )

    # clean the dataset
    clean = (
        dataframe
        .drop_duplicates(["document_id"])
        .where(F.col("submission_date_s3") >= start)
        .where(F.col("submission_date_s3") < end)
        .select([expr.alias(name) for name, expr in columns.iteritems()])
    )

    return clean


def search_aggregates(dataframe, attributes):
    # search engines to pivot against
    search_labels = ["google", "bing", "yahoo", "other"]

    # patterns to filter search engines
    patterns = {
        "google": ["%Google%", "%google%"],
        "bing": ["%Bing%", "%bing%"],
        "yahoo": ["%Yahoo%", "%yahoo%"],
    }

    s_engine = (
        column_like("search_count.engine", patterns, "other").alias("engine")
    )
    s_count = (
        F.when(F.col("search_count.count") > 0, F.col("search_count.count"))
        .otherwise(0)
        .alias("count")
    )

    # generate the search aggregates by exploding and pivoting
    search = (
        dataframe
        .withColumn("search_count", F.explode("search_counts"))
        .select("country", "channel", "os", s_engine, s_count)
        .groupBy(attributes)
        .pivot("engine", search_labels)
        .agg(F.sum("count"))
        .na.fill(0, search_labels)
    )

    return search


def hours_aggregates(dataframe, attributes):
    """ Aggregate hours over the set of attributes"""
    # simple aggregate
    hours = dataframe.groupBy(attributes).agg(F.sum("hours").alias("hours"))
    return hours


def client_aggregates(dataframe, timestamp, attributes):
    """Aggregates clients by properties such as being new or set as default. """

    select_expr = {col: F.col(col) for col in attributes}

    select_expr["new_client"] = F.when(
        F.col("profile_creation_date") >= timestamp, 1).otherwise(0)

    select_expr["default_client"] = F.when(
        F.col("is_default_browser"), 1).otherwise(0)

    select_expr["clientid_rank"] = F.row_number().over(
        Window
        .partitionBy("client_id")
        .orderBy(F.desc("timestamp"))
    )

    clients = (
        dataframe
        .select([expr.alias(name) for name, expr in select_expr.iteritems()])
        .where("clientid_rank = 1")
        .groupBy(attributes)
        .agg(
            F.count("*").alias("actives"),
            F.sum("new_client").alias("new_records"),
            F.sum("default_client").alias("default")
        )
    )

    return clients


def transform(dataframe, start, mode):
    # attributes that break down the aggregates
    attributes = ["country", "channel", "os"]

    end = get_end_date(start, mode)
    # clean the dataset
    df = clean_input(dataframe, start, end)

    # find the timestamp in seconds to find new profiles
    report_delta = datetime.strptime(start, "%Y%m%d") - datetime(1970, 1, 1)
    report_timestamp = report_delta.total_seconds()

    # generate aggregates
    clients = client_aggregates(df, report_timestamp, attributes)
    searches = search_aggregates(df, attributes)
    hours = hours_aggregates(df, attributes)

    # take the outer join of all aggregates and replace null values with zeros
    return (
        clients
        .join(searches, attributes, "outer")
        .join(hours, attributes, "outer")
        .withColumnRenamed('country', 'geo')
        .withColumn('crashes', F.lit(0L))
        .na.fill(0)
    )


def get_end_date(ds_start, period):
    """ Return the end date given the start date and period. """
    date_start = arrow.get(ds_start, "YYYYMMDD")
    if period == "monthly":
        date_end = date_start.replace(months=+1)
    else:
        date_end = date_start.replace(days=+7)
    ds_end = date_end.format("YYYYMMDD")

    return ds_end


def format_spark_path(bucket, prefix):
    return "s3://{}/{}".format(bucket, prefix)


def extract(spark, path):
    """Extract the source dataframe from the spark compatible path.

    spark: SparkSession
    path: path to parquet files in s3
    ds_start: inclusive date
    """
    return (
        spark.read
        .option("mergeSchema", "true")
        .parquet(path)
    )


def save(dataframe, bucket, prefix, version, mode, start_date):
    prefix = (
        "{}/v{}/mode={}/report_start={}"
        .format(prefix, version, mode, start_date)
    )
    location = format_spark_path(bucket, prefix)
    logger.info("Writing topline summary to {}".format(location))

    # report start is implicit in the partition path
    fields = [col for col in topline_schema.names if col != "report_start"]
    (
        dataframe
        .select(fields)
        .repartition(1)
        .write
        .parquet(location)
    )


@click.command()
@click.argument('start_date')
@click.argument('mode', type=click.Choice(['weekly', 'monthly']))
@click.argument('bucket')
@click.argument('prefix')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
def main(start_date, mode, bucket, prefix, input_bucket, input_prefix):
    spark = (
        SparkSession
        .builder
        .appName("topline_summary")
        .getOrCreate()
    )

    version = 1
    source_path = format_spark_path(input_bucket, input_prefix)

    logger.info("Loading main_summary into memory...")
    main_summary = extract(spark, source_path)

    logger.info("Running the topline summary...")
    rollup = transform(main_summary, start_date, mode)

    logger.info("Saving rollup to disk...")
    save(rollup, bucket, prefix, version, mode, start_date)
