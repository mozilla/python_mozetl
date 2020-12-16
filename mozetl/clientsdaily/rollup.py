from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import click
from mozetl.utils import extract_submission_window_for_activity_day
from mozetl.utils import format_spark_path

SEARCH_ACCESS_POINTS = [
    "abouthome",
    "contextmenu",
    "newtab",
    "searchbar",
    "system",
    "urlbar",
]
SEARCH_ACCESS_COLUMN_TEMPLATE = "search_count_{}"
SEARCH_ACCESS_COLUMNS = [
    SEARCH_ACCESS_COLUMN_TEMPLATE.format(sap) for sap in SEARCH_ACCESS_POINTS
]


def load_main_summary(spark, input_bucket, input_prefix):
    main_summary_path = format_spark_path(input_bucket, input_prefix)
    return spark.read.option("mergeSchema", "true").parquet(main_summary_path)


def extract_search_counts(frame):
    """
    :frame DataFrame conforming to main_summary's schema.

    :return one row for each row in frame, replacing the nullable
    array-of-structs column "search_counts" with seven columns
        "search_count_{access_point}_sum":
    one for each valid SEARCH_ACCESS_POINT, plus one named "all"
    which is always a sum of the other six.

    All seven columns default to 0 and will be 0 if search_counts was NULL.
    Note that the Mozilla term of art "search access point", referring to
    GUI elements, is named "source" in main_summary.

    This routine is hairy because it generates a lot of SQL and Spark
    pseudo-SQL; see inline comments.

    TODO:
      Replace (JOIN with WHERE NULL) with fillna() to an array literal.
      Maybe use a PIVOT.
    """
    two_columns = frame.select(F.col("document_id").alias("did"), "search_counts")
    # First, each row becomes N rows, N == len(search_counts)
    exploded = two_columns.select(
        "did", F.explode("search_counts").alias("search_struct")
    )
    # Remove any rows where the values are corrupt
    exploded = exploded.where("search_struct.count > -1").where(
        "search_struct.source in %s" % str(tuple(SEARCH_ACCESS_POINTS))
    )  # This in clause looks like:
    # "search_struct.source in (
    # 'abouthome', 'contextmenu', 'newtab', 'searchbar', 'system', 'urlbar'
    # )"

    # Now we have clean search_count structs. Next block:
    # For each of the form Row(engine=u'engine', source=SAP, count=n):
    #    SELECT
    #     n as search_count_all,
    #     n as search_count_SAP, (one of the 6 above, such as 'newtab')
    #     0 as search_count_OTHER1
    #     ...
    #     0 as search_count_OTHER5
    if_template = "IF(search_struct.source = '{}', search_struct.count, 0)"
    if_expressions = [
        F.expr(if_template.format(sap)).alias(SEARCH_ACCESS_COLUMN_TEMPLATE.format(sap))
        for sap in SEARCH_ACCESS_POINTS
    ]
    unpacked = exploded.select(
        "did", F.expr("search_struct.count").alias("search_count_atom"), *if_expressions
    )

    # Collapse the exploded search_counts rows into a single output row.
    grouping_dict = dict([(c, "sum") for c in SEARCH_ACCESS_COLUMNS])
    grouping_dict["search_count_atom"] = "sum"
    grouped = unpacked.groupBy("did").agg(grouping_dict)
    extracted = grouped.select(
        "did",
        F.col("sum(search_count_atom)").alias("search_count_all"),
        *[F.col("sum({})".format(c)).alias(c) for c in SEARCH_ACCESS_COLUMNS],
    )
    # Create a homologous output row for each input row
    # where search_counts is NULL.
    nulls = (
        two_columns.select("did")
        .where("search_counts is NULL")
        .select(
            "did",
            F.lit(0).alias("search_count_all"),
            *[F.lit(0).alias(c) for c in SEARCH_ACCESS_COLUMNS],
        )
    )
    intermediate = extracted.unionAll(nulls)
    result = frame.join(intermediate, frame.document_id == intermediate.did)
    return result


def to_profile_day_aggregates(frame_with_extracts):
    from .fields import MAIN_SUMMARY_FIELD_AGGREGATORS

    if "activity_date" not in frame_with_extracts.columns:
        from .fields import ACTIVITY_DATE_COLUMN

        with_activity_date = frame_with_extracts.select("*", ACTIVITY_DATE_COLUMN)
    else:
        with_activity_date = frame_with_extracts
    if "geo_subdivision1" not in with_activity_date.columns:
        from .fields import NULL_STRING_COLUMN

        with_activity_date = with_activity_date.withColumn(
            "geo_subdivision1", NULL_STRING_COLUMN
        )
    if "geo_subdivision2" not in with_activity_date.columns:
        from .fields import NULL_STRING_COLUMN

        with_activity_date = with_activity_date.withColumn(
            "geo_subdivision2", NULL_STRING_COLUMN
        )
    grouped = with_activity_date.groupby("client_id", "activity_date")
    return grouped.agg(*MAIN_SUMMARY_FIELD_AGGREGATORS)


def write_one_activity_day(results, date, output_prefix, partition_count):
    output_path = "{}/activity_date_s3={}".format(
        output_prefix, date.strftime("%Y-%m-%d")
    )
    to_write = results.coalesce(partition_count)
    to_write.write.parquet(output_path, mode="overwrite")
    to_write.unpersist()


def get_partition_count_for_writing(is_sampled):
    """
    Return a reasonable partition count.

    using_sample_id: boolean
    One day is O(140MB) if filtering down to a single sample_id, but
    O(14GB) if not. Google reports 256MB < partition size < 1GB as ideal.
    """
    if is_sampled:
        return 1
    return 25


@click.command()
@click.option("--date", default=None, help="Start date to run on")
@click.option(
    "--input-bucket", default="telemetry-parquet", help="Bucket of the input dataset"
)
@click.option(
    "--input-prefix", default="main_summary/v4", help="Prefix of the input dataset"
)
@click.option(
    "--output-bucket",
    default="net-mozaws-prod-us-west-2-pipeline-analysis",
    help="Bucket of the output dataset",
)
@click.option(
    "--output-prefix", default="/clients_daily", help="Prefix of the output dataset"
)
@click.option("--output-version", default=5, help="Version of the output dataset")
@click.option("--sample-id", default=None, help="Sample_id to restrict results to")
@click.option(
    "--lag-days", default=10, help="Number of days to allow for submission latency"
)
def main(
    date,
    input_bucket,
    input_prefix,
    output_bucket,
    output_prefix,
    output_version,
    sample_id,
    lag_days,
):
    """
    Aggregate by (client_id, day) for a given day.

    Note that the target day will actually be `lag-days` days before
    the supplied date. In other words, if you pass in 2017-01-20 and
    set `lag-days` to 5, the aggregation will be processed for
    day 2017-01-15 (the resulting data will cover submission dates
    including the activity day itself plus 5 days of lag for a total
    of 6 days).
    """
    spark = SparkSession.builder.appName("clients_daily").getOrCreate()
    # Per https://issues.apache.org/jira/browse/PARQUET-142 ,
    # don't write _SUCCESS files, which interfere w/ReDash discovery
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    main_summary = load_main_summary(spark, input_bucket, input_prefix)
    day_frame, start_date = extract_submission_window_for_activity_day(
        main_summary, date, lag_days
    )
    if sample_id:
        day_frame = day_frame.where("sample_id = '{}'".format(sample_id))
    with_searches = extract_search_counts(day_frame)
    results = to_profile_day_aggregates(with_searches)
    partition_count = get_partition_count_for_writing(bool(sample_id))
    output_base_path = "{}/v{}/".format(
        format_spark_path(output_bucket, output_prefix), output_version
    )
    write_one_activity_day(results, start_date, output_base_path, partition_count)


if __name__ == "__main__":
    main()
