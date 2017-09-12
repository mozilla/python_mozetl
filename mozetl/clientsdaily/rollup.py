import datetime as DT
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import click
from mozetl.utils import format_spark_path
from fields import MAIN_SUMMARY_FIELD_AGGREGATORS

LAG = 10
ACTIVITY_SUBMISSION_LAG = DT.timedelta(LAG)
ACTIVITY_DATE_COLUMN = F.expr(
    "substr(subsession_start_date, 1, 10)"
).alias("activity_date")
MAIN_SUMMARY_VERSION = 4
MAIN_SUMMARY_PATH = "s3://telemetry-parquet/main_summary/v{}".format(
    MAIN_SUMMARY_VERSION)
WRITE_VERSION = '5'
STORAGE_BUCKET = 'net-mozaws-prod-us-west-2-pipeline-analysis'
STORAGE_PREFIX = '/spenrose/clients-daily/v{}/'.format(WRITE_VERSION)
SEARCH_ACCESS_POINTS = [
    'abouthome', 'contextmenu', 'newtab', 'searchbar', 'system', 'urlbar'
]
SEARCH_ACCESS_COLUMN_TEMPLATE = 'search_count_{}'
SEARCH_ACCESS_COLUMNS = [
    SEARCH_ACCESS_COLUMN_TEMPLATE.format(sap)
    for sap in SEARCH_ACCESS_POINTS
]


def load_main_summary(spark):
    return (
        spark
        .read
        .option("mergeSchema", "true")
        .parquet(MAIN_SUMMARY_PATH)
    )


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
    two_columns = frame.select(F.col("document_id").alias("did"),
                               "search_counts")
    # First, each row becomes N rows, N == len(search_counts)
    exploded = two_columns.select(
        "did", F.explode("search_counts").alias("search_struct"))
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
        F.expr(if_template.format(sap)).alias(
            SEARCH_ACCESS_COLUMN_TEMPLATE.format(sap))
        for sap in SEARCH_ACCESS_POINTS
    ]
    unpacked = exploded.select(
        "did",
        F.expr("search_struct.count").alias("search_count_atom"),
        *if_expressions
    )

    # Collapse the exploded search_counts rows into a single output row.
    grouping_dict = dict([(c, "sum") for c in SEARCH_ACCESS_COLUMNS])
    grouping_dict["search_count_atom"] = "sum"
    grouped = unpacked.groupBy("did").agg(grouping_dict)
    extracted = grouped.select(
        "did", F.col("sum(search_count_atom)").alias("search_count_all"),
        *[
            F.col("sum({})".format(c)).alias(c)
            for c in SEARCH_ACCESS_COLUMNS
         ]
    )
    # Create a homologous output row for each input row
    # where search_counts is NULL.
    nulls = two_columns.select(
        "did").where(
        "search_counts is NULL").select(
        "did", F.lit(0).alias("search_count_all"),
        *[F.lit(0).alias(c) for c in SEARCH_ACCESS_COLUMNS]
    )
    intermediate = extracted.unionAll(nulls)
    result = frame.join(intermediate, frame.document_id == intermediate.did)
    return result


def extract_submission_window_for_activity_day(day, frame):
    """
    Extract rows with an activity_date of first_day and a submission_date
    between first_day and first_day + ACTIVITY_SUBMISSION_LAG.

    :day DT.date(Y, m, d)
    :frame DataFrame homologous with main_summary
    """
    frame = frame.select("*", ACTIVITY_DATE_COLUMN)
    activity_iso = day.isoformat()
    activity_submission_str = day.strftime('%Y%m%d')
    submission_end = day + ACTIVITY_SUBMISSION_LAG
    submission_end_str = submission_end.strftime('%Y%m%d')
    result = frame.where(
        "submission_date_s3 >= '{}'".format(activity_submission_str)) \
        .where("submission_date_s3 <= '{}'".format(submission_end_str)) \
        .where("activity_date='{}'".format(activity_iso))
    return result


def to_profile_day_aggregates(frame_with_extracts):
    if "activity_date" not in frame_with_extracts.columns:
        with_activity_date = frame_with_extracts.select(
            "*", ACTIVITY_DATE_COLUMN
        )
    else:
        with_activity_date = frame_with_extracts
    grouped = with_activity_date.groupby('client_id', 'activity_date')
    return grouped.agg(*MAIN_SUMMARY_FIELD_AGGREGATORS)


def write_one_activity_day(results, date, output_bucket,
                           output_prefix, partition_count):
    prefix = os.path.join(
        output_prefix, 'activity_date_s3={}'.format(date.isoformat()))
    output_path = format_spark_path(output_bucket, prefix)
    to_write = results.coalesce(partition_count)
    to_write.write.parquet(output_path, mode='overwrite')
    to_write.unpersist()


def get_partition_count_for_writing(is_sampled):
    '''
    Return a reasonable partition count.

    using_sample_id: boolean
    One day is O(140MB) if filtering down to a single sample_id, but
    O14GB) if not. Google reports 256MB < partition size < 1GB as ideal.
    '''
    if is_sampled:
        return 1
    return 25


@click.command()
@click.argument('--date')
@click.option('--input_bucket',
              default='telemetry-parquet',
              help='Bucket of the input dataset')
@click.option('--input_prefix',
              default='main_summary/v4',
              help='Prefix of the input dataset')
@click.option('--output_bucket',
              default=STORAGE_BUCKET,
              help='Bucket of the output dataset')
@click.option('--output_prefix',
              default=STORAGE_PREFIX,
              help='Prefix of the output dataset')
@click.option('--sample_id',
              default=None,
              help='Sample_id to restrict results to')
def main(date, input_bucket, input_prefix, output_bucket,
         output_prefix, sample_id):
    """
    Daywise rollup.
    """
    spark = (SparkSession
             .builder
             .appName("engagement_modeling")
             .getOrCreate())
    # Per https://issues.apache.org/jira/browse/PARQUET-142 ,
    # don't write _SUCCESS files, which interfere w/ReDash discovery
    spark.conf.set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
    date = DT.datetime.strptime(date, '%Y-%m-%d').date()
    main_summary = load_main_summary(spark)
    day_frame = extract_submission_window_for_activity_day(
        date, main_summary)
    if sample_id:
        clause = "sample_id='{}'".format(sample_id)
        day_frame = day_frame.where(clause)
    with_searches = extract_search_counts(day_frame)
    results = to_profile_day_aggregates(with_searches)
    partition_count = get_partition_count_for_writing(bool(sample_id))
    write_one_activity_day(results, date, output_bucket,
                           output_prefix, partition_count)


if __name__ == '__main__':
    main()
