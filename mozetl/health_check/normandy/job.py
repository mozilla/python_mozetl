"""Normandy Health Check

As part of our efforts to ensure that Normandy is continuing to
function correctly, this analysis calculates some metrics based on
uptake telemetry. This analysis is not itself responsible for
reporting its results. Instead, it writes a summary to OUTPUT_FILENAME
(a JSON file in S3). This file is monitored by a simple web service,
which reports status as a 200/500 "heartbeat". This heartbeat is
itself monitored using standard ops tools.

"""

import logging
import arrow
import boto3
import click
import json
from pyspark.sql import SparkSession, functions as F, types as T

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_BUCKET = "net-mozaws-prod-us-west-2-pipeline-analysis/"
OUTPUT_KEY = "eglassercamp/normandy-health-check.json"


class NormandyHealthCheckResult:
    """Result of running this job.

    This object will be written to OUTPUT_FILENAME."""

    def __init__(
        self,
        now,
        recipe_versions,
        recipe_health,
        action_health,
        runner_health,
        status_totals,
    ):
        """Constructor.

        :param Arrow now: when this result was computed
        :param recipe_versions: a dict of {recipe_id: {revision_id: count}}
        :param recipe_health: a dict of {recipe_id: {status: count}}
        :param action_health: a dict of {action_name: {status: count}}
        :param runner_health: a dict of {status: count} for Normandy runs (RecipeRunner)
        :param status_totals: a dict of {status: count} for all sources
        """
        self.written_at = now.isoformat()
        self.recipe_versions = recipe_versions
        self.recipe_health = recipe_health
        self.action_health = action_health
        self.runner_health = runner_health
        self.status_totals = status_totals


def crosstab_to_dict(df):
    """Converts a dataframe representing a crosstab into a nested dict.

    Each row in the dataframe will correspond to a key-value pair in
    the output. The key will be the first column; the value will be a
    dict of the remaining columns.
    """
    out = {}
    id_field = df.columns[0]
    for row in df.collect():
        d = row.asDict()
        key = d.pop(id_field)
        out[key] = d
    return out


def drop_normandy_prefix(dct):
    """Strips the 'normandy/foo/' part of the dict's keys."""
    return dict([("/".join(k.split("/")[2:]), v) for k, v in dct.items()])


def nest_keys(dict):
    """Convert a dict with keys {(a, b): ...} into a dict of dicts {a: {b: ...}}."""
    out = {}
    for ((k1, k2), v) in dict.items():
        if k1 not in out:
            out[k1] = {}
        out[k1][k2] = v

    return out


def extract(now, events, start_events, longitudinal, start_longitudinal):
    """Condense Normandy event information into a summary of recent health.

    :param events: dataframe pointing to events.v1
    :param start_events: start of time period from which to examine events data
    :param longitudinal: dataframe of longitudinal data
    :param start_longitudinal: start of time period from which to
      examine longitudinal data. Since longitudinal data is not real
      time, this may lag start_events by a day or more.
    :return: NormandyHealthCheckResult

    """
    events = events.filter(
        "submission_date_s3 >= '%s'" % start_events.strftime("%Y%m%d")
    ).filter("event_category='uptake.remotecontent.result'")
    recipe_health = drop_normandy_prefix(
        crosstab_to_dict(
            events.select(events.event_map_values["source"], events.event_string_value)
            .filter("event_map_values['source'] LIKE 'normandy/recipe/%'")
            # FIXME: https://bugzilla.mozilla.org/show_bug.cgi?id=1543817
            # We can maybe drop this after 68 (ESR) rolls out
            .replace("custom_2_error", "backoff")
            .crosstab("event_map_values[source]", "event_string_value")
        )
    )
    action_health = drop_normandy_prefix(
        crosstab_to_dict(
            events.select(events.event_map_values["source"], events.event_string_value)
            .filter("event_map_values['source'] LIKE 'normandy/action/%'")
            .crosstab("event_map_values[source]", "event_string_value")
        )
    )
    runner_nested_dict = crosstab_to_dict(
        events.select(events.event_map_values["source"], events.event_string_value)
        .filter("event_map_values['source'] = 'normandy/runner'")
        .crosstab("event_map_values[source]", "event_string_value")
    )
    assert len(runner_nested_dict) == 1
    # only one runner, so discard "outer" dict
    runner_health = runner_nested_dict["normandy/runner"]
    status_totals = dict(
        [
            (row[0], row[1])
            for row in events.groupBy(events.event_string_value)
            .agg(F.count("*"))
            .collect()
        ]
    )

    def myzip(submission_date, value):
        return zip(submission_date, value)

    zipped_schema = T.ArrayType(
        T.StructType(
            [
                T.StructField("submission_date", T.StringType()),
                T.StructField("value", T.LongType()),
            ]
        )
    )
    myzip = F.udf(myzip, zipped_schema)

    flat_scalar = (
        longitudinal.select(
            "submission_date", F.explode("scalar_parent_normandy_recipe_freshness")
        )
        .select("key", myzip("submission_date", "value").alias("time_value"))
        .select("key", F.explode("time_value").alias("time_value"))
    )
    flat_scalar = flat_scalar.filter(
        "time_value.submission_date >= '%s'" % start_longitudinal.strftime("%Y-%m-%d")
    ).filter("time_value.value IS NOT NULL")
    recipe_versions = nest_keys(
        dict(
            [
                ((row[0], row[1]), row[2])
                for row in flat_scalar.groupBy("key", "time_value.value")
                .count()
                .collect()
            ]
        )
    )
    return NormandyHealthCheckResult(
        now, recipe_versions, recipe_health, action_health, runner_health, status_totals
    )


@click.command()
@click.option("--period", default=6, help="length of the retention period in hours")
@click.option(
    "--slack", default=6, help="number of hours to account for submission latency"
)
def main(period, slack):
    """Assess Normandy health for the given period.
    """
    spark = SparkSession.builder.appName("normandy-health-check").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    s3 = boto3.client("s3")

    events = spark.read.parquet("s3://telemetry-parquet/events/v1/")
    now = arrow.get()
    events_start = now.shift(hours=-slack)
    longitudinal = spark.sql(
        """
        SELECT client_id, submission_date, scalar_parent_normandy_recipe_freshness
        FROM longitudinal
        """
    )
    longitudinal_start = events_start.shift(days=-1)
    result = extract(events, events_start, longitudinal, longitudinal_start)
    s3.put_object(Body=json.dump(result), Bucket=OUTPUT_BUCKET, Key=OUTPUT_KEY)


if __name__ == "__main__":
    main()
