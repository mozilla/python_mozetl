"""TEMPORARY. Instrumentation for the image 1.5 production job's NULL undercount.

Production reports impossible numbers for NULL-valued items. For signature
'EdgePool::Iterator::operator*' it publishes count_group 54 for {'moz_crash_reason': None}
when that signature has 121 crashes and all 121 have the field NULL. count_reference is
wrong the same way, 7,183 against a true 31,485. Non-NULL counts on the same signature are
exact.

Ten hypotheses were ruled out by re-running the counting code locally, on both Spark 2.4
and 3.5, against the same table and window. All produced the correct 31,485/121. That
leaves the parts of production those reruns did not hold constant, and the notable one is
the read itself: the image 1.5 DAG puts

    gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

on the classpath. "latest" is a floating pointer, so production reads BigQuery through a
connector build no local run reproduced. A connector that drops or mis-decodes nulls in a
column-pruned Arrow batch would produce exactly this shape: NULLs undercounted, non-NULLs
exact.

This module tests that from outside the library, which matters because production does not
use the vendored crashcorrelations. It runs

    os.system("git clone https://github.com/marco-c/crashcorrelations.git")

at import, so there is no TRACE hook to set. Everything here therefore works on the
dataframe that gets handed to find_deviations, and needs no change to the cloned library.

For each traced column it counts NULLs four independent ways on the same dataframe in the
same job:

  connector_pruned   the dataframe as find_deviations sees it, after get_telemetry_crashes
                     did its .select() column subset. This is the suspect path.
  connector_full     the same count from a fresh read with no .select(), so the connector
                     cannot prune. If this disagrees with connector_pruned, pruning is it.
  rdd_path           the flatMap/reduceByKey shape find_deviations uses, on the pruned
                     dataframe, to catch a fault in the RDD conversion rather than the read.
  sql_direct         a BigQuery SQL query over the same window, bypassing Spark completely.
                     This is ground truth.

sql_direct is the anchor: it is what COUNTIF gave locally. Whichever Spark number first
diverges from it names the layer at fault.

Also records the connector jar actually loaded and its version, which is the thing most
likely to explain a failure no local run reproduced.

Usage from the flat 1.5 driver, after `dataset` is built and before find_deviations:

    import count_trace_prod
    tracer = count_trace_prod.Tracer(channel, versions=channel_to_versions[channel], days=5)
    tracer.measure(spark, dataset)
    ...
    tracer.flush(bucket="benwu-correlations-output")

Writes to a count-trace-prod/ prefix, so it cannot disturb what Crash Stats reads.
Remove this file once the cause is known.
"""

import json
import traceback


# Columns production gets wrong, plus process_type and platform as controls. Production
# counts the controls correctly, so if they drift too the problem is broader than nulls.
TRACKED_COLUMNS = [
    "moz_crash_reason",
    "shutdown_progress",
    "startup_crash",
    "address",
    "reason",
    "cpu_microcode_version",
    "dom_ipc_enabled",
    "e10s_enabled",
    "process_type",
    "platform",
]

# The clearest case: 121 crashes, all 121 with moz_crash_reason NULL, production says 54.
WITNESS_SIGNATURE = "EdgePool::Iterator::operator*"

TABLE = "moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2"

TRACE_PREFIX = "count-trace-prod"


def _null_count(df, column):
    return df.filter(df[column].isNull()).count()


class Tracer(object):
    def __init__(self, channel, versions, days, versions_overridden=False):
        self.channel = channel
        self.versions = list(versions)
        self.days = days
        # Whether these versions came from --override-versions rather than product-details.
        # Recorded so a trace can't later be mistaken for one of a scheduled run.
        self.versions_overridden = versions_overridden
        self.measurements = []
        self.environment = {}
        self.errors = []

    def _record(self, method, column, reference, group, note=None):
        row = {
            "method": method,
            "column": column,
            "null_reference": reference,
            "null_witness_group": group,
        }
        if note:
            row["note"] = note
        self.measurements.append(row)

    def _fail(self, where, error):
        self.errors.append({
            "where": where,
            "error": repr(error),
            "traceback": traceback.format_exc(),
        })

    # -- environment ------------------------------------------------------------------

    def note_environment(self, spark, dataset):
        """Record the connector build in use and the schema the read produced.

        The image 1.5 DAG pins the connector to gs://spark-lib/.../latest, so this is the
        one production input that changes without any change to this repo.
        """
        try:
            jvm = spark.sparkContext._jvm
            info = {
                "spark_version": spark.version,
                "spark_jars": spark.conf.get("spark.jars", ""),
            }
            try:
                cls = jvm.java.lang.Class.forName(
                    "com.google.cloud.spark.bigquery.SparkBigQueryConnectorVersion"
                )
                info["connector_version_class"] = str(cls.toString())
            except Exception:
                pass
            # Where the connector class was actually loaded from, which names the jar.
            for name in (
                "com.google.cloud.spark.bigquery.v2.BigQueryDataSourceV2",
                "com.google.cloud.spark.bigquery.DefaultSource",
            ):
                try:
                    cls = jvm.java.lang.Class.forName(name)
                    location = cls.getProtectionDomain().getCodeSource().getLocation()
                    info["jar:" + name] = str(location.toString())
                except Exception:
                    continue
            self.environment = info
        except Exception as error:
            self._fail("note_environment", error)

        try:
            self.environment["dataset_columns"] = len(dataset.columns)
            self.environment["tracked_present"] = sorted(
                set(TRACKED_COLUMNS) & set(dataset.columns)
            )
            self.environment["dataset_schema_tracked"] = {
                field.name: field.dataType.simpleString()
                for field in dataset.schema.fields
                if field.name in TRACKED_COLUMNS
            }
        except Exception as error:
            self._fail("note_environment_schema", error)

    # -- the four measurements --------------------------------------------------------

    def measure(self, spark, dataset):
        """Count nulls four ways on the dataframe find_deviations is about to consume."""
        self.note_environment(spark, dataset)
        # Drive everything off the columns the read actually produced. TRACKED_COLUMNS is a
        # wish list and has at least one name this table doesn't have (e10s_enabled).
        columns = [c for c in TRACKED_COLUMNS if c in dataset.columns]
        self.environment["columns_measured"] = columns
        self.environment["columns_absent"] = [
            c for c in TRACKED_COLUMNS if c not in dataset.columns
        ]
        self._measure_pruned(dataset, columns)
        self._measure_full(spark, columns)
        self._measure_rdd(dataset, columns)
        self._measure_sql(spark, columns)

    def _measure_pruned(self, dataset, columns):
        """The suspect path: the dataframe as find_deviations receives it."""
        try:
            witness = dataset.filter(dataset["signature"] == WITNESS_SIGNATURE)
            self.environment["total_reference"] = dataset.count()
            self.environment["total_witness_group"] = witness.count()
            witness.cache()
            for column in columns:
                try:
                    self._record(
                        "connector_pruned",
                        column,
                        _null_count(dataset, column),
                        _null_count(witness, column),
                    )
                except Exception as error:
                    self._fail("connector_pruned:" + column, error)
            witness.unpersist()
        except Exception as error:
            self._fail("connector_pruned", error)

    def _measure_full(self, spark, columns):
        """A fresh read with no .select(), so the connector cannot prune columns.

        get_telemetry_crashes drops the android_* columns with a .select(), which lets the
        connector push a column subset down. This reads every column instead. A difference
        between this and connector_pruned points straight at the pushdown.
        """
        try:
            full = (
                spark.read.format("bigquery")
                .option("table", TABLE)
                .load()
                .where(self._window_predicate())
            )
            full = full.filter(
                (full["product"] == "Firefox") & (full["version"].isin(self.versions))
            )
            witness = full.filter(full["signature"] == WITNESS_SIGNATURE)
            witness.cache()
            self.environment["total_reference_unpruned"] = full.count()
            self.environment["total_witness_group_unpruned"] = witness.count()
            for column in columns:
                try:
                    self._record(
                        "connector_full",
                        column,
                        _null_count(full, column),
                        _null_count(witness, column),
                    )
                except Exception as error:
                    self._fail("connector_full:" + column, error)
            witness.unpersist()
        except Exception as error:
            self._fail("connector_full", error)

    def _measure_rdd(self, dataset, columns):
        """Count through the RDD, the shape find_deviations actually uses.

        find_deviations counts with flatMap/reduceByKey over dataset.rdd rather than with
        dataframe aggregates. If the dataframe agrees with SQL but this does not, the fault
        is in the RDD conversion, not the read.
        """
        try:
            if not columns:
                return

            def emit(row):
                out = []
                for column in columns:
                    if row[column] is None:
                        out.append(((column, "reference"), 1))
                        if row["signature"] == WITNESS_SIGNATURE:
                            out.append(((column, "witness"), 1))
                return out

            counted = dict(
                dataset.rdd.flatMap(emit).reduceByKey(lambda a, b: a + b).collect()
            )
            for column in columns:
                self._record(
                    "rdd_path",
                    column,
                    counted.get((column, "reference"), 0),
                    counted.get((column, "witness"), 0),
                )
        except Exception as error:
            self._fail("rdd_path", error)

    def _window_predicate(self):
        # Mirror get_telemetry_crashes exactly, via the same helper it uses, so the window
        # can't drift between this and the job.
        from crashcorrelations import utils

        return "crash_date >= to_date('{}')".format(
            utils.get_day(self.days).strftime("%Y-%m-%d")
        )

    def _measure_sql(self, spark, columns):
        """Ground truth: count in BigQuery, no Spark data path involved.

        One query per column. A single combined query is cheaper, but one unrecognised
        column name fails the whole statement and leaves no ground truth at all, which is
        exactly what happened on the first run (e10s_enabled is not in this table).
        `columns` therefore comes from the dataframe's own schema, and each column is
        queried separately so a surprise can only cost that one column.
        """
        try:
            from crashcorrelations import utils
            from google.cloud import bigquery

            start = utils.get_day(self.days).strftime("%Y-%m-%d")
            versions = ", ".join("'{}'".format(v) for v in self.versions)
            where = (
                " FROM `{}`".format(TABLE)
                + " WHERE crash_date >= DATE('{}')".format(start)
                + " AND product = 'Firefox'"
                + " AND version IN ({})".format(versions)
            )
            client = bigquery.Client()
            config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "witness", "STRING", WITNESS_SIGNATURE
                    )
                ]
            )

            totals_query = (
                "SELECT COUNT(*) AS total_reference, "
                "COUNTIF(signature = @witness) AS total_witness" + where
            )
            self.environment["sql_window"] = where
            try:
                row = list(client.query(totals_query, job_config=config).result())[0]
                self.environment["total_reference_sql"] = row["total_reference"]
                self.environment["total_witness_group_sql"] = row["total_witness"]
            except Exception as error:
                self._fail("sql_direct:totals", error)

            for column in columns:
                query = (
                    "SELECT COUNTIF({0} IS NULL) AS ref_null, "
                    "COUNTIF({0} IS NULL AND signature = @witness) AS grp_null".format(
                        column
                    )
                    + where
                )
                try:
                    row = list(client.query(query, job_config=config).result())[0]
                    self._record(
                        "sql_direct", column, row["ref_null"], row["grp_null"]
                    )
                except Exception as error:
                    self._fail("sql_direct:" + column, error)
        except Exception as error:
            self._fail("sql_direct", error)

    # -- output -----------------------------------------------------------------------

    def flush(self, bucket, prefix=TRACE_PREFIX):
        payload = {
            "channel": self.channel,
            "versions": self.versions,
            "versions_overridden": self.versions_overridden,
            "days": self.days,
            "witness_signature": WITNESS_SIGNATURE,
            "environment": self.environment,
            "measurements": self.measurements,
            "errors": self.errors,
        }
        text = json.dumps(payload, indent=1, default=str)

        # Print as well as upload: if the upload fails the driver log still has everything.
        print("COUNT_TRACE_PROD_BEGIN")
        print(text)
        print("COUNT_TRACE_PROD_END")

        try:
            from google.cloud import storage

            path = "{}/{}/trace.json".format(prefix, self.channel)
            storage.Client().bucket(bucket).blob(path).upload_from_string(
                text, content_type="application/json"
            )
            print("count_trace_prod: wrote gs://{}/{}".format(bucket, path))
        except Exception as error:
            print("count_trace_prod: upload failed: {!r}".format(error))
