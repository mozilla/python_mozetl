# Version of top_signatures_correlations.py for Dataproc image 2.2 (Spark 3.5, Python 3.11).
#
# Changes from the original:
#   - crashcorrelations is vendored into this repo instead of being cloned from master at
#     runtime. See crashcorrelations/README.md. It's submitted to Dataproc as a zip, see
#     "Submitting" below.
#   - The top words analysis is gone. It read user_comments, which has been entirely null
#     for years, so it never produced anything. That also removes the stemming download,
#     the addPyFile of porter2, and a second 30 day scan of the table.
#
# Submitting:
#   crashcorrelations is a plain package with no setup.py, so it can't be pip installed the
#   way the graphics jobs install their git dependencies. Zip it and pass it as --py-files,
#   which puts it on sys.path on the driver and the executors:
#
#     cd mozetl/symbolication && zip -r crashcorrelations.zip crashcorrelations -x '*.pyc'
#     gsutil cp crashcorrelations.zip gs://<bucket>/
#
#   moz_dataproc_pyspark_runner in telemetry-airflow doesn't expose py_files, so the DAG
#   needs either a small change there or an equivalent submit. See migration_plan.md.
#
# See migration_plan.md in this directory.
#
# pip install (set as PIP_PACKAGES in the DAG):
# boto3==1.35.36
# scipy==1.11.4
# google-cloud-storage==2.18.2
#
# scipy must not stay at 1.5.4: it has no wheels for the Python 3.11 on image 2.2 and the
# source build fails. The chi2_contingency and fisher_exact return shapes crash_deviations
# unpacks are unchanged in 1.11.
#
# The DAG also needs spark.jars pointed at a Spark 3.5 build of the BigQuery connector,
# gs://spark-lib/bigquery/spark-3.5-bigquery-0.44.2.jar. The spark-bigquery-latest_2.12.jar
# it currently uses is the Spark 2.4 line and won't load.

import argparse
import datetime
import hashlib
import os
from collections import defaultdict

from pyspark import SparkContext
from pyspark.sql import SparkSession

from google.cloud import storage

from crashcorrelations import crash_deviations, download_data, utils


# Number of top signatures to look at. Lower this if the job runs out of memory: the
# correlation code generates candidates per signature, so cost grows with this number.
TOP_SIGNATURE_COUNT = 200

# Number of days to look at to figure out top signatures
TOP_SIGNATURE_PERIOD_DAYS = 5

# Name of the GCS bucket where results are stored
RESULTS_GCS_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"


# workaround airflow not able to different schedules for tasks in a dag
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-on-days",
        nargs="+",
        type=int,
        required=True,
        help="Only run job on given days (0 is sunday)",
    )
    parser.add_argument(
        "--date",
        type=datetime.datetime.fromisoformat,
        default=datetime.datetime.now(datetime.timezone.utc),
        help="Run date, defaults to current date",
    )
    return parser.parse_args()


def remove_results_gcs(gcs_client, job_name):
    bucket = gcs_client.bucket(RESULTS_GCS_BUCKET)
    for key in bucket.list_blobs(prefix=job_name + "/data/"):
        key.delete()


def upload_results_gcs(gcs_client, job_name, directory):
    bucket = gcs_client.bucket(RESULTS_GCS_BUCKET)
    for root, dirs, files in os.walk(directory):
        for name in files:
            full_path = os.path.join(root, name)
            blob = bucket.blob(
                "{}/data/{}".format(
                    job_name, full_path[len(directory) + 1 :]  # noqa E203
                )
            )
            blob.content_encoding = "gzip"
            blob.upload_from_filename(full_path, content_type="application/json")


def main():
    args = parse_args()

    if args.date.isoweekday() % 7 not in args.run_on_days:
        print(
            f"Skipping because run date day of week"
            f" {args.date} is not in {args.run_on_days}"
        )
        return

    sc = SparkContext.getOrCreate()
    spark = SparkSession.builder.appName("top-signatures-correlations").getOrCreate()
    gcs_client = storage.Client()

    print(datetime.datetime.now(datetime.timezone.utc).isoformat())

    channels = ["release", "beta", "nightly", "esr"]
    channel_to_versions = {}

    for channel in channels:
        channel_to_versions[channel] = download_data.get_versions(channel)

    signatures = {}

    for channel in channels:
        signatures[channel] = download_data.get_top(
            TOP_SIGNATURE_COUNT,
            versions=channel_to_versions[channel],
            days=TOP_SIGNATURE_PERIOD_DAYS,
        )

    utils.rmdir("top-signatures-correlations_output")
    utils.mkdir("top-signatures-correlations_output")

    totals = {"date": str(utils.utc_today())}
    addon_related_signatures = defaultdict(list)

    for channel in channels:
        print(channel)

        utils.mkdir("top-signatures-correlations_output/" + channel)

        dataset = crash_deviations.get_telemetry_crashes(
            spark, versions=channel_to_versions[channel], days=TOP_SIGNATURE_PERIOD_DAYS
        )
        results, total_reference, total_groups = crash_deviations.find_deviations(
            sc, dataset, signatures=signatures[channel]
        )

        totals[channel] = total_reference

        for signature in signatures[channel]:
            if signature not in results:
                continue

            addons = [
                result
                for result in results[signature]
                if any(
                    "Addon" in elem
                    and float(result["count_group"]) / total_groups[signature]
                    > float(result["count_reference"]) / total_reference
                    for elem in result["item"].keys()
                    if len(result["item"]) == 1
                )
            ]

            if len(addons) > 0:
                addon_related_signatures[channel].append(
                    {
                        "signature": signature,
                        "addons": addons,
                        "total": total_groups[signature],
                    }
                )

            res = {"total": total_groups[signature], "results": results[signature]}

            utils.write_json(
                "top-signatures-correlations_output/"
                + channel
                + "/"
                + hashlib.sha1(signature.encode("utf-8")).hexdigest()
                + ".json.gz",
                res,
            )

    utils.write_json("top-signatures-correlations_output/all.json.gz", totals)
    utils.write_json(
        "top-signatures-correlations_output/addon_related_signatures.json.gz",
        addon_related_signatures,
    )

    print(datetime.datetime.now(datetime.timezone.utc).isoformat())

    # Will be uploaded under
    # https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/
    remove_results_gcs(gcs_client, "top-signatures-correlations")
    upload_results_gcs(
        gcs_client, "top-signatures-correlations", "top-signatures-correlations_output"
    )


if __name__ == "__main__":
    main()
