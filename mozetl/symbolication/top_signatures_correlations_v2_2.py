# Version of top_signatures_correlations.py for Dataproc image 2.2 (Spark 3.5, Python 3.11).
#
# Changes from the original:
#   - crashcorrelations is vendored into this repo instead of being cloned from master at
#     runtime. See crashcorrelations/README.md. It's pip installed onto the cluster from
#     this repo, see "Submitting" below.
#   - The top words analysis is gone. It read user_comments, which has been entirely null
#     for years, so it never produced anything. That also removes the stemming download,
#     the addPyFile of porter2, and a second 30 day scan of the table.
#   - boto3 is gone. The vendored utils.py imported it at module load for two S3 functions
#     that nothing called, left behind when this job moved to GCS. The job writes to GCS via
#     google-cloud-storage and needs no AWS credentials.
#
# Submitting:
#   The vendored crashcorrelations has a pyproject.toml, so it installs straight from the
#   repo. The DAG puts this in PIP_PACKAGES, pointing at the same ref it fetches this driver
#   from so the two can't drift:
#
#     pip install "git+https://github.com/mozilla/python_mozetl.git\
#     #subdirectory=mozetl/symbolication/crashcorrelations"
#
#   No egg= fragment: pip doesn't need it, and an & in PIP_PACKAGES breaks the Dataproc
#   pip-install init action, which expands the value unquoted.
#
# See migration_plan.md in this directory.
#
# pip install (set as PIP_PACKAGES in the DAG):
# scipy==1.11.4
# google-cloud-storage==2.18.2
#
# scipy must not stay at 1.5.4: it has no wheels for the Python 3.11 on image 2.2 and the
# source build fails. The chi2_contingency and fisher_exact return shapes crash_deviations
# unpacks are unchanged in 1.11.
#
# Unlike the image 1.5 job, the DAG must not set spark.jars for the BigQuery connector.
# Image 2.2 ships one in /usr/lib/spark/jars, and a second one on the classpath makes the
# read fail with "Multiple sources found for bigquery" because both register that name.

import argparse
import datetime
import hashlib
import os
from collections import defaultdict

from pyspark import SparkContext
from pyspark.sql import SparkSession

from google.cloud import storage

from crashcorrelations import crash_deviations, download_data, utils

# TEMPORARY, see count_trace.py. Vendored next to this file so the Dataproc
# submission picks it up from the same directory.
import count_trace


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
    parser.add_argument(
        "--trace-counts",
        action="store_true",
        help="TEMPORARY. Trace the level 1 counts for the release channel and write them to "
        "a count-trace/ prefix, to diagnose the production NULL undercount. See "
        "count_trace.py.",
    )
    parser.add_argument(
        "--trace-bucket",
        default=None,
        help="Bucket for --trace-counts output. Defaults to --results-bucket.",
    )
    parser.add_argument(
        "--results-bucket",
        default=RESULTS_GCS_BUCKET,
        help="GCS bucket to write results to. Point this at a scratch bucket when testing, "
        "the default is the one Crash Stats reads and the job clears it before uploading.",
    )
    return parser.parse_args()


def remove_results_gcs(gcs_client, bucket_name, job_name):
    bucket = gcs_client.bucket(bucket_name)
    for key in bucket.list_blobs(prefix=job_name + "/data/"):
        key.delete()


def upload_results_gcs(gcs_client, bucket_name, job_name, directory):
    bucket = gcs_client.bucket(bucket_name)
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

        # A channel with no crashes for the versions we asked for used to take the whole run
        # down, losing the channels that had already succeeded. Happens when product-details
        # advertises a new nightly before builds with that version have crashed. See
        # migration_plan.md, "Empty channel crash".
        channel_crashes = dataset.count()
        if channel_crashes == 0:
            print(
                f"No crashes for {channel} versions"
                f" {channel_to_versions[channel]}, skipping the channel"
            )
            totals[channel] = 0
            continue

        # TEMPORARY, remove with count_trace.py once the NULL undercount is understood.
        # Only release is traced: it's where the discrepancy was measured, and the witness
        # signature lives there. Writes to a separate prefix so it can't affect the output.
        if args.trace_counts and channel == "release":
            crash_deviations.TRACE = count_trace.Tracer(channel)
        else:
            crash_deviations.TRACE = None

        results, total_reference, total_groups = crash_deviations.find_deviations(
            sc, dataset, signatures=signatures[channel]
        )

        if crash_deviations.TRACE is not None:
            crash_deviations.TRACE.note_results(results, total_reference, total_groups)
            crash_deviations.TRACE.flush(args.trace_bucket or args.results_bucket)
            crash_deviations.TRACE = None

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

    # With the default bucket this ends up under
    # https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/
    print(f"Writing results to gs://{args.results_bucket}")
    remove_results_gcs(gcs_client, args.results_bucket, "top-signatures-correlations")
    upload_results_gcs(
        gcs_client,
        args.results_bucket,
        "top-signatures-correlations",
        "top-signatures-correlations_output",
    )


if __name__ == "__main__":
    main()
