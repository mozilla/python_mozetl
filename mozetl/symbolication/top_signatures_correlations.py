# Migrated from Databricks to run on dataproc
# pip install:
# boto3==1.16.20
# scipy==1.5.4

import argparse
import hashlib
import os
import sys
from collections import defaultdict
from datetime import datetime

from pyspark import SparkContext
from pyspark.sql import SparkSession

from google.cloud import storage

sys.path += [os.path.abspath("."), os.path.abspath("crashcorrelations")]
os.system("git clone https://github.com/marco-c/crashcorrelations.git")

os.system("pip download stemming==1.0.1")
os.system("tar xf stemming-1.0.1.tar.gz")

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("modules-with-missing-symbols").getOrCreate()
gcs_client = storage.Client()

sc.addPyFile("stemming-1.0.1/stemming/porter2.py")


# Number of top signatures to look at
TOP_SIGNATURE_COUNT = 200

# Number of days to look at to figure out top signatures
TOP_SIGNATURE_PERIOD_DAYS = 5

# Number of days to look at for telemetry crash data
TELEMETRY_CRASHES_PERIOD_DAYS = 30

# Name of the GCS bucket where results are stored
RESULTS_GCS_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"


from crashcorrelations import (  # noqa E402
    utils,
    download_data,
    crash_deviations,
    comments,
)


# TEMPORARY, for diagnosing the NULL undercount. Remove with count_trace_prod.py.
def load_count_trace(ref):
    """Fetch and import count_trace_prod from the given git ref.

    Dataproc stages only the driver file, so the tracer isn't on disk next to us and has to
    be fetched. Returns None on any failure: the job must still run if the diagnostic
    can't load.
    """
    url = (
        "https://raw.githubusercontent.com/mozilla/python_mozetl/"
        "{}/mozetl/symbolication/count_trace_prod.py".format(ref)
    )
    try:
        if os.system("curl -sSf -o count_trace_prod.py '{}'".format(url)) != 0:
            print("count_trace_prod: curl failed for {}, continuing without it".format(url))
            return None
        import count_trace_prod

        return count_trace_prod
    except Exception as error:
        print("count_trace_prod unavailable, continuing without it: {!r}".format(error))
        return None


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
        type=datetime.fromisoformat,
        default=datetime.utcnow(),
        help="Run date, defaults to current dat",
    )
    parser.add_argument(
        "--trace-counts",
        action="store_true",
        help="TEMPORARY. Count nulls four ways on the release dataframe to diagnose the "
        "published NULL undercount, and write the result to a count-trace-prod/ prefix. "
        "See count_trace_prod.py.",
    )
    parser.add_argument(
        "--trace-ref",
        default="main",
        help="Git ref to fetch count_trace_prod.py from. Should match the ref this driver "
        "was fetched from.",
    )
    parser.add_argument(
        "--trace-bucket",
        default=RESULTS_GCS_BUCKET,
        help="GCS bucket for --trace-counts output. Use a scratch bucket; the default is "
        "the one Crash Stats reads.",
    )
    parser.add_argument(
        "--override-versions",
        nargs="+",
        default=None,
        metavar="VERSION",
        help="TEMPORARY. Use these versions for the release channel instead of asking "
        "product-details. get_versions() fetches the current version live and ignores "
        "--date, so once a new major ships, the release channel resolves to a version "
        "with almost no crashes and a rerun can't reproduce an earlier run. Pin the "
        "versions to reproduce one, e.g. --override-versions 153.0 153.0.1 153.0.3 "
        "153.0.4. Requires --results-bucket to be a scratch bucket, since the output "
        "won't match what the schedule would have produced.",
    )
    parser.add_argument(
        "--results-bucket",
        default=RESULTS_GCS_BUCKET,
        help="GCS bucket to write results to. Point this at a scratch bucket when "
        "testing; the default is the one Crash Stats reads and the job clears it before "
        "uploading.",
    )
    return parser.parse_args()


def remove_results_gcs(job_name, bucket_name=RESULTS_GCS_BUCKET):
    bucket = gcs_client.bucket(bucket_name)
    for key in bucket.list_blobs(prefix=job_name + "/data/"):
        key.delete()


def upload_results_gcs(job_name, directory, bucket_name=RESULTS_GCS_BUCKET):
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


args = parse_args()

if args.date.isoweekday() % 7 not in args.run_on_days:
    print(
        f"Skipping because run date day of week"
        f" {args.date} is not in {args.run_on_days}"
    )
    sys.exit(0)

print(datetime.utcnow())

# TEMPORARY, see count_trace_prod.py.
count_trace_prod = load_count_trace(args.trace_ref) if args.trace_counts else None

# --override-versions exists to reproduce an earlier run, so its output deliberately doesn't
# match the schedule. Refuse to publish that to the bucket Crash Stats reads.
if args.override_versions and args.results_bucket == RESULTS_GCS_BUCKET:
    sys.exit(
        "--override-versions writes results that don't match the current versions. "
        "Pass --results-bucket with a scratch bucket to use it."
    )

channels = ["release", "beta", "nightly", "esr"]
channel_to_versions = {}

for channel in channels:
    channel_to_versions[channel] = download_data.get_versions(channel)

# TEMPORARY, with --override-versions. Applied here so the pinned versions reach everything
# downstream from one place: get_top, get_telemetry_crashes, and the count trace. Release
# only, since that's the channel being reproduced.
if args.override_versions:
    print(
        "Overriding release versions {} with {}".format(
            channel_to_versions["release"], args.override_versions
        )
    )
    channel_to_versions["release"] = list(args.override_versions)

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
        spark, versions=channel_to_versions[channel], days=5
    )

    # TEMPORARY. Count nulls four ways on this exact dataframe before find_deviations sees
    # it, to find where the published NULL counts lose rows. Release only: that's where the
    # discrepancy was measured. Writes to its own prefix and can't affect the output.
    # Remove with count_trace_prod.py once the cause is known.
    if count_trace_prod is not None and channel == "release":
        try:
            tracer = count_trace_prod.Tracer(
                channel,
                versions=channel_to_versions[channel],
                days=5,
                versions_overridden=bool(args.override_versions),
            )
            tracer.measure(spark, dataset)
            tracer.flush(args.trace_bucket)
        except Exception as error:
            print("count_trace_prod failed, continuing: {!r}".format(error))

    results, total_reference, total_groups = crash_deviations.find_deviations(
        sc, dataset, signatures=signatures[channel]
    )

    totals[channel] = total_reference

    try:
        dataset = crash_deviations.get_telemetry_crashes(
            spark,
            versions=channel_to_versions[channel],
            days=TELEMETRY_CRASHES_PERIOD_DAYS,
        )
        top_words = comments.get_top_words(dataset, signatures[channel])
    except Exception:
        top_words = {}

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

        if signature in top_words:
            res["top_words"] = top_words[signature]

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

print(datetime.utcnow())

# With the default bucket this is served under
# https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/
print("Writing results to gs://{}".format(args.results_bucket))
remove_results_gcs("top-signatures-correlations", args.results_bucket)
upload_results_gcs(
    "top-signatures-correlations",
    "top-signatures-correlations_output",
    args.results_bucket,
)
