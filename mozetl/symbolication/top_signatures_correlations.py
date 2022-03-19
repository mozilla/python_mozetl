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

sys.path += [os.path.abspath("."), os.path.abspath("crashcorrelations")]
os.system("git clone https://github.com/marco-c/crashcorrelations.git")

os.system("pip download stemming==1.0.1")
os.system("tar xf stemming-1.0.1.tar.gz")

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("modules-with-missing-symbols").getOrCreate()

sc.addPyFile("stemming-1.0.1/stemming/porter2.py")

from crashcorrelations import (  # noqa E402
    utils,
    download_data,
    crash_deviations,
    comments,
)


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
    return parser.parse_args()


args = parse_args()

if args.date.isoweekday() % 7 not in args.run_on_days:
    print(
        f"Skipping because run date day of week"
        f" {args.date} is not in {args.run_on_days}"
    )
    sys.exit(0)

print(datetime.utcnow())

channels = ["release", "beta", "nightly", "esr"]
channel_to_versions = {}

for channel in channels:
    channel_to_versions[channel] = download_data.get_versions(channel)

signatures = {}

for channel in channels:
    signatures[channel] = download_data.get_top(
        1000, versions=channel_to_versions[channel], days=5
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
    results, total_reference, total_groups = crash_deviations.find_deviations(
        sc, dataset, signatures=signatures[channel]
    )

    totals[channel] = total_reference

    try:
        dataset = crash_deviations.get_telemetry_crashes(
            spark, versions=channel_to_versions[channel], days=30
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

# Will be uploaded under
# https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/
utils.remove_results("top-signatures-correlations")
utils.upload_results(
    "top-signatures-correlations", "top-signatures-correlations_output"
)
