# Migrated from Databricks to run on dataproc
# pip install:
# boto3==1.16.20

import argparse
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urljoin

import boto3
import requests
from pyspark.sql import functions, SparkSession


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

os.system("git clone https://github.com/marco-c/missing_symbols.git")

spark = SparkSession.builder.appName("modules-with-missing-symbols").getOrCreate()

known_modules = set(
    [module[:-4].lower() for module in os.listdir("missing_symbols/known_modules")]
)

dataset = (
    spark.read.format("bigquery")
    .option("table", "moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2")
    .load()
    .where(
        "crash_date >= to_date('{}')".format(
            (datetime.utcnow() - timedelta(3)).strftime("%Y-%m-%d")
        )
    )
)

modules = (
    dataset.filter(dataset["product"] == "Firefox")
    .select(
        ["uuid"]
        + [functions.explode((dataset["json_dump"]["modules"]["list"])).alias("module")]
    )
    .dropDuplicates(["uuid", "module"])
    .select(["module"])
    .rdd.map(lambda v: v["module"]["element"])
    .filter(
        lambda m: m["missing_symbols"]
        and m["filename"].lower() not in known_modules
        and "(deleted)" not in m["filename"]
    )
    .flatMap(
        lambda m: [((m["filename"], (m["version"], m["debug_id"], m["debug_file"])), 1)]
    )
    .reduceByKey(lambda x, y: x + y)
    .map(lambda v: (v[0][0], [(v[0][1], v[1])]))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda v: sum(count for ver, count in v[1]), ascending=False)
    .collect()
)

print(f"len(modules): {len(modules)}")

[(module, sum(count for ver, count in versions)) for module, versions in modules]

top_missing = sorted(
    [
        (name, version, count)
        for name, versions in modules
        for version, count in versions
        if count > 70
    ],
    key=lambda m: m[2],
    reverse=True,
)

print(f"len(top_missing): {len(top_missing)}")

with open("missing_symbols/firefox_modules.txt", "r") as f:
    firefox_modules = [m.lower() for m in f.read().split("\n") if m.strip() != ""]

with open("missing_symbols/windows_modules.txt", "r") as f:
    windows_modules = [m.lower() for m in f.read().split("\n") if m.strip() != ""]

r = requests.get(
    "https://product-details.mozilla.org/1.0/firefox_history_major_releases.json"
)
firefox_versions = r.json()
old_firefox_versions = []
for version, date in firefox_versions.items():
    delta = datetime.utcnow() - datetime.strptime(date, "%Y-%m-%d")
    if abs(delta.days) > 730:
        old_firefox_versions.append(version[: version.index(".")])


def is_old_firefox_module(module_info):
    """Returns whether this is considered an old firefox module

    Our symbols server expires debug information after 2 years. We don't want
    to be notified of old firefox modules because it's likely they've expired
    out of our system.

    :param module_info: some module information structure consisting of
        (name, (major, minor, rev), count)

    :returns: true if this is an old firefox module, false if either this isn't
        a firefox module or we don't have version information

    """

    name, (version, _, _), count = module_info

    # If this isn't a firefox module or there's no version information (null or
    # empty string), then it's not considered an old firefox module
    if name.lower() not in firefox_modules or not version:
        return False

    return any(version.startswith(v + ".") for v in old_firefox_versions)


top_missing = [m for m in top_missing if not is_old_firefox_module(m)]


def are_symbols_available(debug_file, debug_id):
    if not debug_file or not debug_id:
        return False

    url = urljoin(
        "https://symbols.mozilla.org/",
        "{}/{}/{}".format(
            debug_file,
            debug_id,
            debug_file if not debug_file.endswith(".pdb") else debug_file[:-3] + "sym",
        ),
    )
    r = requests.head(url)
    return r.ok


top_missing_with_avail_info = [
    (name, version, debug_id, count, are_symbols_available(debug_id, debug_file))
    for name, (version, debug_id, debug_file), count in top_missing
]

subject = "Weekly report of modules with missing symbols in crash reports"

body = """
<table style="border-collapse:collapse;">
  <tr>
  <th style="border: 1px solid black;">Name</th>
  <th style="border: 1px solid black;">Version</th>
  <th style="border: 1px solid black;">Debug ID</th>
  <th style="border: 1px solid black;"># of crash reports</th>
</tr>
"""
any_available = False
for name, version, debug_id, count, are_available_now in top_missing_with_avail_info:
    body += "<tr>"
    body += '<td style="border: 1px solid black;">'
    if name.lower() in firefox_modules:
        if debug_id:
            body += '<span style="color:red;">%s</span>' % name
        else:
            body += '<span style="color:orange;">%s</span>' % name
    elif name.lower() in windows_modules:
        body += '<span style="color:blue;">%s</span>' % name
    else:
        body += name
    if are_available_now:
        body += " (*)"
        any_available = True
    body += "</td>"
    body += '<td style="border: 1px solid black;">%s</td>' % version
    body += '<td style="border: 1px solid black;">%s</td>' % debug_id
    body += '<td style="border: 1px solid black;">%d</td>' % count
    body += "</tr>"
body += "</table>"

body += "<pre>"

if any_available:
    body += """
(*) We now have symbols for the modules marked with an asterisk. We could
reprocess them to improve stack traces (and maybe signatures) of some crash reports.\n
"""

body += """
The number of crash reports refers to the past 3 days.
Only modules with at least 2,000 crash reports are shown in this list.

Firefox own modules, for which we should have symbols, and have the debug ID are colored in red.
For Firefox own modules, where we don't have a debug ID are colored in orange.
OS modules, for which we should have symbols, are colored in blue.

If you see modules that shouldn't be in this list as it's expected not
to have their symbols, either contact mcastelluccio@mozilla.com or open
a PR to add them to https://github.com/marco-c/missing_symbols/tree/master/known_modules.
"""

body += "</pre>"

client = boto3.client("ses", region_name="us-west-2")

client.send_email(
    Source="mcastelluccio@data.mozaws.net",
    Destination={
        "ToAddresses": [
            "mcastelluccio@mozilla.com",
            "release-mgmt@mozilla.com",
            "stability@mozilla.org",
        ],
        "CcAddresses": [],
    },
    Message={
        "Subject": {"Data": subject, "Charset": "UTF-8"},
        "Body": {"Html": {"Data": body, "Charset": "UTF-8"}},
    },
)
