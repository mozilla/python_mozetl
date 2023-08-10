# Migrated from Databricks to run on dataproc
# pip install:
# python_moztelemetry
# git+https://github.com/FirefoxGraphics/telemetry.git#egg=pkg&subdirectory=analyses/bigquery_shim
# boto3==1.16.20
# six==1.15.0

import argparse
import datetime
import json
import os
import sys
import time

import requests
from bigquery_shim import trends
from moztelemetry import get_one_ping_per_client
from pyspark import SparkContext
from pyspark.sql import SparkSession

from google.cloud import storage


def fmt_date(d):
    return d.strftime("%Y%m%d")


def jstime(d):
    return time.mktime(d.timetuple())


def repartition(pipeline):
    return pipeline.repartition(MaxPartitions).cache()


storage_client = storage.Client()

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("graphics-trends").getOrCreate()

MaxPartitions = sc.defaultParallelism * 4

# Keep this small (0.00001) for fast backfill testing.
WeeklyFraction = 0.003

# Amount of days Telemetry keeps.
MaxHistoryInDays = datetime.timedelta(days=210)

# Bucket we'll drop files into on GCS. If this is None, we won't attempt any
# GCS uploads, and the analysis will start from scratch.
GCS_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"
GCS_PREFIX = "gfx/telemetry-data/"
GITHUB_REPO = "https://raw.githubusercontent.com/FirefoxGraphics/moz-gfx-telemetry"

# List of jobs allowed to have a first-run (meaning no GCS content).
BrandNewJobs = []

# If true, backfill up to MaxHistoryInDays rather than the last update.
ForceMaxBackfill = False

OUTPUT_PATH = "output"
os.mkdir(OUTPUT_PATH)

ArchKey = "environment/build/architecture"
FxVersionKey = "environment/build/version"
Wow64Key = "environment/system/isWow64"
CpuKey = "environment/system/cpu"
GfxAdaptersKey = "environment/system/gfx/adapters"
GfxFeaturesKey = "environment/system/gfx/features"
OSNameKey = "environment/system/os/name"
OSVersionKey = "environment/system/os/version"
OSServicePackMajorKey = "environment/system/os/servicePackMajor"

FirstValidDate = datetime.datetime.utcnow() - MaxHistoryInDays


# Log spam eats up disk space, so we disable it.
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


quiet_logs(sc)


# This is the entry-point to grabbing reduced, preformatted pings.
def fetch_and_format(start_date, end_date):
    pings = get_raw_pings(start_date, end_date)
    pings = get_one_ping_per_client(pings)
    pings = pings.map(validate)
    pings = pings.filter(lambda p: p.get("valid", False))
    return pings.cache()


def get_raw_pings(start_date, end_date):
    # WeeklyFraction ignored and baked into the included query
    return trends.fetch_results(spark, start_date, end_date, project_id="mozdata")


# Transform each ping to make it easier to work with in later stages.
def validate(p):
    try:
        name = p.get(OSNameKey) or "w"
        version = p.get(OSVersionKey) or "0"
        if name == "Linux":
            p["OSVersion"] = None
            p["OS"] = "Linux"
            p["OSName"] = "Linux"
        elif name == "Windows_NT":
            spmaj = p.get(OSServicePackMajorKey) or "0"
            p["OSVersion"] = version + "." + str(spmaj)
            p["OS"] = "Windows-" + version + "." + str(spmaj)
            p["OSName"] = "Windows"
        elif name == "Darwin":
            p["OSVersion"] = version
            p["OS"] = "Darwin-" + version
            p["OSName"] = "Darwin"
        else:
            p["OSVersion"] = version
            p["OS"] = "{0}-{1}".format(name, version)
            p["OSName"] = name
    except Exception:
        return p

    p["valid"] = True
    return p


# Profiler for debugging. Use in a |with| clause.
class Prof(object):
    level = 0

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.sout("Starting {0}... ".format(self.name))
        self.start = datetime.datetime.now()
        Prof.level += 1
        return None

    def __exit__(self, type, value, traceback):
        Prof.level -= 1
        self.end = datetime.datetime.now()
        self.sout(
            "... {0}: {1}s".format(self.name, (self.end - self.start).total_seconds())
        )

    def sout(self, s):
        sys.stdout.write(("##" * Prof.level) + " ")
        sys.stdout.write(s)
        sys.stdout.write("\n")
        sys.stdout.flush()


# Helpers.
def fix_vendor(vendor_id):
    if vendor_id == "Intel Open Source Technology Center":
        return "0x8086"
    return vendor_id


def get_vendor(ping):
    try:
        adapter = ping[GfxAdaptersKey][0]
        return fix_vendor(adapter["vendorID"])
    except Exception:
        return "unknown"


def get_os_bits(ping):
    arch = ping.get(ArchKey, "unknown")
    if arch == "x86-64":
        return "64"
    elif arch == "x86":
        if ping.get(Wow64Key, False):
            return "32_on_64"
        return "32"
    return "unknown"


def get_gen(ping, vendor_block):
    adapter = ping[GfxAdaptersKey][0]
    device_id = adapter.get("deviceID", "unknown")
    if device_id not in vendor_block:
        return "unknown"
    return vendor_block[device_id][0]


def get_d3d11(ping):
    try:
        d3d11 = ping[GfxFeaturesKey]["d3d11"]
        if d3d11["status"] != "available":
            return d3d11["status"]
        if d3d11.get("warp", False):
            return "warp"
        return d3d11["version"]
    except Exception:
        return "unknown"


def get_d2d(ping):
    try:
        status = ping[GfxFeaturesKey]["d2d"]["status"]
        if status != "available":
            return status
        return ping[GfxFeaturesKey]["d2d"]["version"]
    except Exception:
        return "unknown"


def get_version(ping):
    v = ping.get(FxVersionKey, None)
    if v is None or not isinstance(v, str):
        return "unknown"
    return v.split(".")[0]


def get_compositor(ping):
    features = ping.get(GfxFeaturesKey, None)
    if features is None:
        return "none"
    return features.get("compositor", "none")


# A TrendBase encapsulates the data needed to visualize a trend.
# It has four functions:
#    prepare    (download from cache)
#    willUpdate (check if update is needed)
#    update     (add analysis data for a week of pings)
#    finish     (upload back to cache)
class TrendBase(object):
    def __init__(self, name):
        super(TrendBase, self).__init__()
        self.name = "{0}-v2.json".format(name)

    # Called before analysis starts.
    def prepare(self):
        print("Preparing {0}".format(self.name))
        return True

    # Called before querying pings for the week for the given date. Return
    # false to indicate that this should no longer receive updates.
    def will_update(self, date):
        raise Exception("Return true or false")

    def update(self, pings, **kwargs):
        raise Exception("NYI")

    def finish(self):
        pass


# Given a list of trend objects, query weeks from the last sunday
# and iterating backwards until no trend object requires an update.
def do_update(trends):
    root = TrendGroup("root", trends)
    root.prepare()

    # Start each analysis slice on a Sunday.
    latest = most_recent_sunday()
    end = latest

    while True:
        start = end - datetime.timedelta(7)
        assert latest.weekday() == 6

        if not root.will_update(start):
            break

        try:
            with Prof("fetch {0}".format(start)) as _:
                pings = fetch_and_format(start, end)
        except Exception:
            if not ForceMaxBackfill:
                raise

        with Prof("compute {0}".format(start)) as _:
            if not root.update(pings, start_date=start, end_date=end):
                break

        end = start

    root.finish()


def most_recent_sunday():
    now = datetime.datetime.utcnow()
    this_morning = datetime.datetime(now.year, now.month, now.day)
    if this_morning.weekday() == 6:
        return this_morning
    diff = datetime.timedelta(0 - this_morning.weekday() - 1)
    return this_morning + diff


# A TrendGroup is a collection of TrendBase objects. It lets us
# group similar trends together. For example, if five trends all
# need to filter Windows pings, we can filter for Windows pings
# once and cache the result, rather than redo the filter each
# time.
#
# Trend groups keep an "active" list of trends that will probably
# need another update. If any trend stops requesting data, it is
# removed from the active list.
class TrendGroup(TrendBase):
    def __init__(self, name, trends):
        super(TrendGroup, self).__init__(name)
        self.trends = trends
        self.active = []

    def prepare(self):
        self.trends = [trend for trend in self.trends if trend.prepare()]
        self.active = self.trends[:]
        return len(self.trends) > 0

    def will_update(self, date):
        self.active = [trend for trend in self.active if trend.will_update(date)]
        return len(self.active) > 0

    def update(self, pings, **kwargs):
        pings = pings.cache()
        self.active = [trend for trend in self.active if trend.update(pings, **kwargs)]
        return len(self.active) > 0

    def finish(self):
        for trend in self.trends:
            trend.finish()


# A Trend object takes a new set of pings for a week's worth of data,
# analyzes it, and adds the result to the trend set. Trend sets are
# cached in GCS as JSON.
#
# If the latest entry in the cache covers less than a full week of
# data, the entry is removed so that week can be re-queried.
class Trend(TrendBase):
    def __init__(self, filename):
        super(Trend, self).__init__(filename)
        self.local_path = os.path.join(OUTPUT_PATH, self.name)
        self.cache = None
        self.lastFullWeek = None
        self.newDataPoints = []

    def query(self, pings):
        raise Exception("NYI")

    def will_update(self, date):
        if date < FirstValidDate:
            return False
        if self.lastFullWeek is not None and date <= self.lastFullWeek:
            return False
        return True

    def prepare(self):
        self.cache = self.fetch_json()
        if self.cache is None:
            self.cache = {"created": jstime(datetime.datetime.utcnow()), "trend": []}

        # Make sure trends are sorted in ascending order.
        self.cache["trend"] = self.cache["trend"] or []
        self.cache["trend"] = sorted(self.cache["trend"], key=lambda o: o["start"])

        if len(self.cache["trend"]) and not ForceMaxBackfill:
            last_data_point = self.cache["trend"][-1]
            last_data_point_start = datetime.datetime.utcfromtimestamp(
                last_data_point["start"]
            )
            last_data_point_end = datetime.datetime.utcfromtimestamp(
                last_data_point["end"]
            )
            print(last_data_point, last_data_point_start, last_data_point_end)
            if last_data_point_end - last_data_point_start < datetime.timedelta(7):
                # The last data point had less than a full week, so we stop at the
                # previous week, and remove the incomplete datapoint.
                self.lastFullWeek = last_data_point_start - datetime.timedelta(7)
                self.cache["trend"].pop()
            else:
                # The last data point covered a full week, so that's our stopping
                # point.
                self.lastFullWeek = last_data_point_start
                print(self.lastFullWeek)

        return True

    # Optional hook - transform pings before querying.
    def transform_pings(self, pings):
        return pings

    def update(self, pings, start_date, end_date, **kwargs):
        with Prof("count {0}".format(self.name)):
            pings = self.transform_pings(pings)
            count = pings.count()
        if count == 0:
            print("WARNING: no pings in RDD")
            return False

        with Prof("query {0} (count: {1})".format(self.name, count)):
            data = self.query(pings)

        self.newDataPoints.append(
            {
                "start": jstime(start_date),
                "end": jstime(end_date),
                "total": count,
                "data": data,
            }
        )
        return True

    def finish(self):
        # If we're doing a maximum backfill, remove points from the cache that are
        # after the least recent data point that we newly queried.
        if ForceMaxBackfill and len(self.newDataPoints):
            stop_at = self.newDataPoints[-1]["start"]
            last_index = None
            for index, entry in enumerate(self.cache["trend"]):
                if entry["start"] >= stop_at:
                    last_index = index
                    break
            if last_index is not None:
                self.cache["trend"] = self.cache["trend"][:last_index]

        # Note: the backfill algorithm in DoUpdate() walks in reverse, so dates
        # will be accumulated in descending order. The final list should be in
        # ascending order, so we reverse.
        self.cache["trend"] += self.newDataPoints[::-1]

        text = json.dumps(self.cache)

        print("Writing file {0}".format(self.local_path))
        with open(self.local_path, "w") as fp:
            fp.write(text)

        if GCS_BUCKET is not None:
            try:
                bucket = storage_client.get_bucket(GCS_BUCKET)
                blob = bucket.blob(os.path.join(GCS_PREFIX, self.name))
                blob.upload_from_filename(self.local_path)
            except Exception as e:
                print("Failed gcs upload: {0}".format(e))

    def fetch_json(self):
        print("Reading file {0}".format(self.local_path))
        if GCS_BUCKET is not None:
            try:
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(GCS_BUCKET)
                blob = bucket.blob(os.path.join(GCS_PREFIX, self.name))
                blob.download_to_filename(self.local_path)
                with open(self.local_path, "r") as fp:
                    return json.load(fp)
            except Exception:
                if self.name not in BrandNewJobs:
                    raise
                return None
        else:
            try:
                with open(self.local_path, "r") as fp:
                    return json.load(fp)
            except Exception:
                pass
        return None


class FirefoxTrend(Trend):
    def __init__(self):
        super(FirefoxTrend, self).__init__("trend-firefox")

    def query(self, pings, **kwargs):
        return pings.map(lambda p: (get_version(p),)).countByKey()


class WindowsGroup(TrendGroup):
    def __init__(self, trends):
        super(WindowsGroup, self).__init__("Windows", trends)

    def update(self, pings, **kwargs):
        pings = pings.filter(lambda p: p["OSName"] == "Windows")
        return super(WindowsGroup, self).update(pings, **kwargs)


class WinverTrend(Trend):
    def __init__(self):
        super(WinverTrend, self).__init__("trend-windows-versions")

    def query(self, pings):
        return pings.map(lambda p: (p["OSVersion"],)).countByKey()


class WinCompositorTrend(Trend):
    def __init__(self):
        super(WinCompositorTrend, self).__init__("trend-windows-compositors")

    def will_update(self, date):
        # This metric didn't ship until Firefox 43.
        if date < datetime.datetime(2015, 11, 15):
            return False
        return super(WinCompositorTrend, self).will_update(date)

    def query(self, pings):
        return pings.map(lambda p: (get_compositor(p),)).countByKey()


class WinArchTrend(Trend):
    def __init__(self):
        super(WinArchTrend, self).__init__("trend-windows-arch")

    def query(self, pings):
        return pings.map(lambda p: (get_os_bits(p),)).countByKey()


# This group restricts pings to Windows Vista+, and must be inside a
# group that restricts pings to Windows.
class WindowsVistaPlusGroup(TrendGroup):
    def __init__(self, trends):
        super(WindowsVistaPlusGroup, self).__init__("Windows Vista+", trends)

    def update(self, pings, **kwargs):
        pings = pings.filter(lambda p: not p["OSVersion"].startswith("5.1"))
        return super(WindowsVistaPlusGroup, self).update(pings, **kwargs)


class Direct2DTrend(Trend):
    def __init__(self):
        super(Direct2DTrend, self).__init__("trend-windows-d2d")

    def query(self, pings):
        return pings.map(lambda p: (get_d2d(p),)).countByKey()

    def will_update(self, date):
        # This metric didn't ship until Firefox 43.
        if date < datetime.datetime(2015, 11, 15):
            return False
        return super(Direct2DTrend, self).will_update(date)


class Direct3D11Trend(Trend):
    def __init__(self):
        super(Direct3D11Trend, self).__init__("trend-windows-d3d11")

    def query(self, pings):
        return pings.map(lambda p: (get_d3d11(p),)).countByKey()

    def will_update(self, date):
        # This metric didn't ship until Firefox 43.
        if date < datetime.datetime(2015, 11, 15):
            return False
        return super(Direct3D11Trend, self).will_update(date)


class WindowsVendorTrend(Trend):
    def __init__(self):
        super(WindowsVendorTrend, self).__init__("trend-windows-vendors")

    def query(self, pings):
        return pings.map(lambda p: ((get_vendor(p) or "unknown"),)).countByKey()


# Device generation trend - a little more complicated, since we download
# the generation database to produce a mapping.
class DeviceGenTrend(Trend):
    device_map = None

    def __init__(self, vendor, vendor_name):
        super(DeviceGenTrend, self).__init__(
            "trend-windows-device-gen-{0}".format(vendor_name)
        )
        self.vendorBlock = None
        self.vendorID = vendor

    def prepare(self):
        # Grab the vendor -> device -> gen map.
        if not DeviceGenTrend.device_map:
            resp = requests.get("{0}/master/www/gfxdevices.json".format(GITHUB_REPO))
            DeviceGenTrend.device_map = resp.json()
        self.vendorBlock = DeviceGenTrend.device_map[self.vendorID]
        return super(DeviceGenTrend, self).prepare()

    def transform_pings(self, pings):
        vendor_id = self.vendorID
        return pings.filter(lambda p: get_vendor(p) == vendor_id)

    def query(self, pings):
        # Can't use member variables and methods with maps because parent
        # class references boto3 client which isn't picklable
        vendor_block = self.vendorBlock

        return pings.map(lambda p: (get_gen(p, vendor_block),)).countByKey()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-max-backfill", action="store_true")
    parser.add_argument("--weekly-fraction", type=float, default=0.003)
    parser.add_argument(
        "--gcs-bucket", default="moz-fx-data-static-websit-8565-analysis-output"
    )
    parser.add_argument("--gcs-prefix", default="gfx/telemetry-data/")
    parser.add_argument("--max-history-in-days", type=int, default=210)
    parser.add_argument("--brand-new-jobs", action="append", default=[])
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ForceMaxBackfill = args.force_max_backfill
    WeeklyFraction = args.weekly_fraction
    GCS_BUCKET = args.gcs_bucket
    GCS_PREFIX = args.gcs_prefix
    MaxHistoryInDays = datetime.timedelta(days=args.max_history_in_days)
    BrandNewJobs = args.brand_new_jobs

    do_update(
        [
            FirefoxTrend(),
            WindowsGroup(
                [
                    WinverTrend(),
                    WinCompositorTrend(),
                    WinArchTrend(),
                    WindowsVendorTrend(),
                    WindowsVistaPlusGroup([Direct2DTrend(), Direct3D11Trend()]),
                    DeviceGenTrend("0x8086", "intel"),
                    DeviceGenTrend("0x10de", "nvidia"),
                    DeviceGenTrend("0x1002", "amd"),
                ]
            ),
        ]
    )
