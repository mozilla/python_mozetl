# Migrated from Databricks to run on dataproc
# pip install:
# python_moztelemetry
# git+https://github.com/FirefoxGraphics/telemetry.git#egg=pkg&subdirectory=analyses/bigquery_shim
# six==1.15.0

import argparse
import datetime
import json
import os
import sys
import time

from bigquery_shim import dashboard, snake_case
from moztelemetry import get_pings_properties
from pyspark import SparkContext
from pyspark.sql import SparkSession

from google.cloud import storage


def fmt_date(d):
    return d.strftime("%Y%m%d")


def repartition(pipeline):
    return pipeline.repartition(MaxPartitions).cache()


storage_client = storage.Client()

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("graphics-trends").getOrCreate()

MaxPartitions = sc.defaultParallelism * 4
start_time = datetime.datetime.now()

# Going forward we only care about sessions from Firefox 53+, since it
# is the first release to not support Windows XP and Vista, which distorts
# our statistics.
MinFirefoxVersion = "53"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--default-time-window", type=int, default=14)
    parser.add_argument("--release-fraction", type=float, default=0.003)
    parser.add_argument(
        "--output-bucket", default="moz-fx-data-static-websit-8565-analysis-output"
    )
    parser.add_argument("--output-prefix", default="gfx/telemetry-data/")
    return parser.parse_args()


args = parse_args()
# The directory where to place the telemetry results
OUTPUT_BUCKET = args.output_bucket
OUTPUT_PREFIX = args.output_prefix
# Configuration for general data that spans all Firefox versions. As of
# 7-19-2017 a sample fraction of .003 for 2 weeks of submissions yielded
# 12mil pings, so we estimate there are about 4bn total. We try to target
# about 2-5mil in the general fraction below.
DefaultTimeWindow = args.default_time_window
ReleaseFraction = args.release_fraction

bucket = storage_client.get_bucket(OUTPUT_BUCKET)
blobs = bucket.list_blobs(prefix=OUTPUT_PREFIX)
existing_objects = [blob.name for blob in blobs]

print(f"Existing objects: {existing_objects}")

# List of keys for properties on session pings that we care about.
GfxKey = "environment/system/gfx"
MonitorsKey = "environment/system/gfx/monitors"
ArchKey = "environment/build/architecture"
FeaturesKey = "environment/system/gfx/features"
UserPrefsKey = "environment/settings/userPrefs"
DeviceResetReasonKey = "payload/histograms/DEVICE_RESET_REASON"
SANITY_TEST = "payload/histograms/GRAPHICS_SANITY_TEST"
STARTUP_TEST_KEY = "payload/histograms/GRAPHICS_DRIVER_STARTUP_TEST"
WebGLSuccessKey = "payload/histograms/CANVAS_WEBGL_SUCCESS"
WebGL2SuccessKey = "payload/histograms/CANVAS_WEBGL2_SUCCESS"
PluginModelKey = "payload/histograms/PLUGIN_DRAWING_MODEL"
MediaDecoderKey = "payload/histograms/MEDIA_DECODER_BACKEND_USED"
LayersD3D11FailureKey = "payload/keyedHistograms/D3D11_COMPOSITING_FAILURE_ID"
LayersOGLFailureKey = "payload/keyedHistograms/OPENGL_COMPOSITING_FAILURE_ID"
WebGLAcclFailureKey = "payload/keyedHistograms/CANVAS_WEBGL_ACCL_FAILURE_ID"
WebGLFailureKey = "payload/keyedHistograms/CANVAS_WEBGL_FAILURE_ID"

# This is the filter list, so we only select the above properties.
PropertyList = [
    GfxKey,
    FeaturesKey,
    UserPrefsKey,
    MonitorsKey,
    ArchKey,
    DeviceResetReasonKey,
    SANITY_TEST,
    STARTUP_TEST_KEY,
    WebGLSuccessKey,
    WebGL2SuccessKey,
    PluginModelKey,
    MediaDecoderKey,
    LayersD3D11FailureKey,
    LayersOGLFailureKey,
    WebGLAcclFailureKey,
    WebGLFailureKey,
]


###########################################################
# Helper function block for fetching and filtering pings. #
###########################################################
def union_pipelines(a, b):
    if a is None:
        return b
    return a + b


def fetch_raw_pings(**kwargs):
    time_window = kwargs.pop("timeWindow", DefaultTimeWindow)
    fraction = kwargs.pop("fraction", ReleaseFraction)
    channel = kwargs.pop("channel", None)

    # Since builds take a bit to disseminate, go back about 4 hours. This is a
    # completely made up number.
    limit = datetime.timedelta(0, 60 * 60 * 4)
    now = datetime.datetime.now()
    start = now - datetime.timedelta(time_window) - limit
    end = now - limit

    # NOTE: ReleaseFraction is not used in the shim
    pings = dashboard.fetch_results(
        spark,
        start,
        end,
        project_id="mozdata",
        channel=channel,
        min_firefox_version=MinFirefoxVersion,
    )

    metadata = [
        {
            "info": {
                "channel": "*",
                "fraction": fraction,
                "day_range": (end - start).days,
            }
        }
    ]
    info = {"metadata": metadata, "timestamp": now}

    return pings, info


# Transform each ping to make it easier to work with in later stages.
def validate(p):
    name = p.get("environment/system/os/name") or "w"
    version = p.get("environment/system/os/version") or "0"
    if name == "Linux":
        p["OSVersion"] = None
        p["OS"] = "Linux"
        p["OSName"] = "Linux"
    elif name == "Windows_NT":
        spmaj = p.get("environment/system/os/servicePackMajor") or "0"
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

    # Telemetry data isn't guaranteed to be well-formed so unfortunately
    # we have to do some validation on it. If we get to the end, we set
    # p['valid'] to True, and this gets filtered over later. In addition
    # we have a wrapper below to help fetch strings that may be null.
    if not p.get("environment/build/version", None):
        return p
    p["FxVersion"] = p["environment/build/version"].split(".")[0]

    # Verify that we have at least one adapter.
    try:
        adapter = p["environment/system/gfx/adapters"][0]
    except (KeyError, IndexError, TypeError):
        return p
    if adapter is None or not hasattr(adapter, "__getitem__"):
        return p

    def t(obj, key):
        return obj.get(key, None) or "Unknown"

    # We store the device ID as a vendor/device string, because the device ID
    # alone is not enough to determine whether the key is unique.
    #
    # We also merge 'Intel Open Source Technology Center' with the device ID
    # that should be reported, 0x8086, for simplicity.
    vendor_id = t(adapter, "vendorID")
    if vendor_id == "Intel Open Source Technology Center":
        p["vendorID"] = "0x8086"
    else:
        p["vendorID"] = vendor_id
    p["deviceID"] = "{0}/{1}".format(p["vendorID"], t(adapter, "deviceID"))
    p["driverVersion"] = "{0}/{1}".format(p["vendorID"], t(adapter, "driverVersion"))
    p["deviceAndDriver"] = "{0}/{1}".format(p["deviceID"], t(adapter, "driverVersion"))
    p["driverVendor"] = adapter.get("driverVendor", None)

    p["valid"] = True
    return p


def reduce_pings(pings):
    return get_pings_properties(
        pings,
        [
            "clientId",
            "creationDate",
            "environment/build/version",
            "environment/build/buildId",
            "environment/system/memoryMB",
            "environment/system/isWow64",
            "environment/system/cpu",
            "environment/system/os/name",
            "environment/system/os/version",
            "environment/system/os/servicePackMajor",
            "environment/system/gfx/adapters",
            "payload/info/revision",
        ]
        + PropertyList,
    )


def format_pings(pings):
    pings = pings.map(dashboard.convert_bigquery_results)
    pings = reduce_pings(pings)
    pings = pings.map(snake_case.convert_snake_case_dict)
    pings = pings.map(validate)
    filtered_pings = pings.filter(lambda p: p.get("valid", False))
    return filtered_pings.cache()


def fetch_and_format(**kwargs):
    raw_pings, info = fetch_raw_pings(**kwargs)
    return format_pings(raw_pings), info


##################################################################
# Helper function block for massaging pings into aggregate data. #
##################################################################


# Take each key in |b| and add it to |a|, accumulating its value into
# |a| if it already exists.
def combiner(a, b):
    result = a
    for key in b:
        count_a = a.get(key, 0)
        count_b = b[key]
        result[key] = count_a + count_b
    return result


# Helper for reduceByKey => count.
def map_x_to_count(data, source_key):
    def extract(p):
        return (p[source_key],)

    return data.map(extract).countByKey()


# After reduceByKey(combiner), we get a mapping like:
#  key => { variable => value }
#
# This function collapses 'variable' instances below a threshold into
# a catch-all identifier ('Other').
def coalesce_to_n_items(agg, max_items):
    obj = []
    for superkey, breakdown in agg:
        if len(breakdown) <= max_items:
            obj += [(superkey, breakdown)]
            continue
        items = sorted(breakdown.items(), key=lambda obj: obj[1], reverse=True)
        new_breakdown = {k: v for k, v in items[0:max_items]}
        total = 0
        for k, v in items[max_items:]:
            total += v
        if total:
            new_breakdown["Other"] = new_breakdown.get("Other", 0) + total
        obj += [(superkey, new_breakdown)]
    return obj


#############################
# Helper for writing files. #
#############################


def apply_ping_info(obj, **kwargs):
    if "pings" not in kwargs:
        return

    pings, info = kwargs.pop("pings")

    # To make the sample source information more transparent, we include
    # the breakdown of Firefox channel numbers.
    if "__share" not in info:
        info["__share"] = map_x_to_count(pings, "FxVersion")

    obj["sessions"] = {
        "count": pings.count(),
        "timestamp": time.mktime(info["timestamp"].timetuple()),
        "shortdate": fmt_date(info["timestamp"]),
        "metadata": info["metadata"],
        "share": info["__share"],
    }


def export(filename, obj, **kwargs):
    full_filename = os.path.join(OUTPUT_PREFIX, f"{filename}.json")
    print("Writing to {0}".format(full_filename))
    # serialize snake case dicts via their underlying dict
    bucket = storage_client.get_bucket(OUTPUT_BUCKET)
    blob = bucket.blob(full_filename)

    # serialize snake case dicts via their underlying dict
    blob.upload_from_string(
        json.dumps(obj, cls=snake_case.SnakeCaseEncoder),
        content_type="application/json",
    )


def timed_export(filename, callback, **kwargs):
    start = datetime.datetime.now()

    obj = callback()
    apply_ping_info(obj, **kwargs)

    end = datetime.datetime.now()
    elapsed = end - start
    obj["phaseTime"] = elapsed.total_seconds()

    export(filename, obj, **kwargs)
    export_time = datetime.datetime.now() - end

    print("Computed {0} in {1} seconds.".format(filename, elapsed.total_seconds()))
    print("Exported {0} in {1} seconds.".format(filename, export_time.total_seconds()))


# Profiler for debugging.
class Prof(object):
    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self.sout("Starting {0}... ".format(self.name))
        self.start = datetime.datetime.now()
        return None

    def __exit__(self, type, value, traceback):
        self.end = datetime.datetime.now()
        self.sout(
            "... {0}: {1}s".format(self.name, (self.end - self.start).total_seconds())
        )

    def sout(self, s):
        sys.stdout.write(s)
        sys.stdout.write("\n")
        sys.stdout.flush()


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


quiet_logs(sc)

# Get a general ping sample across all Firefox channels.
with Prof("General pings"):
    general_pings, general_ping_info = fetch_and_format()
    general_pings = general_pings.cache()

    # Windows gets some preferential breakdown treatment.
    windows_pings = general_pings.filter(lambda p: p["OSName"] == "Windows")
    windows_pings = windows_pings.cache()

    mac_pings = general_pings.filter(lambda p: p["OSName"] == "Darwin")
    mac_pings = repartition(mac_pings)

    linux_pings = general_pings.filter(lambda p: p["OSName"] == "Linux")
    linux_pings = repartition(linux_pings)

# # ANALYSES ARE BELOW

# ## General statistics

# Results by operating system.
if "__share" not in general_ping_info:
    general_ping_info["__share"] = map_x_to_count(general_pings, "FxVersion")


def get_general_statistics_for_subset(subset, windows_subset):
    os_share = map_x_to_count(subset, "OSName")

    # Results by Windows version.
    windows_share = map_x_to_count(windows_subset, "OSVersion")

    # Top-level stats.
    vendor_share = map_x_to_count(subset, "vendorID")

    return {"os": os_share, "windows": windows_share, "vendors": vendor_share}


def get_general_statistics():
    obj = {}
    obj["devices"] = map_x_to_count(general_pings, "deviceID")
    obj["drivers"] = map_x_to_count(general_pings, "driverVersion")

    by_fx = {}
    with Prof("general stats for all"):
        by_fx["all"] = get_general_statistics_for_subset(general_pings, windows_pings)

    for key in general_ping_info["__share"]:
        subset = general_pings.filter(lambda p: p["FxVersion"] == key)
        windows = subset.filter(lambda p: p["OSName"] == "Windows")
        subset = repartition(subset)
        windows = repartition(windows)
        with Prof("general stats for " + key):
            by_fx[key] = get_general_statistics_for_subset(subset, windows)

    obj["byFx"] = by_fx
    return obj


timed_export(
    filename="general-statistics",
    callback=get_general_statistics,
    pings=(general_pings, general_ping_info),
)


# ## Device/driver search database


def get_driver_statistics():
    obj = {"deviceAndDriver": map_x_to_count(general_pings, "deviceAndDriver")}
    return obj


timed_export(
    filename="device-statistics",
    callback=get_driver_statistics,
    save_history=False,  # No demand yet, and too much data.
    pings=(general_pings, general_ping_info),
)


# ## TDR Statistics


#############################
# Perform the TDR analysis. #
#############################
def get_tdr_statistics():
    num_tdr_reasons = 8

    def ping_has_tdr_for(p, reason):
        return p[DeviceResetReasonKey][reason] > 0

    # Specialized version of map_x_to_y, for TDRs. We cast to int because for
    # some reason the values Spark returns do not serialize with JSON.
    def map_reason_to_vendor(p, reason, dest_key):
        return (int(reason), {p[dest_key]: int(p[DeviceResetReasonKey][reason])})

    def map_vendor_to_reason(p, reason, dest_key):
        return (p[dest_key], {int(reason): int(p[DeviceResetReasonKey][reason])})

    # Filter out pings that do not have any TDR data. We expect this to be a huge reduction
    # in the sample set, and the resulting partition count gets way off. We repartition
    # immediately for performance.
    tdr_subset = windows_pings.filter(
        lambda p: p.get(DeviceResetReasonKey, None) is not None
    )
    tdr_subset = tdr_subset.repartition(MaxPartitions)
    tdr_subset = tdr_subset.cache()

    # Aggregate the device reset data.
    tdr_results = tdr_subset.map(lambda p: p[DeviceResetReasonKey]).reduce(
        lambda x, y: x + y
    )

    # For each TDR reason, get a list tuple of (reason, vendor => resetCount). Then
    # we combine these into a single series.
    reason_to_vendor_tuples = None
    vendor_to_reason_tuples = None
    for reason in range(1, num_tdr_reasons):
        subset = tdr_subset.filter(lambda p: ping_has_tdr_for(p, reason))
        subset = subset.cache()

        tuples = subset.map(lambda p: map_reason_to_vendor(p, reason, "vendorID"))
        reason_to_vendor_tuples = union_pipelines(reason_to_vendor_tuples, tuples)

        tuples = subset.map(lambda p: map_vendor_to_reason(p, reason, "vendorID"))
        vendor_to_reason_tuples = union_pipelines(vendor_to_reason_tuples, tuples)

    tdr_reason_to_vendor = reason_to_vendor_tuples.reduceByKey(combiner, MaxPartitions)
    tdr_vendor_to_reason = vendor_to_reason_tuples.reduceByKey(combiner, MaxPartitions)

    return {
        "tdrPings": tdr_subset.count(),
        "results": [int(value) for value in tdr_results],
        "reasonToVendor": tdr_reason_to_vendor.collect(),
        "vendorToReason": tdr_vendor_to_reason.collect(),
    }


# Write TDR statistics.
timed_export(
    filename="tdr-statistics",
    callback=get_tdr_statistics,
    pings=(windows_pings, general_ping_info),
)

# ## System Statistics

##########################
# Get system statistics. #
##########################
CpuKey = "environment/system/cpu"
MemoryKey = "environment/system/memoryMB"


def get_bucketed_memory(pings):
    def get_bucket(p):
        x = int(p / 1000)
        if x < 1:
            return "less_1gb"
        if x <= 4:
            return x
        if x <= 8:
            return "4_to_8"
        if x <= 16:
            return "8_to_16"
        if x <= 32:
            return "16_to_32"
        return "more_32"

    memory_rdd = pings.map(lambda p: p.get(MemoryKey, 0))
    memory_rdd = memory_rdd.filter(lambda p: p > 0)
    memory_rdd = memory_rdd.map(lambda p: (get_bucket(p),))
    memory_rdd = repartition(memory_rdd)
    return memory_rdd.countByKey()


def get_cpu_features(pings):
    cpuid_rdd = pings.map(lambda p: p.get(CpuKey, None))
    cpuid_rdd = cpuid_rdd.filter(lambda p: p is not None)
    cpuid_rdd = cpuid_rdd.map(lambda p: p.get("extensions", None))

    # Unfortunately, Firefox 39 had a bug where CPU features could be reported even
    # if they weren't present. To detect this we filter pings that have ARMv6 support
    # on x86/64.
    cpuid_rdd = cpuid_rdd.filter(lambda p: p is not None and "hasARMv6" not in p)
    cpuid_rdd = repartition(cpuid_rdd)

    # Count before we blow up the list.
    with Prof("cpu count for x86"):
        total = cpuid_rdd.count()

    cpuid_rdd = cpuid_rdd.flatMap(lambda p: [(ex, 1) for ex in p])
    with Prof("cpu features for x86"):
        feature_map = cpuid_rdd.countByKey()

    return {"total": total, "features": feature_map}


def get_system_statistics():
    def get_logical_cores(p):
        cpu = p.get(CpuKey, None)
        if cpu is None:
            return "unknown"
        return cpu.get("count", "unknown")

    with Prof("logical cores"):
        logical_cores = general_pings.map(
            lambda p: (get_logical_cores(p),)
        ).countByKey()

    cpu_features = get_cpu_features(general_pings)

    with Prof("memory buckets"):
        memory = get_bucketed_memory(general_pings)

    def get_os_bits(p):
        arch = p.get(ArchKey, "unknown")
        if arch == "x86-64":
            return "64"
        if arch == "x86":
            wow64 = p.get("environment/system/isWow64", False)
            if wow64:
                return "32_on_64"
            return "32"
        return "unknown"

    with Prof("OS bit count"):
        os_bits = windows_pings.map(lambda p: (get_os_bits(p),)).countByKey()

    return {
        "logical_cores": logical_cores,
        "x86": cpu_features,
        "memory": memory,
        "wow": os_bits,
    }


timed_export(
    filename="system-statistics",
    callback=get_system_statistics,
    pings=(general_pings, general_ping_info),
)

# ## Sanity Test Statistics

# Set up constants.
SANITY_TEST_PASSED = 0
SANITY_TEST_FAILED_RENDER = 1
SANITY_TEST_FAILED_VIDEO = 2
SANITY_TEST_CRASHED = 3
SANITY_TEST_TIMEDOUT = 4
SANITY_TEST_LAST_VALUE = 5


# We don't want to fold FAILED_LAYERS and FAILED_VIDEO into the same
# resultset, so we use this function to split them out.
def get_sanity_test_result(p):
    if p.get(SANITY_TEST, None) is None:
        return None
    if p[SANITY_TEST][SANITY_TEST_PASSED] > 0:
        return SANITY_TEST_PASSED
    if p[SANITY_TEST][SANITY_TEST_CRASHED] > 0:
        return SANITY_TEST_CRASHED
    if p[SANITY_TEST][SANITY_TEST_FAILED_RENDER] > 0:
        return SANITY_TEST_FAILED_RENDER
    if p[SANITY_TEST][SANITY_TEST_FAILED_VIDEO] > 0:
        return SANITY_TEST_FAILED_VIDEO
    if p[SANITY_TEST][SANITY_TEST_TIMEDOUT] > 0:
        return SANITY_TEST_TIMEDOUT
    return None


#########################
# Sanity test analysis. #
#########################
def get_sanity_tests_for_slice(sanity_test_pings):
    data = sanity_test_pings.filter(lambda p: get_sanity_test_result(p) is not None)

    # Aggregate the sanity test data.
    with Prof("initial map"):
        sanity_test_results = data.map(
            lambda p: (get_sanity_test_result(p),)
        ).countByKey()

    with Prof("share resolve"):
        os_share = map_x_to_count(data, "OSVersion")

    with Prof("ping_count"):
        sanity_test_count = data.count()
        ping_count = sanity_test_pings.count()

    sanity_test_by_vendor = None
    sanity_test_by_os = None
    sanity_test_by_device = None
    sanity_test_by_driver = None
    for value in range(SANITY_TEST_FAILED_RENDER, SANITY_TEST_LAST_VALUE):
        subset = data.filter(lambda p: get_sanity_test_result(p) == value)

        tuples = subset.map(
            lambda p: (value, {p["vendorID"]: int(p[SANITY_TEST][value])})
        )
        sanity_test_by_vendor = union_pipelines(sanity_test_by_vendor, tuples)

        tuples = subset.map(lambda p: (value, {p["OS"]: int(p[SANITY_TEST][value])}))
        sanity_test_by_os = union_pipelines(sanity_test_by_os, tuples)

        tuples = subset.map(
            lambda p: (value, {p["deviceID"]: int(p[SANITY_TEST][value])})
        )
        sanity_test_by_device = union_pipelines(sanity_test_by_device, tuples)

        tuples = subset.map(
            lambda p: (value, {p["driverVersion"]: int(p[SANITY_TEST][value])})
        )
        sanity_test_by_driver = union_pipelines(sanity_test_by_driver, tuples)

    sanity_test_by_vendor = repartition(sanity_test_by_vendor)
    sanity_test_by_os = repartition(sanity_test_by_os)
    sanity_test_by_device = repartition(sanity_test_by_device)
    sanity_test_by_driver = repartition(sanity_test_by_driver)

    with Prof("vendor resolve"):
        sanity_test_by_vendor = sanity_test_by_vendor.reduceByKey(combiner)
    with Prof("os resolve"):
        sanity_test_by_os = sanity_test_by_os.reduceByKey(combiner)
    with Prof("device resolve"):
        sanity_test_by_device = sanity_test_by_device.reduceByKey(combiner)
    with Prof("driver resolve"):
        sanity_test_by_driver = sanity_test_by_driver.reduceByKey(combiner)

    print(
        "Partitions: {0},{1},{2},{3}".format(
            sanity_test_by_vendor.getNumPartitions(),
            sanity_test_by_os.getNumPartitions(),
            sanity_test_by_device.getNumPartitions(),
            sanity_test_by_driver.getNumPartitions(),
        )
    )

    with Prof("vendor collect"):
        by_vendor = sanity_test_by_vendor.collect()
    with Prof("os collect"):
        by_os = sanity_test_by_os.collect()
    with Prof("device collect"):
        by_device = sanity_test_by_device.collect()
    with Prof("driver collect"):
        by_driver = sanity_test_by_driver.collect()

    return {
        "sanityTestPings": sanity_test_count,
        "totalPings": ping_count,
        "results": sanity_test_results,
        "byVendor": by_vendor,
        "byOS": by_os,
        "byDevice": coalesce_to_n_items(by_device, 10),
        "byDriver": coalesce_to_n_items(by_driver, 10),
        "windows": os_share,
    }


def get_sanity_tests():
    obj = {}
    obj["windows"] = get_sanity_tests_for_slice(windows_pings)
    return obj


# Write Sanity Test statistics.
timed_export(
    filename="sanity-test-statistics",
    callback=get_sanity_tests,
    pings=(windows_pings, general_ping_info),
)

# ## Startup Crash Guard Statistics

STARTUP_OK = 0
STARTUP_ENV_CHANGED = 1
STARTUP_CRASHED = 2
STARTUP_ACCEL_DISABLED = 3


def get_startup_tests():
    startup_test_pings = general_pings.filter(
        lambda p: p.get(STARTUP_TEST_KEY, None) is not None
    )
    startup_test_pings = startup_test_pings.repartition(MaxPartitions)
    startup_test_pings = startup_test_pings.cache()

    startup_test_results = startup_test_pings.map(lambda p: p[STARTUP_TEST_KEY]).reduce(
        lambda x, y: x + y
    )

    os_share = map_x_to_count(startup_test_pings, "OS")

    return {
        "startupTestPings": startup_test_pings.count(),
        "results": [int(i) for i in startup_test_results],
        "windows": os_share,
    }


# Write startup test results.
timed_export(
    filename="startup-test-statistics",
    callback=get_startup_tests,
    pings=(general_pings, general_ping_info),
)


# ## Monitor Statistics


def get_monitor_count(p):
    monitors = p.get(MonitorsKey, None)
    try:
        return len(monitors)
    except TypeError:
        return 0


def get_monitor_res(p, i):
    width = p[MonitorsKey][i].get("screenWidth", 0)
    height = p[MonitorsKey][i].get("screenHeight", 0)
    if width == 0 or height == 0:
        return "Unknown"
    return "{0}x{1}".format(width, height)


def get_monitor_statistics():
    def get_monitor_rdds_for_index(data, i):
        def get_refresh_rate(p):
            refresh_rate = p[MonitorsKey][i].get("refreshRate", 0)
            return refresh_rate if refresh_rate > 1 else "Unknown"

        def get_resolution(p):
            return get_monitor_res(p, i)

        monitors_at_index = data.filter(lambda p: get_monitor_count(p) == monitor_count)
        monitors_at_index = repartition(monitors_at_index)
        refresh_rates = monitors_at_index.map(lambda p: (get_refresh_rate(p),))
        resolutions = monitors_at_index.map(lambda p: (get_resolution(p),))
        return refresh_rates, resolutions

    monitor_counts = windows_pings.map(lambda p: (get_monitor_count(p),)).countByKey()
    monitor_counts.pop(0, None)

    refresh_rates = None
    resolutions = None
    for monitor_count in monitor_counts:
        rate_subset, res_subset = get_monitor_rdds_for_index(
            windows_pings, monitor_count - 1
        )
        refresh_rates = union_pipelines(refresh_rates, rate_subset)
        resolutions = union_pipelines(resolutions, res_subset)

    monitor_refresh_rates = refresh_rates.countByKey()
    monitor_resolutions = resolutions.countByKey()

    return {
        "counts": monitor_counts,
        "refreshRates": monitor_refresh_rates,
        "resolutions": monitor_resolutions,
    }


timed_export(
    filename="monitor-statistics",
    callback=get_monitor_statistics,
    pings=(windows_pings, general_ping_info),
)

# ## Mac OS X Statistics

mac_pings = general_pings.filter(lambda p: p["OSName"] == "Darwin")
mac_pings = repartition(mac_pings)


def get_mac_statistics():
    version_map = map_x_to_count(mac_pings, "OSVersion")

    def get_scale(p):
        monitors = p.get(MonitorsKey, None)
        if not monitors:
            return "unknown"
        try:
            return monitors[0]["scale"]
        except (KeyError, IndexError):
            "unknown"

    scale_map = mac_pings.map(lambda p: (get_scale(p),)).countByKey()

    def get_arch(p):
        arch = p.get(ArchKey, "unknown")
        if arch == "x86-64":
            return "64"
        if arch == "x86":
            return "32"
        return "unknown"

    arch_map = mac_pings.map(lambda p: (get_arch(p),)).countByKey()

    return {"versions": version_map, "retina": scale_map, "arch": arch_map}


timed_export(
    filename="mac-statistics",
    callback=get_mac_statistics,
    pings=(mac_pings, general_ping_info),
)


# ### Helpers for Compositor/Acceleration fields


# Build graphics feature statistics.
def get_compositor(p):
    compositor = p[FeaturesKey].get("compositor", "none")
    if compositor == "none":
        user_prefs = p.get(UserPrefsKey, None)
        if user_prefs is not None:
            omtc = user_prefs.get("layers.offmainthreadcomposition.enabled", True)
            if not omtc:
                return "disabled"
    elif compositor == "d3d11":
        if advanced_layers_status(p) == "available":
            return "advanced_layers"
    return compositor


def get_d3d11_status(p):
    d3d11 = p[FeaturesKey].get("d3d11", None)
    if not hasattr(d3d11, "__getitem__"):
        return "unknown"
    status = d3d11.get("status", "unknown")
    if status != "available":
        return status
    if d3d11.get("warp", False):
        return "warp"
    return d3d11.get("version", "unknown")


def get_warp_status(p):
    if "blacklisted" not in p[FeaturesKey]["d3d11"]:
        return "unknown"
    if p[FeaturesKey]["d3d11"]["blacklisted"]:
        return "blacklist"
    return "device failure"


def get_d2d_status(p):
    d2d = p[FeaturesKey].get("d2d", None)
    if not hasattr(d2d, "__getitem__"):
        return ("unknown",)
    status = d2d.get("status", "unknown")
    if status != "available":
        return (status,)
    return (d2d.get("version", "unknown"),)


def has_working_d3d11(p):
    d3d11 = p[FeaturesKey].get("d3d11", None)
    if d3d11 is None:
        return False
    return d3d11.get("status") == "available"


def gpu_process_status(p):
    gpu_proc = p[FeaturesKey].get("gpuProcess", None)
    if gpu_proc is None or not gpu_proc.get("status", None):
        return "none"
    return gpu_proc.get("status")


def get_texture_sharing_status(p):
    return (p[FeaturesKey]["d3d11"].get("textureSharing", "unknown"),)


def advanced_layers_status(p):
    al = p[FeaturesKey].get("advancedLayers", None)
    if al is None:
        return "none"
    return al.get("status", None)


# ## Windows Compositor and Blacklisting Statistics


# Get pings with graphics features. This landed in roughly the 7-19-2015 nightly.
def windows_feature_filter(p):
    return p["OSName"] == "Windows" and p.get(FeaturesKey) is not None


windows_features = windows_pings.filter(lambda p: p.get(FeaturesKey) is not None)
windows_features = windows_features.cache()

# We skip certain windows versions in detail lists since this phase is
# very expensive to compute.
important_windows_versions = ("6.1.0", "6.1.1", "6.2.0", "6.3.0", "10.0.0")


def get_windows_features():
    windows_compositor_map = windows_features.map(
        lambda p: (get_compositor(p),)
    ).countByKey()
    d3d11_status_map = windows_features.map(
        lambda p: (get_d3d11_status(p),)
    ).countByKey()
    d2d_status_map = windows_features.map(get_d2d_status).countByKey()

    def get_content_backends(rdd):
        rdd = rdd.filter(lambda p: p[GfxKey].get("ContentBackend") is not None)
        rdd = rdd.map(lambda p: (p[GfxKey]["ContentBackend"],))
        return rdd.countByKey()

    content_backends = get_content_backends(windows_features)

    warp_pings = windows_features.filter(lambda p: get_d3d11_status(p) == "warp")
    warp_pings = repartition(warp_pings)
    warp_status_map = warp_pings.map(lambda p: (get_warp_status(p),)).countByKey()

    texture_sharing_map = (
        windows_features.filter(has_working_d3d11)
        .map(get_texture_sharing_status)
        .countByKey()
    )

    blacklisted_pings = windows_features.filter(
        lambda p: get_d3d11_status(p) == "blacklisted"
    )
    blacklisted_pings = repartition(blacklisted_pings)
    blacklisted_devices = map_x_to_count(blacklisted_pings, "deviceID")
    blacklisted_drivers = map_x_to_count(blacklisted_pings, "driverVersion")
    blacklisted_os = map_x_to_count(blacklisted_pings, "OSVersion")

    blocked_pings = windows_features.filter(lambda p: get_d3d11_status(p) == "blocked")
    blocked_pings = repartition(blocked_pings)
    blocked_vendors = map_x_to_count(blocked_pings, "vendorID")

    # Plugin models.
    def aggregate_plugin_models(rdd):
        rdd = rdd.filter(lambda p: p.get(PluginModelKey) is not None)
        rdd = rdd.map(lambda p: p.get(PluginModelKey))

        if rdd.isEmpty():
            return list()

        result = rdd.reduce(lambda x, y: x + y)
        return [int(count) for count in result]

    plugin_models = aggregate_plugin_models(windows_features)

    # Media decoder backends.
    def get_media_decoders(rdd):
        rdd = rdd.filter(lambda p: p.get(MediaDecoderKey, None) is not None)
        if rdd.count() == 0:
            # These three values correspond to WMF Software, DXVA D3D11, and DXVA D3D9
            return [0, 0, 0]
        decoders = rdd.map(lambda p: p.get(MediaDecoderKey)).reduce(lambda x, y: x + y)
        return [int(i) for i in decoders]

    media_decoders = get_media_decoders(windows_features)

    def gpu_process_map(rdd):
        return rdd.map(lambda p: (gpu_process_status(p),)).countByKey()

    def advanced_layers_map(rdd):
        return rdd.map(lambda p: (advanced_layers_status(p),)).countByKey()

    # Now, build the same data except per version.
    feature_pings_by_os = map_x_to_count(windows_features, "OSVersion")
    windows_features_by_version = {}
    for os_version in feature_pings_by_os:
        if os_version not in important_windows_versions:
            continue
        subset = windows_features.filter(lambda p: p["OSVersion"] == os_version)
        subset = repartition(subset)

        results = {
            "count": subset.count(),
            "compositors": subset.map(lambda p: (get_compositor(p),)).countByKey(),
            "plugin_models": aggregate_plugin_models(subset),
            "content_backends": get_content_backends(subset),
            "media_decoders": get_media_decoders(subset),
            "gpu_process": gpu_process_map(subset),
            "advanced_layers": advanced_layers_map(subset),
        }
        try:
            if int(os_version.split(".")[0]) >= 6:
                results["d3d11"] = subset.map(
                    lambda p: (get_d3d11_status(p),)
                ).countByKey()
                results["d2d"] = subset.map(get_d2d_status).countByKey()

                warp_pings = subset.filter(lambda p: get_d3d11_status(p) == "warp")
                results["warp"] = warp_pings.map(
                    lambda p: (get_warp_status(p),)
                ).countByKey()
        except Exception:
            pass
        finally:
            # Free resources.
            warp_pings = None
            subset = None
        windows_features_by_version[os_version] = results

    return {
        "all": {
            "compositors": windows_compositor_map,
            "content_backends": content_backends,
            "d3d11": d3d11_status_map,
            "d2d": d2d_status_map,
            "textureSharing": texture_sharing_map,
            "warp": warp_status_map,
            "plugin_models": plugin_models,
            "media_decoders": media_decoders,
            "gpu_process": gpu_process_map(windows_features),
            "advanced_layers": advanced_layers_map(windows_features),
        },
        "byVersion": windows_features_by_version,
        "d3d11_blacklist": {
            "devices": blacklisted_devices,
            "drivers": blacklisted_drivers,
            "os": blacklisted_os,
        },
        "d3d11_blocked": {"vendors": blocked_vendors},
    }


timed_export(
    filename="windows-features",
    callback=get_windows_features,
    pings=(windows_features, general_ping_info),
)

windows_features = None

# ## Linux

linux_pings = general_pings.filter(lambda p: p["OSName"] == "Linux")
linux_pings = repartition(linux_pings)


def get_linux_statistics():
    pings = linux_pings.filter(lambda p: p["driverVendor"] is not None)
    driver_vendor_map = map_x_to_count(pings, "driverVendor")

    pings = linux_pings.filter(lambda p: p.get(FeaturesKey) is not None)
    compositor_map = pings.map(lambda p: (get_compositor(p),)).countByKey()

    return {"driverVendors": driver_vendor_map, "compositors": compositor_map}


timed_export(
    filename="linux-statistics",
    callback=get_linux_statistics,
    pings=(linux_pings, general_ping_info),
)


# ## WebGL Statistics
# Note, this depends on running the
# "Helpers for Compositor/Acceleration fields" a few blocks above.


def get_gl_statistics():
    webgl_status_rdd = general_pings.filter(
        lambda p: p.get(WebGLFailureKey, None) is not None
    )
    webgl_status_rdd = webgl_status_rdd.map(lambda p: p[WebGLFailureKey])
    webgl_status_map = webgl_status_rdd.reduce(combiner)
    webgl_accl_status_rdd = general_pings.filter(
        lambda p: p.get(WebGLAcclFailureKey, None) is not None
    )
    webgl_accl_status_rdd = webgl_accl_status_rdd.map(lambda p: p[WebGLAcclFailureKey])
    webgl_accl_status_map = webgl_accl_status_rdd.reduce(combiner)
    return {
        "webgl": {
            "acceleration_status": webgl_accl_status_map,
            "status": webgl_status_map,
        }
    }


def web_gl_statistics_for_key(key):
    histogram_pings = general_pings.filter(lambda p: p.get(key) is not None)
    histogram_pings = repartition(histogram_pings)

    # Note - we're counting sessions where WebGL succeeded or failed,
    # rather than the raw number of times either succeeded or failed.
    # Also note that we don't double-count systems where both is true.
    # Instead we only count a session's successes if it had no failures.
    failure_rdd = histogram_pings.filter(lambda p: p[key][0] > 0)
    success_rdd = histogram_pings.filter(lambda p: p[key][0] == 0 and p[key][1] > 0)

    failure_count = failure_rdd.count()
    failure_by_os = map_x_to_count(failure_rdd, "OS")
    failure_by_vendor = map_x_to_count(failure_rdd, "vendorID")
    failure_by_device = map_x_to_count(failure_rdd, "deviceID")
    failure_by_driver = map_x_to_count(failure_rdd, "driverVersion")

    success_count = success_rdd.count()
    success_by_os = map_x_to_count(success_rdd, "OS")

    def get_compositor_any_os(p):
        if p["OSName"] != "Windows":
            # This data is not reliable yet - see bug 1247148.
            return "unknown"
        return get_compositor(p)

    success_by_cc = success_rdd.map(lambda p: (get_compositor_any_os(p),)).countByKey()

    return {
        "successes": {
            "count": success_count,
            "os": success_by_os,
            "compositors": success_by_cc,
        },
        "failures": {
            "count": failure_count,
            "os": failure_by_os,
            "vendors": failure_by_vendor,
            "devices": failure_by_device,
            "drivers": failure_by_driver,
        },
    }


def get_web_gl_statistics():
    return {
        "webgl1": web_gl_statistics_for_key(WebGLSuccessKey),
        "webgl2": web_gl_statistics_for_key(WebGL2SuccessKey),
        "general": get_gl_statistics(),
    }


timed_export(
    filename="webgl-statistics",
    callback=get_web_gl_statistics,
    pings=(general_pings, general_ping_info),
)


def get_layers_status():
    d3d11_status_rdd = general_pings.filter(
        lambda p: p.get(LayersD3D11FailureKey, None) is not None
    )
    d3d11_status_rdd = d3d11_status_rdd.map(lambda p: p[LayersD3D11FailureKey])
    d3d11_status_map = d3d11_status_rdd.reduce(combiner)
    ogl_status_rdd = general_pings.filter(
        lambda p: p.get(LayersOGLFailureKey, None) is not None
    )
    ogl_status_rdd = ogl_status_rdd.map(lambda p: p[LayersOGLFailureKey])
    ogl_status_map = ogl_status_rdd.reduce(combiner)
    print(d3d11_status_map)
    print(ogl_status_map)
    return {"layers": {"d3d11": d3d11_status_map, "opengl": ogl_status_map}}


def get_layers_statistics():
    return {"general": get_layers_status()}


timed_export(
    filename="layers-failureid-statistics",
    callback=get_layers_statistics,
    pings=(general_pings, general_ping_info),
)

end_time = datetime.datetime.now()
total_elapsed = (end_time - start_time).total_seconds()

print("Total time: {0}".format(total_elapsed))
