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

import boto3
from bigquery_shim import dashboard, snake_case
from moztelemetry import get_pings_properties
from pyspark import SparkContext
from pyspark.sql import SparkSession


def fmt_date(d):
    return d.strftime("%Y%m%d")


def repartition(pipeline):
    return pipeline.repartition(MaxPartitions).cache()


s3_client = boto3.client("s3")

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("graphics-trends").getOrCreate()

MaxPartitions = sc.defaultParallelism * 4
StartTime = datetime.datetime.now()

# Going forward we only care about sessions from Firefox 53+, since it
# is the first release to not support Windows XP and Vista, which distorts
# our statistics.
MinFirefoxVersion = "53"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--default-time-window", type=int, default=14)
    parser.add_argument("--release-fraction", type=float, default=0.003)
    parser.add_argument("--output-bucket", default="telemetry-public-analysis-2")
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

existing_objects = [
    obj["Key"]
    for obj in s3_client.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=OUTPUT_PREFIX)[
        "Contents"
    ]
]
print(f"Existing objects: {existing_objects}")

# List of keys for properties on session pings that we care about.
GfxKey = "environment/system/gfx"
MonitorsKey = "environment/system/gfx/monitors"
ArchKey = "environment/build/architecture"
FeaturesKey = "environment/system/gfx/features"
UserPrefsKey = "environment/settings/userPrefs"
DeviceResetReasonKey = "payload/histograms/DEVICE_RESET_REASON"
SANITY_TEST = "payload/histograms/GRAPHICS_SANITY_TEST"
SANITY_TEST_REASON = "payload/histograms/GRAPHICS_SANITY_TEST_REASON"
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
    SANITY_TEST_REASON,
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


def FetchRawPings(**kwargs):
    timeWindow = kwargs.pop("timeWindow", DefaultTimeWindow)
    fraction = kwargs.pop("fraction", ReleaseFraction)
    channel = kwargs.pop("channel", None)

    # Since builds take a bit to disseminate, go back about 4 hours. This is a
    # completely made up number.
    limit = datetime.timedelta(0, 60 * 60 * 4)
    now = datetime.datetime.now()
    start = now - datetime.timedelta(timeWindow) - limit
    end = now - limit

    # NOTE: ReleaseFraction is not used in the shim
    pings = dashboard.fetch_results(
        spark, start, end, channel=channel, min_firefox_version=MinFirefoxVersion
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
def Validate(p):
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
    except:
        return p
    if adapter is None or not hasattr(adapter, "__getitem__"):
        return p

    def T(obj, key):
        return obj.get(key, None) or "Unknown"

    # We store the device ID as a vendor/device string, because the device ID
    # alone is not enough to determine whether the key is unique.
    #
    # We also merge 'Intel Open Source Technology Center' with the device ID
    # that should be reported, 0x8086, for simplicity.
    vendorID = T(adapter, "vendorID")
    if vendorID == "Intel Open Source Technology Center":
        p["vendorID"] = "0x8086"
    else:
        p["vendorID"] = vendorID
    p["deviceID"] = "{0}/{1}".format(p["vendorID"], T(adapter, "deviceID"))
    p["driverVersion"] = "{0}/{1}".format(p["vendorID"], T(adapter, "driverVersion"))
    p["deviceAndDriver"] = "{0}/{1}".format(p["deviceID"], T(adapter, "driverVersion"))
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


def FormatPings(pings):
    pings = pings.map(dashboard.convert_bigquery_results)
    pings = reduce_pings(pings)
    pings = pings.map(snake_case.convert_snake_case_dict)
    pings = pings.map(Validate)
    filtered_pings = pings.filter(lambda p: p.get("valid", False) == True)
    return filtered_pings.cache()


def FetchAndFormat(**kwargs):
    raw_pings, info = FetchRawPings(**kwargs)
    return FormatPings(raw_pings), info


##################################################################
# Helper function block for massaging pings into aggregate data. #
##################################################################

# Take each key in |b| and add it to |a|, accumulating its value into
# |a| if it already exists.
def combiner(a, b):
    result = a
    for key in b:
        countA = a.get(key, 0)
        countB = b[key]
        result[key] = countA + countB
    return result


# Helper for reduceByKey => count.
def map_x_to_count(data, sourceKey):
    def extract(p):
        return (p[sourceKey],)

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


def ApplyPingInfo(obj, **kwargs):
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


def Export(filename, obj, **kwargs):
    full_filename = os.path.join(OUTPUT_PREFIX, f"{filename}.json")
    print("Writing to {0}".format(full_filename))
    # serialize snake case dicts via their underlying dict
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=full_filename,
        Body=bytes(json.dumps(obj, cls=snake_case.SnakeCaseEncoder), encoding="utf-8"),
    )


def TimedExport(filename, callback, **kwargs):
    start = datetime.datetime.now()

    obj = callback()
    ApplyPingInfo(obj, **kwargs)

    end = datetime.datetime.now()
    elapsed = end - start
    obj["phaseTime"] = elapsed.total_seconds()

    Export(filename, obj, **kwargs)
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
with Prof("General pings") as px:
    GeneralPings, GeneralPingInfo = FetchAndFormat()
    GeneralPings = GeneralPings.cache()

    # Windows gets some preferential breakdown treatment.
    WindowsPings = GeneralPings.filter(lambda p: p["OSName"] == "Windows")
    WindowsPings = WindowsPings.cache()

    MacPings = GeneralPings.filter(lambda p: p["OSName"] == "Darwin")
    MacPings = repartition(MacPings)

    LinuxPings = GeneralPings.filter(lambda p: p["OSName"] == "Linux")
    LinuxPings = repartition(LinuxPings)

# # ANALYSES ARE BELOW

# ## General statistics

# Results by operating system.
if "__share" not in GeneralPingInfo:
    GeneralPingInfo["__share"] = map_x_to_count(GeneralPings, "FxVersion")


def GetGeneralStatisticsForSubset(subset, windows_subset):
    OSShare = map_x_to_count(subset, "OSName")

    # Results by Windows version.
    WindowsShare = map_x_to_count(windows_subset, "OSVersion")

    # Top-level stats.
    VendorShare = map_x_to_count(subset, "vendorID")

    return {"os": OSShare, "windows": WindowsShare, "vendors": VendorShare}


def GetGeneralStatistics():
    obj = {}
    obj["devices"] = map_x_to_count(GeneralPings, "deviceID")
    obj["drivers"] = map_x_to_count(GeneralPings, "driverVersion")

    byFx = {}
    with Prof("general stats for all") as px:
        byFx["all"] = GetGeneralStatisticsForSubset(GeneralPings, WindowsPings)

    for key in GeneralPingInfo["__share"]:
        subset = GeneralPings.filter(lambda p: p["FxVersion"] == key)
        windows = subset.filter(lambda p: p["OSName"] == "Windows")
        subset = repartition(subset)
        windows = repartition(windows)
        with Prof("general stats for " + key) as px:
            byFx[key] = GetGeneralStatisticsForSubset(subset, windows)

    obj["byFx"] = byFx
    return obj


TimedExport(
    filename="general-statistics",
    callback=GetGeneralStatistics,
    pings=(GeneralPings, GeneralPingInfo),
)


# ## Device/driver search database


def GetDriverStatistics():
    obj = {}
    obj["deviceAndDriver"] = map_x_to_count(GeneralPings, "deviceAndDriver")
    return obj


TimedExport(
    filename="device-statistics",
    callback=GetDriverStatistics,
    save_history=False,  # No demand yet, and too much data.
    pings=(GeneralPings, GeneralPingInfo),
)


# ## TDR Statistics

#############################
# Perform the TDR analysis. #
#############################
def GetTDRStatistics():
    NumTDRReasons = 8

    def ping_has_tdr_for(p, reason):
        return p[DeviceResetReasonKey][reason] > 0

    # Specialized version of map_x_to_y, for TDRs. We cast to int because for
    # some reason the values Spark returns do not serialize with JSON.
    def map_reason_to_vendor(p, reason, destKey):
        return (int(reason), {p[destKey]: int(p[DeviceResetReasonKey][reason])})

    def map_vendor_to_reason(p, reason, destKey):
        return (p[destKey], {int(reason): int(p[DeviceResetReasonKey][reason])})

    # Filter out pings that do not have any TDR data. We expect this to be a huge reduction
    # in the sample set, and the resulting partition count gets way off. We repartition
    # immediately for performance.
    TDRSubset = WindowsPings.filter(
        lambda p: p.get(DeviceResetReasonKey, None) is not None
    )
    TDRSubset = TDRSubset.repartition(MaxPartitions)
    TDRSubset = TDRSubset.cache()

    # Aggregate the device reset data.
    TDRResults = TDRSubset.map(lambda p: p[DeviceResetReasonKey]).reduce(
        lambda x, y: x + y
    )

    # For each TDR reason, get a list tuple of (reason, vendor => resetCount). Then
    # we combine these into a single series.
    reason_to_vendor_tuples = None
    vendor_to_reason_tuples = None
    for reason in range(1, NumTDRReasons):
        subset = TDRSubset.filter(lambda p: ping_has_tdr_for(p, reason))
        subset = subset.cache()

        tuples = subset.map(lambda p: map_reason_to_vendor(p, reason, "vendorID"))
        reason_to_vendor_tuples = union_pipelines(reason_to_vendor_tuples, tuples)

        tuples = subset.map(lambda p: map_vendor_to_reason(p, reason, "vendorID"))
        vendor_to_reason_tuples = union_pipelines(vendor_to_reason_tuples, tuples)

    TDRReasonToVendor = reason_to_vendor_tuples.reduceByKey(combiner, MaxPartitions)
    TDRVendorToReason = vendor_to_reason_tuples.reduceByKey(combiner, MaxPartitions)

    return {
        "tdrPings": TDRSubset.count(),
        "results": [int(value) for value in TDRResults],
        "reasonToVendor": TDRReasonToVendor.collect(),
        "vendorToReason": TDRVendorToReason.collect(),
    }


# Write TDR statistics.
TimedExport(
    filename="tdr-statistics",
    callback=GetTDRStatistics,
    pings=(WindowsPings, GeneralPingInfo),
)

# ## System Statistics

##########################
# Get system statistics. #
##########################
CpuKey = "environment/system/cpu"
MemoryKey = "environment/system/memoryMB"


def GetBucketedMemory(pings):
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


def GetCpuFeatures(pings):
    cpuid_rdd = pings.map(lambda p: p.get(CpuKey, None))
    cpuid_rdd = cpuid_rdd.filter(lambda p: p is not None)
    cpuid_rdd = cpuid_rdd.map(lambda p: p.get("extensions", None))

    # Unfortunately, Firefox 39 had a bug where CPU features could be reported even
    # if they weren't present. To detect this we filter pings that have ARMv6 support
    # on x86/64.
    cpuid_rdd = cpuid_rdd.filter(lambda p: p is not None and "hasARMv6" not in p)
    cpuid_rdd = repartition(cpuid_rdd)

    # Count before we blow up the list.
    with Prof("cpu count for x86") as px:
        total = cpuid_rdd.count()

    cpuid_rdd = cpuid_rdd.flatMap(lambda p: [(ex, 1) for ex in p])
    with Prof("cpu features for x86") as px:
        feature_map = cpuid_rdd.countByKey()

    return {"total": total, "features": feature_map}


def GetSystemStatistics():
    def get_logical_cores(p):
        cpu = p.get(CpuKey, None)
        if cpu is None:
            return "unknown"
        return cpu.get("count", "unknown")

    with Prof("logical cores") as px:
        logical_cores = GeneralPings.map(lambda p: (get_logical_cores(p),)).countByKey()

    cpu_features = GetCpuFeatures(GeneralPings)

    with Prof("memory buckets") as px:
        memory = GetBucketedMemory(GeneralPings)

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

    with Prof("OS bit count") as px:
        os_bits = WindowsPings.map(lambda p: (get_os_bits(p),)).countByKey()

    return {
        "logical_cores": logical_cores,
        "x86": cpu_features,
        "memory": memory,
        "wow": os_bits,
    }


TimedExport(
    filename="system-statistics",
    callback=GetSystemStatistics,
    pings=(GeneralPings, GeneralPingInfo),
)

# ## Sanity Test Statistics

# Set up constants.
SANITY_TEST_PASSED = 0
SANITY_TEST_FAILED_RENDER = 1
SANITY_TEST_FAILED_VIDEO = 2
SANITY_TEST_CRASHED = 3
SANITY_TEST_TIMEDOUT = 4
SANITY_TEST_LAST_VALUE = 5
SANITY_TEST_REASON_FIRST_RUN = 0
SANITY_TEST_REASON_FIREFOX_CHANGED = 1
SANITY_TEST_REASON_DEVICE_CHANGED = 2
SANITY_TEST_REASON_DRIVER_CHANGED = 3
SANITY_TEST_REASON_LAST_VALUE = 4


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
def GetSanityTestsForSlice(sanity_test_pings):
    data = sanity_test_pings.filter(lambda p: get_sanity_test_result(p) is not None)

    # Aggregate the sanity test data.
    with Prof("initial map") as px:
        SanityTestResults = data.map(
            lambda p: (get_sanity_test_result(p),)
        ).countByKey()

    with Prof("share resolve") as px:
        os_share = map_x_to_count(data, "OSVersion")

    with Prof("ping_count") as px:
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

    with Prof("vendor resolve") as px:
        SanityTestByVendor = sanity_test_by_vendor.reduceByKey(combiner)
    with Prof("os resolve") as px:
        SanityTestByOS = sanity_test_by_os.reduceByKey(combiner)
    with Prof("device resolve") as px:
        SanityTestByDevice = sanity_test_by_device.reduceByKey(combiner)
    with Prof("driver resolve") as px:
        SanityTestByDriver = sanity_test_by_driver.reduceByKey(combiner)

    print(
        "Partitions: {0},{1},{2},{3}".format(
            SanityTestByVendor.getNumPartitions(),
            SanityTestByOS.getNumPartitions(),
            SanityTestByDevice.getNumPartitions(),
            SanityTestByDriver.getNumPartitions(),
        )
    )

    with Prof("vendor collect") as px:
        byVendor = SanityTestByVendor.collect()
    with Prof("os collect") as px:
        byOS = SanityTestByOS.collect()
    with Prof("device collect") as px:
        byDevice = SanityTestByDevice.collect()
    with Prof("driver collect") as px:
        byDriver = SanityTestByDriver.collect()

    return {
        "sanityTestPings": sanity_test_count,
        "totalPings": ping_count,
        "results": SanityTestResults,
        "byVendor": byVendor,
        "byOS": byOS,
        "byDevice": coalesce_to_n_items(byDevice, 10),
        "byDriver": coalesce_to_n_items(byDriver, 10),
        "windows": os_share,
    }


def GetSanityTests():
    obj = {}
    obj["windows"] = GetSanityTestsForSlice(WindowsPings)
    return obj


# Write Sanity Test statistics.
TimedExport(
    filename="sanity-test-statistics",
    callback=GetSanityTests,
    pings=(WindowsPings, GeneralPingInfo),
)

# ## Startup Crash Guard Statistics

STARTUP_OK = 0
STARTUP_ENV_CHANGED = 1
STARTUP_CRASHED = 2
STARTUP_ACCEL_DISABLED = 3


def GetStartupTests():
    startup_test_pings = GeneralPings.filter(
        lambda p: p.get(STARTUP_TEST_KEY, None) is not None
    )
    startup_test_pings = startup_test_pings.repartition(MaxPartitions)
    startup_test_pings = startup_test_pings.cache()

    StartupTestResults = startup_test_pings.map(lambda p: p[STARTUP_TEST_KEY]).reduce(
        lambda x, y: x + y
    )

    os_share = map_x_to_count(startup_test_pings, "OS")

    return {
        "startupTestPings": startup_test_pings.count(),
        "results": [int(i) for i in StartupTestResults],
        "windows": os_share,
    }


# Write startup test results.
TimedExport(
    filename="startup-test-statistics",
    callback=GetStartupTests,
    pings=(GeneralPings, GeneralPingInfo),
)


# ## Monitor Statistics


def get_monitor_count(p):
    monitors = p.get(MonitorsKey, None)
    try:
        return len(monitors)
    except:
        return 0


def get_monitor_res(p, i):
    width = p[MonitorsKey][i].get("screenWidth", 0)
    height = p[MonitorsKey][i].get("screenHeight", 0)
    if width == 0 or height == 0:
        return "Unknown"
    return "{0}x{1}".format(width, height)


def GetMonitorStatistics():
    def get_monitor_rdds_for_index(data, i):
        def get_refresh_rate(p):
            refreshRate = p[MonitorsKey][i].get("refreshRate", 0)
            return refreshRate if refreshRate > 1 else "Unknown"

        def get_resolution(p):
            return get_monitor_res(p, i)

        monitors_at_index = data.filter(lambda p: get_monitor_count(p) == monitor_count)
        monitors_at_index = repartition(monitors_at_index)
        refresh_rates = monitors_at_index.map(lambda p: (get_refresh_rate(p),))
        resolutions = monitors_at_index.map(lambda p: (get_resolution(p),))
        return refresh_rates, resolutions

    MonitorCounts = WindowsPings.map(lambda p: (get_monitor_count(p),)).countByKey()
    MonitorCounts.pop(0, None)

    refresh_rates = None
    resolutions = None
    for monitor_count in MonitorCounts:
        rate_subset, res_subset = get_monitor_rdds_for_index(
            WindowsPings, monitor_count - 1
        )
        refresh_rates = union_pipelines(refresh_rates, rate_subset)
        resolutions = union_pipelines(resolutions, res_subset)

    MonitorRefreshRates = refresh_rates.countByKey()
    MonitorResolutions = resolutions.countByKey()

    return {
        "counts": MonitorCounts,
        "refreshRates": MonitorRefreshRates,
        "resolutions": MonitorResolutions,
    }


TimedExport(
    filename="monitor-statistics",
    callback=GetMonitorStatistics,
    pings=(WindowsPings, GeneralPingInfo),
)

# ## Mac OS X Statistics

MacPings = GeneralPings.filter(lambda p: p["OSName"] == "Darwin")
MacPings = repartition(MacPings)


def GetMacStatistics():
    version_map = map_x_to_count(MacPings, "OSVersion")

    def get_scale(p):
        monitors = p.get(MonitorsKey, None)
        if not monitors:
            return "unknown"
        try:
            return monitors[0]["scale"]
        except:
            "unknown"

    scale_map = MacPings.map(lambda p: (get_scale(p),)).countByKey()

    def get_arch(p):
        arch = p.get(ArchKey, "unknown")
        if arch == "x86-64":
            return "64"
        if arch == "x86":
            return "32"
        return "unknown"

    arch_map = MacPings.map(lambda p: (get_arch(p),)).countByKey()

    return {"versions": version_map, "retina": scale_map, "arch": arch_map}


TimedExport(
    filename="mac-statistics",
    callback=GetMacStatistics,
    pings=(MacPings, GeneralPingInfo),
)


# ### Helpers for Compositor/Acceleration fields

# Build graphics feature statistics.
def get_compositor(p):
    compositor = p[FeaturesKey].get("compositor", "none")
    if compositor == "none":
        userPrefs = p.get(UserPrefsKey, None)
        if userPrefs is not None:
            omtc = userPrefs.get("layers.offmainthreadcomposition.enabled", True)
            if omtc != True:
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
    if d3d11.get("warp", False) == True:
        return "warp"
    return d3d11.get("version", "unknown")


def get_warp_status(p):
    if "blacklisted" not in p[FeaturesKey]["d3d11"]:
        return "unknown"
    if p[FeaturesKey]["d3d11"]["blacklisted"] == True:
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
    gpuProc = p[FeaturesKey].get("gpuProcess", None)
    if gpuProc is None or not gpuProc.get("status", None):
        return "none"
    return gpuProc.get("status")


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


WindowsFeatures = WindowsPings.filter(lambda p: p.get(FeaturesKey) is not None)
WindowsFeatures = WindowsFeatures.cache()

# We skip certain windows versions in detail lists since this phase is
# very expensive to compute.
ImportantWindowsVersions = ("6.1.0", "6.1.1", "6.2.0", "6.3.0", "10.0.0")


def GetWindowsFeatures():
    WindowsCompositorMap = WindowsFeatures.map(
        lambda p: (get_compositor(p),)
    ).countByKey()
    D3D11StatusMap = WindowsFeatures.map(lambda p: (get_d3d11_status(p),)).countByKey()
    D2DStatusMap = WindowsFeatures.map(get_d2d_status).countByKey()

    def get_content_backends(rdd):
        rdd = rdd.filter(lambda p: p[GfxKey].get("ContentBackend") is not None)
        rdd = rdd.map(lambda p: (p[GfxKey]["ContentBackend"],))
        return rdd.countByKey()

    content_backends = get_content_backends(WindowsFeatures)

    warp_pings = WindowsFeatures.filter(lambda p: get_d3d11_status(p) == "warp")
    warp_pings = repartition(warp_pings)
    WarpStatusMap = warp_pings.map(lambda p: (get_warp_status(p),)).countByKey()

    TextureSharingMap = (
        WindowsFeatures.filter(has_working_d3d11)
        .map(get_texture_sharing_status)
        .countByKey()
    )

    blacklisted_pings = WindowsFeatures.filter(
        lambda p: get_d3d11_status(p) == "blacklisted"
    )
    blacklisted_pings = repartition(blacklisted_pings)
    blacklisted_devices = map_x_to_count(blacklisted_pings, "deviceID")
    blacklisted_drivers = map_x_to_count(blacklisted_pings, "driverVersion")
    blacklisted_os = map_x_to_count(blacklisted_pings, "OSVersion")
    blacklisted_pings = None

    blocked_pings = WindowsFeatures.filter(lambda p: get_d3d11_status(p) == "blocked")
    blocked_pings = repartition(blocked_pings)
    blocked_vendors = map_x_to_count(blocked_pings, "vendorID")
    blocked_pings = None

    # Plugin models.
    def aggregate_plugin_models(rdd):
        rdd = rdd.filter(lambda p: p.get(PluginModelKey) is not None)
        rdd = rdd.map(lambda p: p.get(PluginModelKey))
        result = rdd.reduce(lambda x, y: x + y)
        return [int(count) for count in result]

    plugin_models = aggregate_plugin_models(WindowsFeatures)

    # Media decoder backends.
    def get_media_decoders(rdd):
        rdd = rdd.filter(lambda p: p.get(MediaDecoderKey, None) is not None)
        decoders = rdd.map(lambda p: p.get(MediaDecoderKey)).reduce(lambda x, y: x + y)
        return [int(i) for i in decoders]

    media_decoders = get_media_decoders(WindowsFeatures)

    def gpu_process_map(rdd):
        return rdd.map(lambda p: (gpu_process_status(p),)).countByKey()

    def advanced_layers_map(rdd):
        return rdd.map(lambda p: (advanced_layers_status(p),)).countByKey()

    # Now, build the same data except per version.
    feature_pings_by_os = map_x_to_count(WindowsFeatures, "OSVersion")
    WindowsFeaturesByVersion = {}
    for os_version in feature_pings_by_os:
        if os_version not in ImportantWindowsVersions:
            continue
        subset = WindowsFeatures.filter(lambda p: p["OSVersion"] == os_version)
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
        except:
            pass
        finally:
            # Free resources.
            warp_pings = None
            subset = None
        WindowsFeaturesByVersion[os_version] = results

    return {
        "all": {
            "compositors": WindowsCompositorMap,
            "content_backends": content_backends,
            "d3d11": D3D11StatusMap,
            "d2d": D2DStatusMap,
            "textureSharing": TextureSharingMap,
            "warp": WarpStatusMap,
            "plugin_models": plugin_models,
            "media_decoders": media_decoders,
            "gpu_process": gpu_process_map(WindowsFeatures),
            "advanced_layers": advanced_layers_map(WindowsFeatures),
        },
        "byVersion": WindowsFeaturesByVersion,
        "d3d11_blacklist": {
            "devices": blacklisted_devices,
            "drivers": blacklisted_drivers,
            "os": blacklisted_os,
        },
        "d3d11_blocked": {"vendors": blocked_vendors},
    }


TimedExport(
    filename="windows-features",
    callback=GetWindowsFeatures,
    pings=(WindowsFeatures, GeneralPingInfo),
)

WindowsFeatures = None

# ## Linux

LinuxPings = GeneralPings.filter(lambda p: p["OSName"] == "Linux")
LinuxPings = repartition(LinuxPings)


def GetLinuxStatistics():
    pings = LinuxPings.filter(lambda p: p["driverVendor"] is not None)
    driver_vendor_map = map_x_to_count(pings, "driverVendor")

    pings = LinuxPings.filter(lambda p: p.get(FeaturesKey) is not None)
    compositor_map = pings.map(lambda p: (get_compositor(p),)).countByKey()

    return {"driverVendors": driver_vendor_map, "compositors": compositor_map}


TimedExport(
    filename="linux-statistics",
    callback=GetLinuxStatistics,
    pings=(LinuxPings, GeneralPingInfo),
)


# ## WebGL Statistics
# _Note, this depends on running the "Helpers for Compositor/Acceleration fields" a few blocks above._


def GetGLStatistics():
    webgl_status_rdd = GeneralPings.filter(
        lambda p: p.get(WebGLFailureKey, None) is not None
    )
    webgl_status_rdd = webgl_status_rdd.map(lambda p: p[WebGLFailureKey])
    webgl_status_map = webgl_status_rdd.reduce(combiner)
    webgl_accl_status_rdd = GeneralPings.filter(
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


def WebGLStatisticsForKey(key):
    histogram_pings = GeneralPings.filter(lambda p: p.get(key) is not None)
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


def GetWebGLStatistics():
    return {
        "webgl1": WebGLStatisticsForKey(WebGLSuccessKey),
        "webgl2": WebGLStatisticsForKey(WebGL2SuccessKey),
        "general": GetGLStatistics(),
    }


TimedExport(
    filename="webgl-statistics",
    callback=GetWebGLStatistics,
    pings=(GeneralPings, GeneralPingInfo),
)


def GetLayersStatus():
    d3d11_status_rdd = GeneralPings.filter(
        lambda p: p.get(LayersD3D11FailureKey, None) is not None
    )
    d3d11_status_rdd = d3d11_status_rdd.map(lambda p: p[LayersD3D11FailureKey])
    d3d11_status_map = d3d11_status_rdd.reduce(combiner)
    ogl_status_rdd = GeneralPings.filter(
        lambda p: p.get(LayersOGLFailureKey, None) is not None
    )
    ogl_status_rdd = ogl_status_rdd.map(lambda p: p[LayersOGLFailureKey])
    ogl_status_map = ogl_status_rdd.reduce(combiner)
    print(d3d11_status_map)
    print(ogl_status_map)
    return {"layers": {"d3d11": d3d11_status_map, "opengl": ogl_status_map}}


def GetLayersStatistics():
    return {"general": GetLayersStatus()}


TimedExport(
    filename="layers-failureid-statistics",
    callback=GetLayersStatistics,
    pings=(GeneralPings, GeneralPingInfo),
)

EndTime = datetime.datetime.now()
TotalElapsed = (EndTime - StartTime).total_seconds()

print("Total time: {0}".format(TotalElapsed))
