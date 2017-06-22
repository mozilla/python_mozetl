"""
This job was originally located at [1].

[1] https://git.io/vHRWO
"""
import ujson as json
import datetime as dt
import os.path
import boto3
import botocore
import requests
import logging
import moztelemetry.standards as moz_std
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

# Reasons why the data for a client can be discarded.
REASON_INACTIVE = "inactive"
REASON_BROKEN_DATA = "broken"
# These fields have a fixed set of values and we need to report all of them.
EXCLUSION_LIST = ["has_flash", "browser_arch", "os_arch"]

S3_DATA_PATH = "game-hardware-survey/data/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_json(uri):
    """ Perform an HTTP GET on the given uri, return the results as json.
    If there is an error fetching the data, raise an exception.

    Args:
        uri: the string URI to fetch.

    Returns:
        A JSON object with the response.
    """
    data = requests.get(uri)
    # Raise an exception if the fetch failed.
    data.raise_for_status()
    return data.json()


def get_OS_arch(browser_arch, os_name, is_wow64):
    """ Infers the OS arch from environment data.

    Args:
        browser_arch: the browser architecture string (either "x86" or "x86-64").
        os_name: the operating system name.
        is_wow64: on Windows, indicates if the browser process is running under WOW64.

    Returns:
        'x86' if the underlying OS is 32bit, 'x86-64' if it's a 64bit OS.
    """

    is_64bit_browser = browser_arch == 'x86-64'
    # If it's a 64bit browser build, then we're on a 64bit system.
    if is_64bit_browser:
        return 'x86-64'

    is_windows = os_name == 'Windows_NT'
    # If we're on Windows, with a 32bit browser build, and |isWow64 = true|,
    # then we're on a 64 bit system.
    if is_windows and is_wow64:
        return 'x86-64'

    # Otherwise we're probably on a 32 bit system.
    return 'x86'


def vendor_name_from_id(id):
    """ Get the string name matching the provided vendor id.

    Args:
        id: A string containing the vendor id.

    Returns:
        A string containing the vendor name or "(Other <ID>)" if
        unknown.
    """

    # TODO: We need to make this an external resource for easier
    # future updates.
    vendor_map = {
        '0x1013': 'Cirrus Logic',
        '0x1002': 'AMD',
        '0x8086': 'Intel',
        '0x5333': 'S3 Graphics',
        '0x1039': 'SIS',
        '0x1106': 'VIA',
        '0x10de': 'NVIDIA',
        '0x102b': 'Matrox',
        '0x15ad': 'VMWare',
        '0x80ee': 'Oracle VirtualBox',
        '0x1414': 'Microsoft Basic',
    }

    return vendor_map.get(id, "Other")


def get_device_family_chipset(vendor_id, device_id, device_map):
    """ Get the family and chipset strings given the vendor and device ids.

    Args:
        vendor_id: a string representing the vendor id (e.g. '0xabcd').
        device_id: a string representing the device id (e.g. '0xbcde').

    Returns:
        A string in the format "Device Family Name-Chipset Name".
    """
    if not vendor_id in device_map:
        return "Unknown"

    if not device_id in device_map[vendor_id]:
        return "Unknown"

    return "-".join(device_map[vendor_id][device_id])


def invert_device_map(m):
    """ Inverts a GPU device map fetched from the jrmuizel's Github repo.
    The layout of the fetched GPU map layout is:
        Vendor ID -> Device Family -> Chipset -> [Device IDs]
    We should convert it to:
        Vendor ID -> Device ID -> [Device Family, Chipset]
    """
    device_id_map = {}
    for vendor, u in m.iteritems():
        device_id_map['0x' + vendor] = {}
        for family, v in u.iteritems():
            for chipset, ids in v.iteritems():
                device_id_map['0x' +
                              vendor].update({('0x' +
                                               gfx_id): [family, chipset] for gfx_id in ids})
    return device_id_map


def build_device_map():
    """ This function builds a dictionary that will help us mapping vendor/device ids to a
    human readable device family and chipset name."""

    intel_raw = fetch_json(
        "https://github.com/jrmuizel/gpu-db/raw/master/intel.json")
    nvidia_raw = fetch_json(
        "https://github.com/jrmuizel/gpu-db/raw/master/nvidia.json")
    amd_raw = fetch_json(
        "https://github.com/jrmuizel/gpu-db/raw/master/amd.json")

    device_map = {}
    device_map.update(invert_device_map(intel_raw))
    device_map.update(invert_device_map(nvidia_raw))
    device_map.update(invert_device_map(amd_raw))

    return device_map


def get_valid_client_record(r, data_index):
    """ Check if the referenced record is sane or contains partial/broken data.

    Args:
        r: The client entry in the longitudinal dataset.
        dat_index: The index of the sample within the client record.

    Returns:
        An object containing the client hardware data or REASON_BROKEN_DATA if the
        data is invalid.
    """

    gfx_adapters = r["system_gfx"][data_index]["adapters"]
    monitors = r["system_gfx"][data_index]["monitors"]

    # We should make sure to have GFX adapter. If we don't, discard this
    # record.
    if not gfx_adapters or not gfx_adapters[0]:
        return REASON_BROKEN_DATA

    # Due to bug 1175005, Firefox on Linux isn't sending the screen resolution data.
    # Don't discard the rest of the ping for that: just set the resolution to 0 if
    # unavailable. See bug 1324014 for context.
    screen_width = 0
    screen_height = 0
    if monitors and monitors[0]:
        screen_width = monitors[0]["screen_width"]
        screen_height = monitors[0]["screen_height"]

    # Non Windows OS do not have that property.
    is_wow64 = r["system"][data_index]["is_wow64"] == True

    # At this point, we should have filtered out all the weirdness. Fetch
    # the data we need.
    data = {
        'browser_arch': r["build"][data_index]["architecture"],
        'os_name': r["system_os"][data_index]["name"],
        'os_version': r["system_os"][data_index]["version"],
        'memory_mb': r["system"][data_index]["memory_mb"],
        'is_wow64': is_wow64,
        'gfx0_vendor_id': gfx_adapters[0]["vendor_id"],
        'gfx0_device_id': gfx_adapters[0]["device_id"],
        'screen_width': screen_width,
        'screen_height': screen_height,
        'cpu_cores': r["system_cpu"][data_index]["cores"],
        'cpu_vendor': r["system_cpu"][data_index]["vendor"],
        'cpu_speed': r["system_cpu"][data_index]["speed_mhz"],
        'has_flash': False
    }

    # The plugins data can still be null or empty, account for that.
    plugins = r["active_plugins"][data_index] if r["active_plugins"] else None
    if plugins:
        data['has_flash'] = any(
            [True for p in plugins if p['name'] == 'Shockwave Flash'])

    return REASON_BROKEN_DATA if None in data.values() else data


def get_latest_valid_per_client(entry, time_start, time_end):
    """ Get the most recently submitted ping for a client within the given timeframe.

    Then use this index to look up the data from the other columns (we can assume that the sizes
    of these arrays match, otherwise the longitudinal dataset is broken).
    Once we have the data, we make sure it's valid and return it.

    Args:
        entry: The record containing all the data for a single client.
        time_start: The beginning of the reference timeframe.
        time_end: The end of the reference timeframe.

    Returns:
        An object containing the valid hardware data for the client or a string
        describing why the data is discarded. Either REASON_INACTIVE, if the client didn't
        submit a ping within the desired timeframe, or REASON_BROKEN_DATA if it send
        broken data.

    Raises:
        ValueError: if the columns within the record have mismatching lengths. This
        means the longitudinal dataset is corrupted.
    """
    latest_entry = None
    for index, pkt_date in enumerate(entry["submission_date"]):
        sub_date = dt.datetime.strptime(
            pkt_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()

        # The data is in descending order, the most recent ping comes first.
        # The first item less or equal than the time_end date is our thing.
        if sub_date >= time_start.date() and sub_date <= time_end.date():
            latest_entry = index
            break

        # Ok, we went too far, we're not really interested in the data
        # outside of [time_start, time_end]. Since records are ordered,
        # we can actually skip this.
        if sub_date < time_start.date():
            break

    # This client wasn't active in the reference timeframe, just map it to no
    # data.
    if latest_entry is None:
        return REASON_INACTIVE

    # Some clients might be missing entire sections. If that's
    # a basic section, skip them, we don't want partial data.
    # Don't enforce the presence of "active_plugins", as it's not included
    # by the pipeline if no plugin is reported by Firefox (see bug 1333806).
    desired_sections = [
        "build", "system_os", "submission_date", "system",
        "system_gfx", "system_cpu"
    ]

    for field in desired_sections:
        if entry[field] is None:
            return REASON_BROKEN_DATA

        # All arrays in the longitudinal dataset should have the same length, for a
        # single client. If that's not the case, if our index is not there,
        # throw.
        if entry[field][latest_entry] is None:
            raise ValueError("Null " + field + " index: " + str(latest_entry))

    return get_valid_client_record(entry, latest_entry)


def prepare_data(p, device_map):
    """ This function prepares the data for further analyses (e.g. unit conversion,
    vendor id to string, ...). """

    cpu_speed = round(p['cpu_speed'] / 1000.0, 1)
    return {
        'browser_arch': p['browser_arch'],
        'cpu_cores': p['cpu_cores'],
        'cpu_cores_speed': str(p['cpu_cores']) + '_' + str(cpu_speed),
        'cpu_vendor': p['cpu_vendor'],
        'cpu_speed': cpu_speed,
        'gfx0_vendor_name': vendor_name_from_id(p['gfx0_vendor_id']),
        'gfx0_model': get_device_family_chipset(p['gfx0_vendor_id'], p['gfx0_device_id'], device_map),
        'resolution': str(p['screen_width']) + 'x' + str(p['screen_height']),
        'memory_gb': int(round(p['memory_mb'] / 1024.0)),
        'os': p['os_name'] + '-' + p['os_version'],
        'os_arch': get_OS_arch(p['browser_arch'], p['os_name'], p['is_wow64']),
        'has_flash': p['has_flash']
    }


def aggregate_data(processed_data):
    def seq(acc, v):
        # The dimensions over which we want to aggregate the different values.
        keys_to_aggregate = [
            'browser_arch',
            'cpu_cores',
            'cpu_cores_speed',
            'cpu_vendor',
            'cpu_speed',
            'gfx0_vendor_name',
            'gfx0_model',
            'resolution',
            'memory_gb',
            'os',
            'os_arch',
            'has_flash'
        ]

        for key_name in keys_to_aggregate:
            # We want to know how many users have a particular configuration (e.g. using a particular
            # cpu vendor). For each dimension of interest, build a key as (hw, value) and count its
            # occurrences among the user base.
            acc_key = (key_name, v[key_name])
            acc[acc_key] = acc.get(acc_key, 0) + 1

        return acc

    def cmb(v1, v2):
        # Combine the counts from the two partial dictionaries. Hacky?
        return {k: v1.get(k, 0) + v2.get(k, 0) for k in set(v1) | set(v2)}

    return processed_data.aggregate({}, seq, cmb)


def collapse_buckets(aggregated_data, count_threshold):
    """ Collapse uncommon configurations in generic groups to preserve privacy.

    This takes the dictionary of aggregated results from |aggregate_data| and collapses
    entries with a value less than |count_threshold| in a generic bucket.

    Args:
        aggregated_data: The object containing aggregated data.
        count_threhold: Groups (or "configurations") containing less than this value
        are collapsed in a generic bucket.
    """

    collapsed_groups = {}
    for k, v in aggregated_data.iteritems():
        key_type = k[0]

        # If the resolution is 0x0 (see bug 1324014), put that into the "Other"
        # bucket.
        if key_type == 'resolution' and k[1] == '0x0':
            other_key = ('resolution', 'Other')
            collapsed_groups[other_key] = collapsed_groups.get(
                other_key, 0) + v
            continue

        # Don't clump this group into the "Other" bucket if it has enough
        # users it in.
        if v > count_threshold or key_type in EXCLUSION_LIST:
            collapsed_groups[k] = v
            continue

        # If we're here, it means that the key has not enough elements.
        # Fall through the next cases and try to group things together.
        new_group_key = 'Other'

        # Let's try to group similar resolutions together.
        if key_type == 'resolution':
            # Extract the resolution.
            [w, h] = k[1].split('x')
            # Round to the nearest hundred.
            w = int(round(int(w), -2))
            h = int(round(int(h), -2))
            # Build up a new key.
            new_group_key = '~' + str(w) + 'x' + str(h)
        elif key_type == 'os':
            [os, ver] = k[1].split('-', 1)
            new_group_key = os + '-' + 'Other'

        # We don't have enough data for this particular group/configuration.
        # Aggregate it with the data in the "Other" bucket
        other_key = (k[0], new_group_key)
        collapsed_groups[other_key] = collapsed_groups.get(other_key, 0) + v

    # The previous grouping might have created additional groups. Let's check
    # again.
    final_groups = {}
    for k, v in collapsed_groups.iteritems():
        # Don't clump this group into the "Other" bucket if it has enough
        # users it in.
        if (v > count_threshold and k[1] != 'Other') or k[0] in EXCLUSION_LIST:
            final_groups[k] = v
            continue

        # We don't have enough data for this particular group/configuration.
        # Aggregate it with the data in the "Other" bucket
        other_key = (k[0], 'Other')
        final_groups[other_key] = final_groups.get(other_key, 0) + v

    return final_groups


def finalize_data(data, sample_count, broken_ratio,
                  inactive_ratio, report_date):
    """ Finalize the aggregated data.

    Translate raw sample numbers to percentages and add the date for the reported
    week along with the percentage of discarded samples due to broken data.

    Rename the keys to more human friendly names.

    Args:
        data: Data in aggregated form.
        sample_count: The number of samples the aggregates where generated from.
        broken_ratio: The percentage of samples discarded due to broken data.
        inactive_ratio: The percentage of samples discarded due to the client not sending data.
        report_date: The starting day for the reported week.

    Returns:
        An object containing the reported hardware statistics.
    """

    denom = float(sample_count)

    aggregated_percentages = {
        'date': report_date.isoformat(),
        'broken': broken_ratio,
        'inactive': inactive_ratio,
    }

    keys_translation = {
        'browser_arch': 'browserArch_',
        'cpu_cores': 'cpuCores_',
        'cpu_cores_speed': 'cpuCoresSpeed_',
        'cpu_vendor': 'cpuVendor_',
        'cpu_speed': 'cpuSpeed_',
        'gfx0_vendor_name': 'gpuVendor_',
        'gfx0_model': 'gpuModel_',
        'resolution': 'resolution_',
        'memory_gb': 'ram_',
        'os': 'osName_',
        'os_arch': 'osArch_',
        'has_flash': 'hasFlash_'
    }

    # Compute the percentages from the raw numbers.
    for k, v in data.iteritems():
        # The old key is a tuple (key, value). We translate the key part and concatenate the
        # value as a string.
        new_key = keys_translation[k[0]] + unicode(k[1])
        aggregated_percentages[new_key] = v / denom

    return aggregated_percentages


def validate_finalized_data(data):
    """ Validate the aggregated and finalized data.

    This checks that the aggregated hardware data object contains all the expectd keys
    and that they sum up roughly 1.0.

    When a problem is found a message is printed to make debugging easier.

    Args:
        data: Data in aggregated form.

    Returns:
        True if the data validates correctly, false otherwise.
    """
    keys_accumulator = {
        'browserArch': 0.0,
        'cpuCores': 0.0,
        'cpuCoresSpeed': 0.0,
        'cpuVendor': 0.0,
        'cpuSpeed': 0.0,
        'gpuVendor': 0.0,
        'gpuModel': 0.0,
        'resolution': 0.0,
        'ram': 0.0,
        'osName': 0.0,
        'osArch': 0.0,
        'hasFlash': 0.0
    }

    # We expect to have at least a key in |data| whose name begins with one
    # of the keys in |keys_accumulator|. Iterate through the keys in |data|
    # and accumulate their values in the accumulator.
    for key, value in data.iteritems():
        if key in ["inactive", "broken", "date"]:
            continue

        # Get the name of the property to look it up in the accumulator.
        property_name = key.split('_')[0]
        if property_name not in keys_accumulator:
            logger.warning("Cannot find {} in |keys_accumulator|".format(property_name))
            return False

        keys_accumulator[property_name] += value

    # Make sure all the properties add up to 1.0 (or close enough).
    for key, value in keys_accumulator.iteritems():
        if abs(1.0 - value) > 0.05:
            logger.warning(
                "{} values do not add up to 1.0. Their sum is {}.".format(
                    key, value))
            return False

    return True


def get_file_name(suffix=""):
    return "hwsurvey-weekly" + suffix + ".json"


def serialize_results(date_to_json):
    logger.info("Serializing results locally...")
    for file_name, aggregated_data in date_to_json.items():
        # This either appends to an existing file, or creates a new one.
        if os.path.exists(file_name):
            logger.info("{} exists, we will overwrite it.".format(file_name))

        # Our aggregated data is a JSON object.
        json_entry = json.dumps(aggregated_data)

        with open(file_name, "w") as json_file:
            json_file.write("[" + json_entry.encode('utf8') + "]\n")


def fetch_previous_state(s3_source_file_name, local_file_name, bucket):
    """
    This function fetches the previous state from S3's bucket and stores it locally.

    Args:
        s3_source_file_name: The name of the file on S3.
        local_file_name: The name of the file to save to, locally.
    """

    # Fetch the previous state.
    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)
    key_path = S3_DATA_PATH + s3_source_file_name

    try:
        transfer.download_file(bucket, key_path, local_file_name)
    except botocore.exceptions.ClientError as e:
        # If the file wasn't there, that's ok. Otherwise, abort!
        if e.response['Error']['Code'] != "404":
            raise e
        else:
            logger.exception("Did not find an existing file at '{}'".format(key_path))


def store_new_state(source_file_name, s3_dest_file_name, bucket):
    """
    Store the new state file to S3.

    Args:
        source_file_name: The name of the local source file.
        s3_dest_file_name: The name of the destination file on S3.
    """

    client = boto3.client('s3', 'us-west-2')
    transfer = boto3.s3.transfer.S3Transfer(client)

    # Update the state in the analysis bucket.
    key_path = S3_DATA_PATH + s3_dest_file_name
    transfer.upload_file(source_file_name, bucket, key_path)


def generate_report(start_date, end_date, spark):
    """ Generates the hardware survey dataset for the reference timeframe.

    If the timeframe is longer than a week, split it in in weekly chunks
    and process each chunk individually (eases backfilling).

    The report for each week is saved in a local JSON file.

    Args:
        start_date: The date from which we start generating the report. If None,
           the report starts from the beginning of the past week (Sunday).
        end_date: The date the marks the end of the reporting period. This only
           makes sense if a |start_date| was provided. If None, this defaults
           to the end of the past week (Saturday).
    """

    # If no start_date was provided, generate a report for the past complete
    # week.

    last_week = moz_std.get_last_week_range()
    date_range = (
        moz_std.snap_to_beginning_of_week(
            start_date, "Sunday") if start_date is not None else last_week[0],
        end_date if (
            end_date is not None and start_date is not None) else last_week[1]
    )

    # Connect to the longitudinal dataset.
    sqlQuery = """
               SELECT 
                  build, 
                  client_id, 
                  active_plugins, 
                  system_os, 
                  submission_date, 
                  system, 
                  system_gfx, 
                  system_cpu, 
                  normalized_channel  
               FROM 
                  longitudinal
               LIMIT 1000
               """

    frame = spark.sql(sqlQuery).where("normalized_channel = 'release'").where("build is not null and build[0].application_name = 'Firefox'")

    # The number of all the fetched records (including inactive and broken).
    records_count = frame.count()
    logger.info("Total record count: {}".format(records_count))

    # Split the submission period in chunks, so we don't run out of resources while aggregating if
    # we want to backfill.
    chunk_start = date_range[0]
    chunk_end = None
    # Stores all hardware reports in json by date
    date_to_json = {}

    while chunk_start < date_range[1]:
        chunk_end = chunk_start + dt.timedelta(days=6)

        # Fetch the data we need.
        data = frame.rdd.map(
            lambda r: get_latest_valid_per_client(
                r, chunk_start, chunk_end))

        # Filter out broken data.
        filtered_data = data.filter(
            lambda r: r not in [
                REASON_BROKEN_DATA,
                REASON_INACTIVE])

        # Count the broken records and inactive records.
        discarded = data.filter(
            lambda r: r in [
                REASON_BROKEN_DATA,
                REASON_INACTIVE]).countByValue()

        broken_count = discarded[REASON_BROKEN_DATA]
        inactive_count = discarded[REASON_INACTIVE]
        broken_ratio = broken_count / float(records_count)
        inactive_ratio = inactive_count / float(records_count)
        logger.info("Broken pings ratio: {}; Inactive clients ratio: {}".format(broken_ratio, inactive_ratio))

        # If we're not seeing sane values for the broken or inactive ratios,
        # bail out early on. There's no point in aggregating.
        if broken_ratio >= 0.9 or inactive_ratio >= 0.9:
            raise Exception(
                "Unexpected ratio of broken pings or inactive clients. Broken ratio: " + str(broken_ratio) + 
                ", inactive ratio: " + str(inactive_ratio))

        # Process the data, transforming it in the form we desire.
        device_map = build_device_map()
        processed_data = filtered_data.map(lambda d: prepare_data(d, device_map))
        
        logger.info("Aggregating entries...")
        aggregated_pings = aggregate_data(processed_data)

        # Get the sample count, we need it to compute the percentages instead of raw numbers.
        # Since we're getting only the newest ping for each client, we can simply count the
        # number of pings. THIS MAY NOT BE CONSTANT ACROSS WEEKS!
        valid_records_count = filtered_data.count()

        # Collapse together groups that count less than 1% of our samples.
        threshold_to_collapse = int(valid_records_count * 0.01)

        logger.info("Collapsing smaller groups into the other bucket (threshold {th})".format(th=threshold_to_collapse))
        collapsed_aggregates = collapse_buckets(
            aggregated_pings, threshold_to_collapse)

        logger.info("Post-processing raw values...")

        processed_aggregates = finalize_data(collapsed_aggregates,
                                             valid_records_count,
                                             broken_ratio,
                                             inactive_ratio,
                                             chunk_start)

        if not validate_finalized_data(processed_aggregates):
            raise Exception("The aggregates failed to validate.")

        # Write the week start/end in the filename.
        suffix = "-" + chunk_start.strftime("%Y%d%m") + \
                 "-" + chunk_end.strftime("%Y%d%m")
        file_name = get_file_name(suffix)

        date_to_json[file_name] = processed_aggregates

        # Move on to the next chunk, just add one day the end of the last
        # chunk.
        chunk_start = chunk_end + dt.timedelta(days=1)

    return date_to_json
