import ujson as json
import datetime as dt
import os.path
import boto3
import botocore
import requests
import json
import ast
import moztelemetry.standards as moz_std
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from datetime import datetime, timedelta
from mozetl.hardware_report.summarize_json import *

def test_run_tests():
    # Does |get_OS_arch| work as expected?
    assert get_OS_arch("x86", "Windows_NT", False) == "x86", "get_OS_arch should report an 'x86' for an x86 browser with no is_wow64."
    assert get_OS_arch("x86", "Windows_NT", True) == "x86-64", "get_OS_arch should report an 'x86-64' for an x86 browser, on Windows, using Wow64."
    assert get_OS_arch("x86", "Darwin", True) == "x86", "get_OS_arch should report an 'x86' for an x86 browser on non Windows platforms."
    assert get_OS_arch("x86-64", "Darwin", True) == "x86-64", "get_OS_arch should report an 'x86-64' for an x86-64 browser on non Windows platforms."
    assert get_OS_arch("x86-64", "Windows_NT", False) == "x86-64", "get_OS_arch should report an 'x86-64' for an x86-64 browser on Windows platforms."
        
    # Does |vendor_name_from_id| behave correctly?
    assert vendor_name_from_id("0x1013") == "Cirrus Logic", "vendor_name_from_id must report the correct vendor name for a known vendor id."
    assert vendor_name_from_id("0xfeee") == "Other", "vendor_name_from_id must report 'Other' for an unknown vendor id."
   
    # Make sure |invert_device_map| works as expected.
    device_data = {"feee": {"family":{"chipset":["d1d1", "d2d2"]}}}
    inverted_device_data = invert_device_map(device_data)
    assert "0xfeee" in inverted_device_data, "The vendor id must be prefixed with '0x' and be at the root of the map."
    assert len(inverted_device_data["0xfeee"].keys()) == 2, "There must be two devices for the '0xfeee' vendor."
    assert all(device_id in inverted_device_data["0xfeee"] for device_id in ("0xd1d1", "0xd2d2")), "The '0xfeee' vendor must contain the expected devices."
    assert all(d in inverted_device_data["0xfeee"]["0xd1d1"] for d in ("family", "chipset")), "The family and chipset data must be reported in the device section."
    
    # Let's test |get_device_family_chipset|.
    global device_map
    device_map = inverted_device_data
    assert get_device_family_chipset("0xfeee", "0xd1d1", device_map) == "family-chipset", "The family and chipset info must be returned as '<family>-<chipset>' for known devices."
    assert get_device_family_chipset("0xfeee", "0xdeee", device_map) == "Unknown", "Unknown devices must be reported as 'Unknown'."
    assert get_device_family_chipset("0xfeeb", "0xdeee", device_map) == "Unknown", "Unknown families must be reported as 'Unknown'."


def test_prepare_data():
    data = {
        'browser_arch': 'x86',
        'os_name': 'Windows_NT',
        'os_version': '6.1',
        'memory_mb': 4286,
        'is_wow64': False,
        'gfx0_vendor_id': '0xfeee',
        'gfx0_device_id': '0xd1d1',
        'screen_width': 1280,
        'screen_height': 1024,
        'cpu_cores': 2,
        'cpu_vendor': 'SomeCpuVendor',
        'cpu_speed': 3261,
        'has_flash': True
    }
    
    prepared_data = prepare_data(data, device_map)
    assert prepared_data["browser_arch"] == "x86",\
           "The browser architecture must be correct."
    assert prepared_data["cpu_cores"] == 2,\
           "The number of CPU cores must be correct."
    assert prepared_data["cpu_speed"] == 3.3,\
           "The CPU speed must be in GHz and correctly rounded to 1 decimal."
    assert prepared_data["cpu_vendor"] == "SomeCpuVendor",\
           "The CPU vendor must be correct."
    assert prepared_data["cpu_cores_speed"] == "2_3.3",\
           "The CPU cores and speed must be correctly merged together."
    assert prepared_data["gfx0_vendor_name"] == "Other",\
           "The GPU vendor name must be correctly converted from the vendor id."
    assert prepared_data["gfx0_model"] == "family-chipset",\
           "The GPU family and chipset must be correctly derived from the vendor and device ids."
    assert prepared_data["resolution"] == "1280x1024",\
           "The screen resolution must be correctly concatenated."
    assert prepared_data["memory_gb"] == 4,\
           "The RAM memory must be converted to GB."
    assert prepared_data["os"] == "Windows_NT-6.1",\
           "The OS string must contain the OS name and version."
    assert prepared_data["os_arch"] == "x86",\
           "The OS architecture must be correctly inferred."
    assert prepared_data["has_flash"] == True,\
           "The flash plugin must be correctly reported."

def test_aggregate_data():
    raw_data = [
        {
            'browser_arch': 'x86',
            'os_name': 'Windows_NT',
            'os_version': '6.1',
            'memory_mb': 4286,
            'is_wow64': False,
            'gfx0_vendor_id': '0xfeee',
            'gfx0_device_id': '0xd1d1',
            'screen_width': 1280,
            'screen_height': 1024,
            'cpu_cores': 2,
            'cpu_vendor': 'SomeCpuVendor',
            'cpu_speed': 3261,
            'has_flash': True
        },
        {
            'browser_arch': 'x86',
            'os_name': 'Windows_NT',
            'os_version': '6.1',
            'memory_mb': 4286,
            'is_wow64': False,
            'gfx0_vendor_id': '0xfeee',
            'gfx0_device_id': '0xd1d1',
            'screen_width': 1280,
            'screen_height': 1024,
            'cpu_cores': 2,
            'cpu_vendor': 'SomeCpuVendor',
            'cpu_speed': 3261,
            'has_flash': True
        },
        {
            'browser_arch': 'x86-64',
            'os_name': 'Darwin',
            'os_version': '15.3',
            'memory_mb': 8322,
            'is_wow64': False,
            'gfx0_vendor_id': '0xfeee',
            'gfx0_device_id': '0xd1d2',
            'screen_width': 1320,
            'screen_height': 798,
            'cpu_cores': 4,
            'cpu_vendor': 'SomeCpuVendor',
            'cpu_speed': 4211,
            'has_flash': True
        },
    ]

    spark = (SparkSession
         .builder
         .appName("hardware_report_dashboard")
         .getOrCreate())

    # Create an rdd with the pepared data, then aggregate.
    data_rdd = spark.sparkContext.parallelize([prepare_data(d, device_map) for d in raw_data])
    agg_data = aggregate_data(data_rdd)
    
    assert agg_data[('os_arch', 'x86')] == 2,\
           "Two 'x86' OS architectures must be reported."
    assert agg_data[('os_arch', 'x86-64')] == 1,\
           "One 'x86-64' OS architecture must be reported."
    assert agg_data[('has_flash', True)] == 3,\
           "All the entries had the flash plugin, so this must be 3."

def test_collapse_buckets():
    agg_data = {
        ("has_flash", True): 72,
        ("has_flash", False): 2,
        ("resolution", "1280x1024"): 50,
        ("resolution", "2560x1440"): 8,
        ("resolution", "2563x1440"): 8,
        ("resolution", "640x480"): 6,
        ("resolution", "640x472"): 2,
        ("resolution", "0x0"): 15, # Invalid data.
        ("os", "Windows_NT-6.11"): 34,
        ("os", "Windows_NT-5.10"): 8,
        ("os", "Windows_NT-4"): 8,
        ("os", "FunkyOS-4"): 1,
        ("os", "Darwin-11.0"): 22,
        ("os", "Darwin-1.0"): 1,
    }
    
    threshold = 10
    collapsed_data = collapse_buckets(agg_data, threshold)
    
    # Test that keys with values above the threshold are kept.
    assert ("os", "Darwin-11.0") in collapsed_data,\
           "Keys with enough elements must not be collapsed."
    assert ("has_flash", True) in collapsed_data,\
           "Keys with enough elements must not be collapsed."
    assert ("resolution", "1280x1024") in collapsed_data,\
           "Keys with enough elements must not be collapsed."
    assert ("os", "Windows_NT-6.11") in collapsed_data,\
           "Keys with enough elements must not be collapsed."
        
    # Test resolution collapsing.
    assert ("resolution", "~2600x1400") in collapsed_data,\
           "Collapsed resolutions with enough elements must be reported, prefixed with ~."
    assert ("resolution", "Other") in collapsed_data,\
           "Collapsed resolutions with not enough elements must be reported as 'Other'."
    assert collapsed_data[("resolution", "Other")] == 23,\
           "The 640x* and the 0x0 resolution must be reported in the 'Other' bucket."
        
    # Test that whitelisted keys are not collapsed.
    assert ("has_flash", False) in collapsed_data,\
           "Whitelisted keys must not be collapsed."

def test_finalize_data():
    collapsed_data = {
        ('os', 'Darwin-11.0') : 22,
        ('resolution', '1280x1024') : 50,
        ('os', 'Windows_NT-6.11') : 34,
        ('resolution', '~2600x1400') : 16,
        ('os', 'Other') : 2,
        ('has_flash', True) : 72,
        ('os', 'Windows_NT-Other') : 16,
        ('has_flash', False) : 2,
        ('resolution', 'Other') : 8
    }

    finalized_data = finalize_data(collapsed_data, 74, 0.1, 0.2, datetime.strptime('20160703', '%Y%m%d'))
    
    # Check that the basic fields are in the finalized data.
    assert finalized_data['broken'] == 0.1,\
           "The ratio of broken data must be correctly reported."
    assert finalized_data['inactive'] == 0.2,\
           "The ratio of inactive clients must be correctly reported."
    assert finalized_data['date'] == "2016-07-03",\
           "The first day of the reporting period must be reported."
        
    # Make sure that all the reported numbers are ratios.
    all_ratios = [(v >= 0.0 and v <= 1.0) for (k, v) in finalized_data.iteritems() if k != 'date']
    assert all(all_ratios), "All the reported entries must be ratios."

def test_validate_finalized_data():
    MISSING_KEYS = {
        'browserArch_x86': 1.0,
        'cpuCores_2': 1.0
    }

    assert validate_finalized_data(MISSING_KEYS) == False,\
           "The validator must fail when expected keys are missing"
        
    KEYS_NOT_ADDING_UP = {
        'browserArch_x86': 0.5,
        'browserArch_x64': 0.4,
        'cpuCores_1': 1.0,
        'cpuCoresSpeed_2_2.2': 1.0,
        'cpuVendor_Vendor1': 1.0,
        'cpuSpeed_2.2': 1.0,
        'gpuVendor_Vendor3': 1.0,
        'gpuModel_Model1': 1.0,
        'resolution_800x600': 1.0,
        'ram_2': 1.0,
        'osName_SomeOS': 1.0,
        'osArch_x64': 1.0,
        'hasFlash_True': 1.0
    }

    assert validate_finalized_data(KEYS_NOT_ADDING_UP) == False,\
           "The validator must fail when the reported values don't add up to 1.0"
    
    WORKING_DATA = {
        'browserArch_x86': 0.7,
        'browserArch_x64': 0.29,
        'cpuCores_1': 0.5,
        'cpuCores_2': 0.5,
        'cpuCoresSpeed_2_2.2': 0.1,
        'cpuCoresSpeed_2_2.4': 0.9,
        'cpuVendor_Vendor1': 0.725,
        'cpuVendor_Vendor2': 0.275,
        'cpuSpeed_2.2': 0.1,
        'cpuSpeed_2.4': 0.9,
        'gpuVendor_Vendor3': 0.9,
        'gpuVendor_Vendor4': 0.1,
        'gpuModel_Model1': 0.00001,
        'gpuModel_Model2': 0.99999,
        'resolution_800x600': 1.0,
        'ram_2': 1.0,
        'osName_SomeOS': 1.0,
        'osArch_x64': 1.0,
        'hasFlash_True': 1.0,
        'broken': 0.1,
        'inactive': 0.1,
        'date': '2017-03-26'
    }

    assert validate_finalized_data(WORKING_DATA),\
           "The validator must not fail when the reported data is correct"

def create_row():
    with open('tests/longitudinal_schema.json') as infile:
        return json.load(infile)


def test_generate_report():
    spark = (SparkSession
         .builder
         .appName("hardware_report_dashboard")
         .getOrCreate())

    df = spark.read.json('tests/longitudinal_schema.json')
    df.createOrReplaceTempView('longitudinal')

    expected = {
        "cpuCores_4":1.0,
        "osArch_x86":1.0,
        "gpuModel_SI-PITCAIRN":1.0,
        "gpuVendor_AMD":1.0,
        "ram_8":1.0,
        "browserArch_x86":1.0,
        "cpuVendor_GenuineIntel":1.0,
        "osName_Windows-10.0":1.0,
        "broken":0.0,
        "inactive":0.0,
        "cpuSpeed_4.0":1.0,
        "date":"2016-07-03",
        "cpuCoresSpeed_4_4.0":1.0,
        "hasFlash_False":1.0,
        "resolution_1920x1200":1.0
    }

    assert generate_report(datetime.strptime('20160703', '%Y%m%d'), datetime.strptime('20160710', '%Y%m%d'), spark)['hwsurvey-weekly-20160307-20160907.json'] == expected
