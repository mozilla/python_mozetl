# Migrated from Databricks to run on dataproc
# pip install:
# boto3==1.16.20
# click==7.1.2

import contextlib
import gc
import gzip
import json
import os
import random
import re
import time
import urllib
import uuid
from bisect import bisect
from datetime import datetime, timedelta
from io import BytesIO

import boto3
import click
from boto3.s3.transfer import S3Transfer
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, collect_list
from pyspark.sql.types import Row
from google.cloud import storage

UNSYMBOLICATED = "<unsymbolicated>"
SYMBOL_TRUNCATE_LENGTH = 200

spark = SparkSession.builder.appName("bhr-collection").getOrCreate()
sc = spark.sparkContext


def to_struct_of_arrays(a):
    if len(a) == 0:
        raise Exception("Need at least one item in array for this to work.")

    result = {k: [e[k] for e in a] for k in a[0].keys()}
    result["length"] = len(a)
    return result


class UniqueKeyedTable(object):
    def __init__(self, get_default_from_key, key_names=()):
        self.get_default_from_key = get_default_from_key
        self.key_to_index_map = {}
        self.key_names = key_names
        self.items = []

    def key_to_index(self, key):
        if key in self.key_to_index_map:
            return self.key_to_index_map[key]

        index = len(self.items)
        self.items.append(self.get_default_from_key(key))
        self.key_to_index_map[key] = index
        return index

    def key_to_item(self, key):
        return self.items[self.key_to_index(key)]

    def index_to_item(self, index):
        return self.items[index]

    def get_items(self):
        return self.items

    def inner_struct_of_arrays(self, items):
        if len(items) == 0:
            raise Exception("Need at least one item in array for this to work.")

        result = {}
        num_keys = len(self.key_names)
        for i in range(0, num_keys):
            result[self.key_names[i]] = [x[i] for x in items]

        result["length"] = len(items)
        return result

    def struct_of_arrays(self):
        return self.inner_struct_of_arrays(self.items)

    def sorted_struct_of_arrays(self, key):
        return self.inner_struct_of_arrays(sorted(self.items, key=key))


class GrowToFitList(list):
    def __setitem__(self, index, value):
        if index >= len(self):
            to_grow = index + 1 - len(self)
            self.extend([None] * to_grow)
        list.__setitem__(self, index, value)

    def __getitem__(self, index):
        if index >= len(self):
            return None
        return list.__getitem__(self, index)


def hexify(num):
    return "{0:#0{1}x}".format(num, 8)


def get_default_lib(name):
    return {
        "name": re.sub(r"\.pdb$", "", name),
        "offset": 0,
        "path": "",
        "debugName": name,
        "debugPath": name,
        "arch": "",
    }


def get_default_thread(name, minimal_sample_table):
    strings_table = UniqueKeyedTable(lambda str: str)
    libs = UniqueKeyedTable(get_default_lib)
    func_table = UniqueKeyedTable(
        lambda key: (
            strings_table.key_to_index(key[0]),
            None if key[1] is None else libs.key_to_index(key[1]),
        ),
        ("name", "lib"),
    )
    stack_table = UniqueKeyedTable(
        lambda key: (key[2], func_table.key_to_index((key[0], key[1]))),
        ("prefix", "func"),
    )
    annotations_table = UniqueKeyedTable(
        lambda key: (
            key[0],
            strings_table.key_to_index(key[1]),
            strings_table.key_to_index(key[2]),
        ),
        ("prefix", "name", "value"),
    )
    if minimal_sample_table:
        sample_table = UniqueKeyedTable(
            lambda key: (
                key[0],
                strings_table.key_to_index(key[1]),
                key[2],
                strings_table.key_to_index(key[3]),
            ),
            ("stack", "platform"),
        )
    else:
        sample_table = UniqueKeyedTable(
            lambda key: (
                key[0],
                strings_table.key_to_index(key[1]),
                key[2],
                strings_table.key_to_index(key[3]),
            ),
            ("stack", "runnable", "annotations", "platform"),
        )

    stack_table.key_to_index(("(root)", None, None))

    prune_stack_cache = UniqueKeyedTable(lambda key: [0.0])
    prune_stack_cache.key_to_index(("(root)", None, None))

    return {
        "name": name,
        "libs": libs,
        "funcTable": func_table,
        "stackTable": stack_table,
        "annotationsTable": annotations_table,
        "pruneStackCache": prune_stack_cache,
        "sampleTable": sample_table,
        "stringArray": strings_table,
        "processType": "tab"
        if name == "Gecko_Child" or name == "Gecko_Child_ForcePaint"
        else "default",
        "dates": UniqueKeyedTable(
            lambda date: (
                {
                    "date": date,
                    "sampleHangMs": GrowToFitList(),
                    "sampleHangCount": GrowToFitList(),
                }
            ),
            ("date", "sampleHangMs", "sampleHangCount"),
        ),
    }


def reconstruct_stack(string_array, func_table, stack_table, lib_table, stack_index):
    result = []
    while stack_index != 0:
        func_index = stack_table["func"][stack_index]
        prefix = stack_table["prefix"][stack_index]
        func_name = string_array[func_table["name"][func_index]]
        lib_name = lib_table[func_table["lib"][func_index]]["debugName"]
        result.append((func_name, lib_name))
        stack_index = prefix
    return result[::-1]


def merge_number_dicts(a, b):
    keys = set(a.keys()).union(set(b.keys()))
    return {k: a.get(k, 0.0) + b.get(k, 0.0) for k in keys}


class ProfileProcessor(object):
    def __init__(self, config):
        self.config = config

        def default_thread_closure(name):
            return get_default_thread(name, config["use_minimal_sample_table"])

        self.thread_table = UniqueKeyedTable(default_thread_closure)
        self.usage_hours_by_date = {}

    def debug_dump(self, dump_str):
        if self.config["print_debug_info"]:
            print(dump_str)

    def ingest_processed_profile(self, profile):
        for existing_thread in self.thread_table.get_items():
            prune_stack_cache = UniqueKeyedTable(lambda key: [0.0])
            prune_stack_cache.key_to_index(("(root)", None, None))
            existing_thread["pruneStackCache"] = prune_stack_cache

        sample_size = self.config["post_sample_size"]
        threads = profile["threads"]
        for other in threads:
            other_samples = other["sampleTable"]
            other_dates = other["dates"]

            for date in other_dates:
                build_date = date["date"]
                for i in range(0, len(date["sampleHangCount"])):
                    stack_index = other_samples["stack"][i]
                    stack = reconstruct_stack(
                        other["stringArray"],
                        other["funcTable"],
                        other["stackTable"],
                        other["libs"],
                        stack_index,
                    )
                    self.pre_ingest_row(
                        (
                            stack,
                            other["stringArray"][other_samples["runnable"][i]],
                            other["name"],
                            build_date,
                            other_samples["annotations"][i],
                            other["stringArray"][other_samples["platform"][i]],
                            date["sampleHangMs"][i],
                            date["sampleHangCount"][i],
                        )
                    )

            for date in other_dates:
                build_date = date["date"]
                for i in range(0, len(date["sampleHangCount"])):
                    stack_index = other_samples["stack"][i]
                    stack = reconstruct_stack(
                        other["stringArray"],
                        other["funcTable"],
                        other["stackTable"],
                        other["libs"],
                        stack_index,
                    )
                    if sample_size == 1.0 or random.random() <= sample_size:
                        self.ingest_row(
                            (
                                stack,
                                other["stringArray"][other_samples["runnable"][i]],
                                other["name"],
                                build_date,
                                other_samples["annotations"][i],
                                other["stringArray"][other_samples["platform"][i]],
                                date["sampleHangMs"][i],
                                date["sampleHangCount"][i],
                            )
                        )

        self.usage_hours_by_date = merge_number_dicts(
            self.usage_hours_by_date, profile.get("usageHoursByDate", {})
        )

    def pre_ingest_row(self, row):
        (
            stack,
            runnable_name,
            thread_name,
            build_date,
            annotations,
            platform,
            hang_ms,
            hang_count,
        ) = row

        thread = self.thread_table.key_to_item(thread_name)
        prune_stack_cache = thread["pruneStackCache"]
        root_stack = prune_stack_cache.key_to_item(("(root)", None, None))
        root_stack[0] += hang_ms

        last_stack = 0
        for func_name, lib_name in stack:
            last_stack = prune_stack_cache.key_to_index(
                (func_name, lib_name, last_stack)
            )
            cache_item = prune_stack_cache.index_to_item(last_stack)
            cache_item[0] += hang_ms

    def ingest_row(self, row):
        (
            stack,
            runnable_name,
            thread_name,
            build_date,
            annotations,
            platform,
            hang_ms,
            hang_count,
        ) = row

        thread = self.thread_table.key_to_item(thread_name)
        stack_table = thread["stackTable"]
        annotations_table = thread["annotationsTable"]
        sample_table = thread["sampleTable"]
        dates = thread["dates"]
        prune_stack_cache = thread["pruneStackCache"]

        last_annotation = None
        for name, value in annotations:
            last_annotation = annotations_table.key_to_index(
                (last_annotation, name, value)
            )

        last_stack = 0
        last_cache_item_index = 0
        for func_name, lib_name in stack:
            cache_item_index = prune_stack_cache.key_to_index(
                (func_name, lib_name, last_cache_item_index)
            )
            cache_item = prune_stack_cache.index_to_item(cache_item_index)
            parent_cache_item = prune_stack_cache.index_to_item(last_cache_item_index)
            if (
                cache_item[0] / parent_cache_item[0]
                > self.config["stack_acceptance_threshold"]
            ):
                last_stack = stack_table.key_to_index((func_name, lib_name, last_stack))
                last_cache_item_index = cache_item_index
            else:
                # If we're below the acceptance threshold, just lump it under (other) below
                # its parent.
                last_stack = stack_table.key_to_index(("(other)", lib_name, last_stack))
                break

        if self.config["use_minimal_sample_table"] and thread_name == "Gecko_Child":
            return

        sample_index = sample_table.key_to_index(
            (last_stack, runnable_name, last_annotation, platform)
        )

        date = dates.key_to_item(build_date)
        if date["sampleHangCount"][sample_index] is None:
            date["sampleHangCount"][sample_index] = 0.0
            date["sampleHangMs"][sample_index] = 0.0

        date["sampleHangCount"][sample_index] += hang_count
        date["sampleHangMs"][sample_index] += hang_ms

    def ingest(self, data, usage_hours_by_date):
        print("{} unfiltered samples in data".format(len(data)))
        data = [
            x
            for x in data
            # x[6] should be hang_ms
            if x[6] > 0.0
        ]
        print("{} filtered samples in data".format(len(data)))

        print("Preprocessing stacks for prune cache...")
        for row in data:
            self.pre_ingest_row(row)

        print("Processing stacks...")
        for row in data:
            self.ingest_row(row)

        self.usage_hours_by_date = merge_number_dicts(
            self.usage_hours_by_date, usage_hours_by_date
        )

    def process_date(self, date):
        if self.config["use_minimal_sample_table"]:
            return {
                "date": date["date"],
                "sampleHangCount": date["sampleHangCount"],
            }
        return date

    def process_thread(self, thread):
        string_array = thread["stringArray"]
        func_table = thread["funcTable"].struct_of_arrays()
        stack_table = thread["stackTable"].struct_of_arrays()
        annotations_table = thread["annotationsTable"].struct_of_arrays()
        sample_table = thread["sampleTable"].struct_of_arrays()

        return {
            "name": thread["name"],
            "processType": thread["processType"],
            "libs": thread["libs"].get_items(),
            "funcTable": func_table,
            "stackTable": stack_table,
            "annotationsTable": annotations_table,
            "sampleTable": sample_table,
            "stringArray": string_array.get_items(),
            "dates": [self.process_date(d) for d in thread["dates"].get_items()],
        }

    def process_into_split_profile(self):
        return {
            "main_payload": {
                "splitFiles": {
                    t["name"]: [k for k in t.keys() if k != "name"]
                    for t in self.thread_table.get_items()
                },
                "usageHoursByDate": self.usage_hours_by_date,
                "uuid": self.config["uuid"],
                "isSplit": True,
            },
            "file_data": [
                [
                    (t["name"] + "_" + k, v)
                    for k, v in self.process_thread(t).iteritems()
                    if k != "name"
                ]
                for t in self.thread_table.get_items()
            ],
        }

    def process_into_profile(self):
        print("Processing into final format...")
        if self.config["split_threads_in_out_file"]:
            return [
                {
                    "name": t["name"],
                    "threads": [self.process_thread(t)],
                    "usageHoursByDate": self.usage_hours_by_date,
                    "uuid": self.config["uuid"],
                }
                for t in self.thread_table.get_items()
            ]

        return {
            "threads": [self.process_thread(t) for t in self.thread_table.get_items()],
            "usageHoursByDate": self.usage_hours_by_date,
            "uuid": self.config["uuid"],
        }


def deep_merge(original, overrides):
    original_copy = original.copy()
    for k, v in overrides.iteritems():
        if (
            isinstance(v, dict)
            and k in original_copy
            and isinstance(original_copy[k], dict)
        ):
            original_copy[k] = deep_merge(original_copy[k], v)
        else:
            original_copy[k] = v
    return original_copy


def shallow_merge(original, overrides):
    original_copy = original.copy()
    for k, v in overrides.iteritems():
        original_copy[k] = v
    return original_copy


def time_code(name, callback):
    print("{}...".format(name))
    start = time.time()
    result = callback()
    end = time.time()
    delta = end - start
    print("{} took {}ms to complete".format(name, int(round(delta * 1000))))
    debug_print_rdd_count(result)
    return result


def get_prop(val, prop):
    if val is None:
        return None

    for key in prop.split("/"):
        val = val[key]
        if val is None:
            return None
    return val


def get_ping_properties(ping, properties):
    result = {}
    for prop in properties:
        result[prop] = get_prop(ping, prop)
    return result


def properties_are_not_none(ping, properties):
    for prop in properties:
        if ping[prop] is None:
            return False

    return True


def get_data(sc, sql_context, config, date, end_date=None):
    sql_context.sql("set spark.sql.shuffle.partitions={}".format(sc.defaultParallelism))

    if end_date is None:
        end_date = date

    submission_start_str = date - timedelta(days=5)
    submission_end_str = end_date + timedelta(days=5)

    date_str = date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    pings_df = (
        spark.read.format("bigquery")
        .option("table", "moz-fx-data-shared-prod.telemetry_stable.bhr_v4")
        .load()
        .where(
            "submission_timestamp>=to_date('%s') and submission_timestamp<=to_date('%s')"
            % (submission_start_str, submission_end_str)
        )
        .where("normalized_channel='nightly'")
        .sample(config["sample_size"])
    )

    if config["exclude_modules"]:
        properties = [
            "environment/system/os/name",
            "environment/system/os/version",
            "application/architecture",
            "application/build_id",
            "payload/hangs",
            "payload/time_since_last_ping",
        ]
    else:
        properties = [
            "environment/system/os/name",
            "environment/system/os/version",
            "application/architecture",
            "application/build_id",
            "payload/modules",
            "payload/hangs",
            "payload/time_since_last_ping",
        ]

    print("%d results total" % pings_df.rdd.count())
    mapped = pings_df.rdd.map(lambda p: get_ping_properties(p, properties)).filter(
        lambda p: properties_are_not_none(p, properties)
    )

    try:
        result = mapped.filter(
            lambda p: p["application/build_id"][:8] >= date_str
            and p["application/build_id"][:8] <= end_date_str
        )
        print("%d results after first filter" % result.count())
        return result
    except ValueError:
        return None


def ping_is_valid(ping):
    if not isinstance(ping["environment/system/os/version"], str):
        return False
    if not isinstance(ping["environment/system/os/name"], str):
        return False
    if not isinstance(ping["application/build_id"], str):
        return False
    if not isinstance(ping["payload/time_since_last_ping"], int):
        return False

    return True


def module_to_string(module):
    if module is None:
        return None
    return module[0] + "\\" + str(module[1])


def string_to_module(string_module):
    if string_module is None:
        return None
    split_module = string_module.split("\\")
    if len(split_module) != 2:
        raise Exception("Module strings had an extra \\")
    return (split_module[0], None if split_module[1] == "None" else split_module[1])


def process_frame(frame, modules):
    if isinstance(frame, list):
        module_index, offset = frame
        if module_index is None or module_index < 0 or module_index >= len(modules):
            return (None, offset)
        debug_name, breakpad_id = modules[module_index]
        return ((debug_name, breakpad_id), offset)
    else:
        return (("pseudo", None), frame)


def filter_hang(hang):
    return (
        hang["thread"] == "Gecko"
        and len(hang["stack"]) > 0
        and len(hang["stack"]) < 300
    )


def process_hang(hang):
    result = hang.asDict()
    result["stack"] = json.loads(hang["stack"])
    return result


def process_hangs(ping):
    build_date = ping["application/build_id"][:8]  # "YYYYMMDD" : 8 characters

    platform = "{}".format(ping["environment/system/os/name"])

    modules = ping["payload/modules"]
    hangs = [process_hang(h) for h in ping["payload/hangs"]]
    if hangs is None:
        return []

    pre_result = [
        (
            [
                process_frame(frame, modules)
                for frame in h["stack"]
                if not isinstance(frame, list) or len(frame) == 2
            ],
            h["duration"],
            h["thread"],
            "",
            h["process"],
            h["annotations"],
            build_date,
            platform,
        )
        for h in hangs
        if filter_hang(h)
    ]

    result = []
    for (
        stack,
        duration,
        thread,
        runnable_name,
        process,
        annotations,
        build_date,
        platform,
    ) in pre_result:
        result.append(
            (
                stack,
                duration,
                thread,
                runnable_name,
                process,
                annotations,
                build_date,
                platform,
            )
        )

        if "PaintWhileInterruptingJS" in annotations:
            result.append(
                (
                    stack,
                    duration,
                    "Gecko_Child_ForcePaint",
                    runnable_name,
                    process,
                    annotations,
                    build_date,
                    platform,
                )
            )

    return result


def get_all_hangs(pings):
    return pings.flatMap(process_hangs)


def map_to_frame_info(hang):
    memory_map = hang["hang"]["nativeStack"]["memoryMap"]
    stack = hang["hang"]["nativeStack"]["stacks"][0]
    return [
        (tuple(memory_map[module_index]), (offset,))
        if module_index != -1
        else (None, (offset,))
        for module_index, offset in stack
    ]


def get_frames_by_module(hangs):
    return (
        hangs.flatMap(lambda hang: hang[0])  # turn into an RDD of frames
        .map(lambda frame: (frame[0], (frame[1],)))
        .distinct()
        .reduceByKey(lambda a, b: a + b)
    )


def symbolicate_stacks(stack, processed_modules):
    symbol_map = {k: v for k, v in processed_modules}
    symbolicated = []
    for module, offset in stack:
        if module is not None:
            debug_name = module[0]
            processed = symbol_map.get((tuple(module), offset), None)
            if processed is not None and processed[0] is not None:
                symbolicated.append(processed)
            else:
                symbolicated.append((UNSYMBOLICATED, debug_name))
        else:
            symbolicated.append((UNSYMBOLICATED, "unknown"))
    return symbolicated


def tupleize_annotation_list(d):
    return tuple((k, v) for k, v in sorted(d, key=lambda x: x[0]))


def map_to_hang_data(hang, config):
    (
        stack,
        duration,
        thread,
        runnable_name,
        process,
        annotations,
        build_date,
        platform,
    ) = hang
    result = []
    if duration < config["hang_lower_bound"]:
        return result
    if duration >= config["hang_upper_bound"]:
        return result

    key = (
        tuple((a, b) for a, b in stack),
        runnable_name,
        thread,
        build_date,
        tupleize_annotation_list(annotations),
        platform,
    )

    result.append((key, (float(duration), 1.0)))

    return result


def merge_hang_data(a, b):
    return (
        a[0] + b[0],
        a[1] + b[1],
    )


def process_hang_key(key, processed_modules):
    stack = key[0]
    symbolicated = symbolicate_stacks(stack, processed_modules)

    return (symbolicated,) + tuple(key[1:])


def process_hang_value(key, val, usage_hours_by_date):
    stack, runnable_name, thread, build_date, annotations, platform = key
    return (
        val[0] / usage_hours_by_date[build_date],
        val[1] / usage_hours_by_date[build_date],
    )


def get_frames_with_hang_id(hang_tuple):
    hang_id, hang = hang_tuple
    stack = hang[0]
    return [(frame, hang_id) for frame in stack]


def get_symbolication_mapping_by_hang_id(joined):
    unsymbolicated, (hang_id, symbolicated) = joined
    return (hang_id, {unsymbolicated: symbolicated})


def symbolicate_hang_with_mapping(joined):
    hang, symbol_map = joined
    return process_hang_key(hang, symbol_map)


def symbolicate_hang_keys(hangs, processed_modules):
    hangs_by_id = hangs.zipWithUniqueId().map(lambda x: (x[1], x[0]))
    hang_ids_by_frame = hangs_by_id.flatMap(get_frames_with_hang_id)

    # NOTE: this is the logic that we replaced with Dataframes
    # symbolication_maps_by_hang_id = (hang_ids_by_frame.leftOuterJoin(processed_modules)
    #                                  .map(get_symbolication_mapping_by_hang_id)
    #                                  .reduceByKey(shallow_merge))
    # return hangs_by_id.join(symbolication_maps_by_hang_id).map(symbolicate_hang_with_mapping)

    def get_hang_id_by_frame_row(hang_id_by_frame):
        frame, hang_id = hang_id_by_frame
        return Row(module_to_string(frame[0]), frame[1], hang_id)

    hibf_cols = ["module", "offset", "hang_id"]
    hibf_df = hang_ids_by_frame.map(get_hang_id_by_frame_row).toDF(hibf_cols)

    def get_processed_modules_row(processed_module):
        (module, offset), (symbol, module_name) = processed_module
        return Row(module_to_string(module), offset, symbol, module_name)

    pm_cols = ["module", "offset", "symbol", "module_name"]
    pm_df = processed_modules.map(get_processed_modules_row).toDF(pm_cols)

    smbhid_df = hibf_df.join(pm_df, on=["module", "offset"], how="left_outer")
    debug_print_rdd_count(smbhid_df.rdd)

    symbol_mapping_array = array("module", "offset", "symbol", "module_name")
    symbol_mappings_df = (
        smbhid_df.select("hang_id", symbol_mapping_array.alias("symbol_mapping"))
        .groupBy("hang_id")
        .agg(collect_list("symbol_mapping").alias("symbol_mappings"))
    )
    debug_print_rdd_count(symbol_mappings_df.rdd)

    def get_hang_by_id_row(hang_by_id):
        hang_id, hang = hang_by_id
        return Row(hang_id, json.dumps(hang, ensure_ascii=False))

    hbi_cols = ["hang_id", "hang_json"]
    hbi_df = hangs_by_id.map(get_hang_by_id_row).toDF(hbi_cols)

    result_df = hbi_df.join(symbol_mappings_df, on=["hang_id"])
    debug_print_rdd_count(result_df.rdd)

    def get_result_obj_from_row(row):
        # creates a tuple of (unsymbolicated, symbolicated) for each item in row.symbol_mappings
        mappings = tuple(
            ((string_to_module(mapping[0]), mapping[1]), (mapping[2], mapping[3]))
            for mapping in row.symbol_mappings
        )
        hang = json.loads(row.hang_json)
        return hang, mappings

    result = result_df.rdd.map(get_result_obj_from_row)
    debug_print_rdd_count(result)

    return result.map(symbolicate_hang_with_mapping)


def get_grouped_sums_and_counts(hangs, usage_hours_by_date, config):
    reduced = (
        hangs.flatMap(lambda hang: map_to_hang_data(hang, config))
        .reduceByKey(merge_hang_data)
        .collect()
    )
    items = [(k, process_hang_value(k, v, usage_hours_by_date)) for k, v in reduced]
    return [k + v for k, v in items if k is not None]


def get_usage_hours(ping):
    build_date = ping["application/build_id"][:8]  # "YYYYMMDD" : 8 characters
    usage_hours = float(ping["payload/time_since_last_ping"]) / 3600000.0
    return (build_date, usage_hours)


def merge_usage_hours(a, b):
    return a + b


def get_usage_hours_by_date(pings):
    return pings.map(get_usage_hours).reduceByKey(merge_usage_hours).collectAsMap()


def make_sym_map(data):
    public_symbols = {}
    func_symbols = {}

    for line in data.splitlines():
        line = line.decode("utf-8")
        if line.startswith("PUBLIC "):
            line = line.rstrip()
            fields = line.split(" ", 3)
            m_offset = 0
            if fields[1] == "m":
                m_offset = 1
                fields = line.split(" ", 4)
            if len(fields) < 4 + m_offset:
                raise ValueError("Failed to parse address - line: {}".format(line))
            address = int(fields[1 + m_offset], 16)
            symbol = fields[3 + m_offset]
            public_symbols[address] = symbol[:SYMBOL_TRUNCATE_LENGTH]
        elif line.startswith("FUNC "):
            line = line.rstrip()
            fields = line.split(" ", 4)
            m_offset = 0
            if fields[1] == "m":
                m_offset = 1
                fields = line.split(" ", 5)
            if len(fields) == 4 + m_offset:
                symbol = "(no symbol)"
            elif len(fields) < 4 + m_offset:
                raise ValueError("Failed to parse address - line: {}".format(line))
            else:
                symbol = fields[4 + m_offset]
            address = int(fields[1 + m_offset], 16)
            func_symbols[address] = symbol[:SYMBOL_TRUNCATE_LENGTH]
    # Prioritize PUBLIC symbols over FUNC ones
    sym_map = func_symbols
    sym_map.update(public_symbols)

    return sorted(sym_map), sym_map


def get_file_url(module, config):
    lib_name, breakpad_id = module
    if lib_name is None or breakpad_id is None:
        return None
    if lib_name.endswith(".pdb"):
        file_name = lib_name[:-4] + ".sym"
    else:
        file_name = lib_name + ".sym"

    try:
        return config["symbol_server_url"] + "/".join(
            [
                urllib.parse.quote_plus(lib_name),
                urllib.parse.quote_plus(breakpad_id),
                urllib.parse.quote_plus(file_name),
            ]
        )
    except KeyError:
        # urllib throws with unicode strings. TODO: investigate why
        # any of these values (lib_name, breakpad_id, file_name) would
        # have unicode strings, or if this is just bad pings.
        return None


def process_module(module, offsets, config):
    result = []
    if module is None or module[0] is None:
        return [((module, offset), (UNSYMBOLICATED, "unknown")) for offset in offsets]
    if module[0] == "pseudo":
        return [
            ((module, offset), ("" if offset is None else offset, ""))
            for offset in offsets
        ]
    file_url = get_file_url(module, config)
    module_name = module[0]
    if file_url:
        success, response = fetch_url(file_url)
    else:
        success = False

    if success:
        sorted_keys, sym_map = make_sym_map(response)

        for offset in offsets:
            try:
                i = bisect(sorted_keys, int(offset, 16))
                key = sorted_keys[i - 1] if i else None
                symbol = sym_map.get(key)
            except UnicodeEncodeError:
                symbol = None
            except ValueError:
                symbol = None
            if symbol is not None:
                result.append(((module, offset), (symbol, module_name)))
            else:
                result.append(((module, offset), (UNSYMBOLICATED, module_name)))
    else:
        for offset in offsets:
            result.append(((module, offset), (UNSYMBOLICATED, module_name)))
    return result


def process_modules(frames_by_module, config):
    return frames_by_module.flatMap(lambda x: process_module(x[0], x[1], config))


def debug_print_rdd_count(rdd, really=False):
    if really:
        print("RDD count:{}".format(rdd.count()))


def transform_pings(_, pings, config):
    global DEBUG_VARS
    DEBUG_VARS = []
    print("Transforming pings")
    filtered = time_code(
        "Filtering to valid pings", lambda: pings.filter(ping_is_valid)
    )
    DEBUG_VARS.append(filtered.first())

    hangs = time_code(
        "Filtering to hangs with native stacks", lambda: get_all_hangs(filtered)
    )

    DEBUG_VARS.append(hangs.first())

    frames_by_module = time_code(
        "Getting stacks by module", lambda: get_frames_by_module(hangs)
    )

    processed_modules = time_code(
        "Processing modules", lambda: process_modules(frames_by_module, config)
    )

    hangs = symbolicate_hang_keys(hangs, processed_modules)

    usage_hours_by_date = time_code(
        "Getting usage hours", lambda: get_usage_hours_by_date(filtered)
    )

    result = time_code(
        "Grouping stacks",
        lambda: get_grouped_sums_and_counts(hangs, usage_hours_by_date, config),
    )
    return result, usage_hours_by_date


def fetch_url(url):
    result = False, ""
    try:
        with contextlib.closing(urllib.request.urlopen(url)) as response:
            response_code = response.getcode()
            if response_code == 404:
                return False, ""
            if response_code != 200:
                result = False, ""
            return True, decode_response(response)
    except IOError:
        result = False, ""

    if not result[0]:
        try:
            with contextlib.closing(urllib.request.urlopen(url)) as response:
                response_code = response.getcode()
                if response_code == 404:
                    return False, ""
                if response_code != 200:
                    result = False, ""
                return True, decode_response(response)
        except IOError:
            result = False, ""

    return result


def decode_response(response):
    headers = response.info()
    content_encoding = headers.get("Content-Encoding", "").lower()
    if content_encoding in ("gzip", "x-gzip", "deflate"):
        with contextlib.closing(BytesIO(response.read())) as data_stream:
            try:
                with gzip.GzipFile(fileobj=data_stream) as f:
                    return f.read()
            except EnvironmentError:
                data_stream.seek(0)
                return data_stream.read().decode("zlib")
    return response.read()


def read_file(name, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    if config["read_files_from_network"]:
        s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        url = config["analysis_output_url"] + s3_key
        success, response = fetch_url(url)
        if not success:
            raise Exception("Could not find file at url: " + url)
        return json.loads(response)
    else:
        if config["append_date"]:
            filename = "./output/%s-%s.json" % (name, end_date_str)
        else:
            filename = "./output/%s.json" % name
        gzfilename = filename + ".gz"
        with gzip.open(gzfilename, "r") as f:
            return json.loads(f.read())


def write_file(name, stuff, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    if config["append_date"]:
        filename = "./output/%s-%s.json" % (name, end_date_str)
    else:
        filename = "./output/%s.json" % name
    gzfilename = filename + ".gz"
    jsonblob = json.dumps(stuff, ensure_ascii=False).encode("utf-8")

    if not os.path.exists("./output"):
        os.makedirs("./output")
    with gzip.open(gzfilename, "w") as f:
        f.write(jsonblob)

    if config["use_s3"]:
        bucket = "telemetry-public-analysis-2"
        s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        client = boto3.client("s3", "us-west-2")
        transfer = S3Transfer(client)
        extra_args = {"ContentType": "application/json", "ContentEncoding": "gzip"}
        transfer.upload_file(gzfilename, bucket, s3_key, extra_args=extra_args)
        if config["uuid"] is not None:
            s3_uuid_key = (
                "bhr/data/hang_aggregates/" + name + "_" + config["uuid"] + ".json"
            )
            transfer.upload_file(gzfilename, bucket, s3_uuid_key, extra_args=extra_args)
    elif config["use_gcs"]:
        bucket_name = "moz-fx-data-static-websit-8565-analysis-output"
        gcs_key = "bhr/data/hang_aggregates/" + name + ".json"
        if config["uuid"] is not None:
            gcs_key = (
                "bhr/data/hang_aggregates/" + name + "_" + config["uuid"] + ".json"
            )
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_key)
        blob.upload_from_filename(gzfilename)


default_config = {
    "start_date": datetime.today() - timedelta(days=9),
    "end_date": datetime.today() - timedelta(days=1),
    "use_s3": True,
    "use_gcs": False,
    "sample_size": 0.50,
    "symbol_server_url": "https://symbols.mozilla.org/",
    "hang_profile_in_filename": "hang_profile_128_16000",
    "hang_profile_out_filename": None,
    "print_debug_info": False,
    "hang_lower_bound": 128,
    "hang_upper_bound": 16000,
    "stack_acceptance_threshold": 0.0,
    "hang_outlier_threshold": 512,
    "append_date": False,
    "channel": "nightly",
    "analysis_output_url": "https://analysis-output.telemetry.mozilla.org/",
    "read_files_from_network": False,
    "split_threads_in_out_file": False,
    "use_minimal_sample_table": False,
    "post_sample_size": 1.0,
    "exclude_modules": False,
    "uuid": uuid.uuid4().hex,
}


def print_progress(
    job_start, iterations, current_iteration, iteration_start, iteration_name
):
    iteration_end = time.time()
    iteration_delta = iteration_end - iteration_start
    print(
        "Iteration for {} took {}s".format(iteration_name, int(round(iteration_delta)))
    )
    job_elapsed = iteration_end - job_start
    percent_done = float(current_iteration + 1) / float(iterations)
    projected = job_elapsed / percent_done
    remaining = projected - job_elapsed
    print("Job should finish in {}".format(timedelta(seconds=remaining)))


def etl_job_daily(sc, sql_context, config=None):
    final_config = {}
    final_config.update(default_config)

    if config is not None:
        final_config.update(config)

    if final_config["hang_profile_out_filename"] is None:
        final_config["hang_profile_out_filename"] = final_config[
            "hang_profile_in_filename"
        ]

    iterations = (final_config["end_date"] - final_config["start_date"]).days + 1
    job_start = time.time()
    current_date = None
    transformed = None
    usage_hours = None
    for x in range(iterations):
        iteration_start = time.time()
        current_date = final_config["start_date"] + timedelta(days=x)
        date_str = current_date.strftime("%Y%m%d")
        data = time_code(
            "Getting data",
            lambda: get_data(sc, sql_context, final_config, current_date),
        )
        if data is None:
            print("No data")
            continue
        transformed, usage_hours = transform_pings(sc, data, final_config)
        profile_processor = ProfileProcessor(final_config)
        profile_processor.ingest(transformed, usage_hours)
        profile = profile_processor.process_into_profile()
        filepath = "%s_%s" % (final_config["hang_profile_out_filename"], date_str)
        print("writing file %s" % filepath)
        write_file(filepath, profile, final_config)
        filepath = "%s_current" % final_config["hang_profile_out_filename"]
        print("writing file %s" % filepath)
        write_file(filepath, profile, final_config)
        gc.collect()
        print_progress(job_start, iterations, x, iteration_start, date_str)


@click.command()
@click.option("--date", type=datetime.fromisoformat, required=True)
@click.option(
    "--sample-size",
    type=float,
    default=0.5,
    help="Proportion of pings to use (1.0 is 100%)",
)
@click.option("--use_gcs", is_flag=True, default=False)
def start_job(date, sample_size, use_gcs):
    print(f"Running for {date}")
    print(f"Using sample size {sample_size}")
    etl_job_daily(
        sc,
        spark,
        {
            "start_date": date - timedelta(days=4),
            "end_date": date - timedelta(days=4),
            "hang_profile_in_filename": "hangs_main",
            "hang_profile_out_filename": "hangs_main",
            "hang_lower_bound": 128,
            "hang_upper_bound": 65536,
            "sample_size": sample_size,
            "use_gcs": use_gcs,
            "use_s3": not use_gcs,
        },
    )


if __name__ == "__main__":
    start_job()
