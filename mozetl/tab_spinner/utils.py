import pandas as pd

from moztelemetry import get_pings_properties

def windows_only(p):
    return p["environment/system/os/name"] == "Windows_NT"

def e10s_enabled_only(p):
    return p["environment/settings/e10sEnabled"]

def long_spinners_keyed_by_build_and_client(ping):
    return ((ping["application/buildId"][:8], ping["clientId"]), (ping["payload/histograms/FX_TAB_SWITCH_SPINNER_VISIBLE_LONG_MS"], ping["payload/histograms/FX_TAB_SWITCH_SPINNER_VISIBLE_MS"]))

def add_tuple_series(x, y):
    long_x = x[0]
    long_y = y[0]
    short_x = x[1]
    short_y = y[1]

    if long_x is None:
        long_x = pd.Series()
    if long_y is None:
        long_y = pd.Series()
    if short_x is None:
        short_x = pd.Series()
    if short_y is None:
        short_y = pd.Series()

    return (long_x.add(long_y, fill_value=0.0), short_x.add(short_y, fill_value=0.0))

def bucket_by_long_severity_per_client(spinner_pair):
    buildId = spinner_pair[0][0]
    hist = spinner_pair[1][0]
    named_index = ["unaffected",
                   "0ms - 999ms",
                   "1000ms - 2296ms",
                   "2297ms - 5276ms",
                   "5277ms - 12123ms",
                   "12124ms - 27855ms",
                   "27856ms - 63999ms",
                   "64000ms+"]

    severity = pd.Series([0, 0, 0, 0, 0, 0, 0, 0], index=named_index)

    if hist is None or hist.empty:
        severity[named_index[0]] = 1
    elif hist[hist.index >= 64000].sum() > 0:
        severity[named_index[7]] = 1
    elif hist[hist.index >= 27856].sum() > 0:
        severity[named_index[6]] = 1
    elif hist[hist.index >= 12124].sum() > 0:
        severity[named_index[5]] = 1
    elif hist[hist.index >= 5277].sum() > 0:
        severity[named_index[4]] = 1
    elif hist[hist.index >= 2297].sum() > 0:
        severity[named_index[3]] = 1
    elif hist[hist.index >= 1000].sum() > 0:
        severity[named_index[2]] = 1
    elif hist[hist.index >= 0].sum() > 0:
        severity[named_index[1]] = 1

    return (buildId, severity)

def bucket_by_short_severity_per_client(spinner_pair):
    buildId = spinner_pair[0][0]
    long_hist = spinner_pair[1][0]
    hist = spinner_pair[1][1]

    named_index = ["unaffected",
                   "not short",
                   "0ms - 49ms",
                   "50ms - 99ms",
                   "100ms - 199ms",
                   "200ms - 399ms",
                   "400ms - 799ms",
                   "800ms+"]

    severity = pd.Series([0, 0, 0, 0, 0, 0, 0, 0], index=named_index)

    if hist is None or hist.empty or long_hist is None or long_hist.empty:
        severity[named_index[0]] = 1
    elif long_hist[long_hist.index >= 1000].sum() > 0:
        severity[named_index[1]] = 1
    elif hist[hist.index >= 800].sum() > 0:
        severity[named_index[7]] = 1
    elif hist[hist.index >= 400].sum() > 0:
        severity[named_index[6]] = 1
    elif hist[hist.index >= 200].sum() > 0:
        severity[named_index[5]] = 1
    elif hist[hist.index >= 100].sum() > 0:
        severity[named_index[4]] = 1
    elif hist[hist.index >= 50].sum() > 0:
        severity[named_index[3]] = 1
    elif hist[hist.index >= 0].sum() > 0:
        severity[named_index[2]] = 1

    return (buildId, severity)

def to_percentages(build_severities):
    severities = build_severities[1]
    total_clients = severities.sum()
    if total_clients > 0:
        return (build_severities[0], severities / total_clients)

def collect_aggregated_spinners(rdd, map_func):
    collected_percentages = rdd \
        .map(map_func) \
        .reduceByKey(lambda x, y: x + y) \
        .repartition(200) \
        .map(to_percentages) \
        .collect()

    return sorted(collected_percentages, key=lambda result: result[0])

def get_short_and_long_spinners(pings):

    properties = ["clientId",
                  "payload/histograms/FX_TAB_SWITCH_SPINNER_VISIBLE_LONG_MS",
                  "payload/histograms/FX_TAB_SWITCH_SPINNER_VISIBLE_MS",
                  "environment/system/os/name",
                  "application/buildId",
                  "environment/settings/e10sEnabled"]

    ping_props = get_pings_properties(pings, properties)

    windows_pings_only = ping_props.filter(windows_only)
    e10s_enabled_on_windows_pings_only = windows_pings_only.filter(e10s_enabled_only)
    grouped_spinners = e10s_enabled_on_windows_pings_only.repartition(200).map(long_spinners_keyed_by_build_and_client).reduceByKey(add_tuple_series)

    final_result_long = collect_aggregated_spinners(
        grouped_spinners,
        bucket_by_long_severity_per_client
    )

    final_result_short = collect_aggregated_spinners(
        grouped_spinners,
        bucket_by_short_severity_per_client
    )

    if round(final_result_short[0][1][2:].sum(), 3) == round(final_result_long[0][1][1], 3):
        print "Short and long counts match"
    else:
        print "Error: Short and long counts do not match"

    return {
        'long': final_result_long,
        'short': final_result_short,
    }
