"""TEMPORARY. Read a count_trace_prod dump and say which layer loses the NULL rows.

    python read_count_trace_prod.py ./trace.json
    python read_count_trace_prod.py \
        gs://benwu-correlations-output/count-trace-prod/release/trace.json

Prints the four independent null counts per column side by side and names the first layer
that diverges from BigQuery's own answer. See count_trace_prod.py for what each means.
"""

import json
import subprocess
import sys


METHODS = ["sql_direct", "connector_full", "connector_pruned", "rdd_path"]

# Columns production publishes correctly. If these diverge too, it isn't null-specific.
CONTROLS = {"process_type", "platform"}


def load(path):
    if path.startswith("gs://"):
        return json.loads(
            subprocess.run(
                ["gsutil", "cat", path], capture_output=True, check=True
            ).stdout
        )
    with open(path) as handle:
        return json.load(handle)


def index(trace):
    table = {}
    for row in trace["measurements"]:
        table.setdefault(row["column"], {})[row["method"]] = (
            row["null_reference"],
            row["null_witness_group"],
        )
    return table


def main(path):
    trace = load(path)
    env = trace.get("environment", {})

    print("channel: {}   versions: {}".format(trace["channel"], trace["versions"]))
    print("witness: {}\n".format(trace["witness_signature"]))

    print("environment:")
    for key in sorted(env):
        if key == "sql":
            continue
        print("  {:34} {}".format(key, env[key]))
    print()

    if trace.get("errors"):
        print("errors ({}):".format(len(trace["errors"])))
        for error in trace["errors"]:
            print("  {}: {}".format(error["where"], error["error"]))
        print()

    table = index(trace)

    print("null counts, reference / witness group")
    header = "{:26}".format("column") + "".join(
        "{:>21}".format(m) for m in METHODS
    )
    print(header)
    print("-" * len(header))

    diverged = {}
    for column in sorted(table):
        cells = []
        truth = table[column].get("sql_direct")
        for method in METHODS:
            value = table[column].get(method)
            if value is None:
                cells.append("{:>21}".format("-"))
                continue
            mark = ""
            if truth is not None and method != "sql_direct" and value != truth:
                mark = " *"
                diverged.setdefault(method, []).append(column)
            cells.append("{:>21}".format("{}/{}{}".format(value[0], value[1], mark)))
        flag = "  (control)" if column in CONTROLS else ""
        print("{:26}".format(column) + "".join(cells) + flag)

    print("\n* = disagrees with BigQuery's own count\n")
    print("verdict:")

    if not table:
        print("  no measurements recorded; check the errors above.")
        return

    if not diverged:
        print("  every Spark path agrees with BigQuery, so the read and the RDD are fine")
        print("  on this run. The counts going into find_deviations are correct, which")
        print("  means the loss happens inside it. Next step is the in-library trace")
        print("  (count_trace.py), which needs a crashcorrelations that has the TRACE")
        print("  hooks; production clones upstream, so point it at a fork or vendor it.")
        return

    # Report the earliest layer that breaks, since later ones inherit its error.
    for method in ["connector_full", "connector_pruned", "rdd_path"]:
        if method not in diverged:
            continue
        columns = diverged[method]
        controls = [c for c in columns if c in CONTROLS]
        if method == "connector_full":
            print("  the BigQuery connector undercounts nulls even with no column pruning")
            print("  ({} columns). The read itself is the bug, not anything in".format(
                len(columns)))
            print("  crashcorrelations. Check the connector jar recorded above: the image")
            print("  1.5 DAG pins it to gs://spark-lib/.../latest, which floats.")
        elif method == "connector_pruned":
            print("  the unpruned read is correct but the pruned one is not ({} columns).".format(
                len(columns)))
            print("  get_telemetry_crashes' .select() lets the connector push a column")
            print("  subset down, and that pushdown is losing null rows. Dropping the")
            print("  .select() or disabling pushdown would fix it.")
        else:
            print("  the dataframe is correct but counting through the RDD is not")
            print("  ({} columns). The fault is in the row conversion, not the read.".format(
                len(columns)))
        if controls:
            print("  note: controls {} also diverge, so this is not".format(controls))
            print("  specific to nulls.")
        break

    published = {"moz_crash_reason": (7183, 54)}
    for column, expected in published.items():
        actual = table.get(column, {}).get("connector_pruned")
        if actual and tuple(actual) == expected:
            print()
            print("  connector_pruned for {} is {}/{}, exactly what production".format(
                column, actual[0], actual[1]))
            print("  published. The bug is reproduced and localised to the read.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1])
