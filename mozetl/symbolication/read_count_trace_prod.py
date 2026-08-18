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


METHODS = ["sql_direct", "connector_full", "connector_pruned", "rdd_path",
           "level1_rdd_expr"]

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

    print("channel: {}   versions: {}{}".format(
        trace["channel"],
        trace["versions"],
        "  (pinned via --override-versions)" if trace.get("versions_overridden") else "",
    ))
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
            if method != "sql_direct":
                if truth is None:
                    # No ground truth for this column, so "no mark" must not read as
                    # "agrees". The verdict below refuses to conclude in this case.
                    mark = " ?"
                elif value != truth:
                    mark = " *"
                    diverged.setdefault(method, []).append(column)
            cells.append("{:>21}".format("{}/{}{}".format(value[0], value[1], mark)))
        flag = "  (control)" if column in CONTROLS else ""
        print("{:26}".format(column) + "".join(cells) + flag)

    print("\n* = disagrees with BigQuery's own count"
          "   ? = no ground truth, cannot say\n")
    print("verdict:")

    if not table:
        print("  no measurements recorded; check the errors above.")
        return

    # Refuse to conclude anything without ground truth. Agreement among the three Spark
    # paths means nothing on its own: they share the read, so a read bug moves all three
    # together. Only sql_direct is independent.
    missing_truth = [c for c in table if "sql_direct" not in table[c]]
    if missing_truth:
        print("  INCONCLUSIVE: no BigQuery ground truth for {} of {} columns".format(
            len(missing_truth), len(table)))
        print("  ({}).".format(", ".join(sorted(missing_truth))))
        print("  The Spark paths all share the same read, so their agreeing with each")
        print("  other says nothing. Fix the sql_direct errors above and rerun.")
        return

    # An empty or witness-free window can't show a discrepancy either way.
    total_reference = env.get("total_reference")
    total_witness = env.get("total_witness_group")
    if total_reference is not None and total_reference < 1000:
        print("  INCONCLUSIVE: only {} rows in the reference set.".format(total_reference))
        print("  Release normally has tens of thousands. The job asked for versions {}"
              .format(trace["versions"]))
        print("  which have barely shipped, so this window has almost no data. Rerun")
        print("  against a window whose versions carry real volume.")
        return
    if not total_witness:
        print("  INCONCLUSIVE: the witness signature has 0 crashes in this window, so")
        print("  there is nothing to compare for the group counts. Pick a window and")
        print("  versions where it appears, or change WITNESS_SIGNATURE.")
        return

    if not diverged:
        print("  every Spark path agrees with BigQuery, so the read and the RDD are fine")
        print("  on this run. The counts going into find_deviations are correct, which")
        print("  means the loss happens inside it. Next step is the in-library trace")
        print("  (count_trace.py), which needs a crashcorrelations that has the TRACE")
        print("  hooks; production clones upstream, so point it at a fork or vendor it.")
        return

    # Report the earliest layer that breaks, since later ones inherit its error.
    for method in ["connector_full", "connector_pruned", "rdd_path", "level1_rdd_expr"]:
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
        elif method == "rdd_path":
            print("  the dataframe is correct but counting through the RDD is not")
            print("  ({} columns). The fault is in the row conversion, not the read.".format(
                len(columns)))
        else:
            print("  the read and the plain RDD counts are correct, but find_deviations'")
            print("  own level 1 expression is not ({} columns). That expression is the".format(
                len(columns)))
            print("  bug: it builds frozenset([(key, p[key])]) over every column at once,")
            print("  so the fault is in how it keys or reduces, not in the data.")
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
