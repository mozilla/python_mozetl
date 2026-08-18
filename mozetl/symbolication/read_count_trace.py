"""TEMPORARY. Read a count_trace dump and say where the NULL counts go wrong.

    python read_count_trace.py gs://benwu-correlations-output/count-trace/release/trace.json
    python read_count_trace.py ./trace.json

The trace records the same counts at four points. This prints them side by side for the
witness item and says which transition breaks, so the answer doesn't depend on reading raw
JSON. See count_trace.py for what each stage means.
"""

import json
import subprocess
import sys


WITNESS_COLUMN = "moz_crash_reason"

# What production published for the item being explained. Fixed, because it's a fact about
# the Aug 17 published output rather than about any particular run.
PRODUCTION_REFERENCE = 7183
PRODUCTION_WITNESS_GROUP = 54

# Truth is NOT hardcoded. The job's window is utcnow()-5, so it moves every day and the
# absolute counts move with it. An earlier version of this file compared against fixed
# numbers from one window, which would report a divergence on any other day. Instead the
# groupBy stage recorded in the trace itself is the reference: it runs on the same
# dataframe, in the same run, and doesn't share the flatMap/reduceByKey path being tested.
# total_groups[witness] is the upper bound on any group count for that signature.


def load(path):
    if path.startswith("gs://"):
        return json.loads(
            subprocess.run(
                ["gsutil", "cat", path], capture_output=True, check=True
            ).stdout
        )
    with open(path) as handle:
        return json.load(handle)


def _is_witness_item(items):
    return len(items) == 1 and items[0]["column"] == WITNESS_COLUMN and items[0]["is_none"]


def stage_value(trace, stage, group):
    for row in trace["stages"].get(stage, []):
        if "items" in row and _is_witness_item(row["items"]) and row["group"] == group:
            return row["count"]
    return None


def independent_value(trace, group):
    for row in trace["stages"].get("level1_independent", []):
        if row.get("column") != WITNESS_COLUMN or not row.get("is_none"):
            continue
        if group == "reference" and "group" not in row:
            return row["count"]
        if group != "reference" and row.get("group") == group:
            return row["count"]
    return None


def final_value(trace):
    for row in trace["stages"].get("final_output_witness", []):
        item = row["item"]
        if len(item) == 1 and WITNESS_COLUMN in item and item[WITNESS_COLUMN] is None:
            return row["count_reference"], row["count_group"]
    return None, None


def main(path):
    trace = load(path)
    witness = trace["witness_signature"]
    print(f"channel: {trace['channel']}   witness: {witness}")
    print(f"totals : {json.dumps(trace['totals'])}\n")

    final_ref, final_grp = final_value(trace)
    truth_ref = independent_value(trace, "reference")
    truth_grp = independent_value(trace, witness)
    rows = [
        ("level1_independent (groupBy)", truth_ref, truth_grp),
        (
            "level1_collect (off the RDD)",
            stage_value(trace, "level1_collect", "reference"),
            stage_value(trace, "level1_collect", witness),
        ),
        (
            "before_final_filtering",
            stage_value(trace, "before_final_filtering", "reference"),
            stage_value(trace, "before_final_filtering", witness),
        ),
        ("final_output", final_ref, final_grp),
        ("production published (Aug 17)", PRODUCTION_REFERENCE, PRODUCTION_WITNESS_GROUP),
    ]
    print(f"{'stage':32} {'count_reference':>16} {'count_group':>13}")
    for label, reference, group in rows:
        show = lambda v: "-" if v is None else f"{v:.0f}"  # noqa: E731
        print(f"{label:32} {show(reference):>16} {show(group):>13}")

    print("\nverdict:")
    collect_ref = stage_value(trace, "level1_collect", "reference")
    saved_ref = stage_value(trace, "before_final_filtering", "reference")
    totals = trace.get("totals", {})
    group_total = totals.get(witness)

    def differs(a, b):
        return a is not None and b is not None and abs(a - b) > 0.5

    if truth_ref is None:
        print("  no groupBy reference recorded, so there's nothing to compare against.")
        print("  Check that note_independent_counts ran; without it the stages below")
        print("  only agree with each other, which proves nothing.")
    elif collect_ref is None:
        print("  the witness item never reached level1_collect. Either it was filtered by")
        print("  MIN_COUNT, or the column wasn't in the level 1 column list. Check")
        print("  '1 CANDIDATES' in the driver log.")
    elif differs(collect_ref, truth_ref):
        print(f"  wrong already at collect(): {collect_ref:.0f} against {truth_ref:.0f}")
        print("  from groupBy on the same dfReference in the same run. The dataframe is")
        print("  fine and the flatMap/reduceByKey expression is where it breaks. That")
        print("  expression keys on frozenset([(key, p[key])]) over every column at once.")
    elif differs(saved_ref, truth_ref):
        print(f"  correct at collect() ({collect_ref:.0f}) but wrong in saved_counts")
        print(f"  ({saved_ref:.0f}). Something overwrote it; see the writes below.")
    elif differs(final_ref, truth_ref):
        print(f"  correct in saved_counts ({saved_ref:.0f}) but wrong in the output")
        print(f"  ({final_ref:.0f}). The final filtering is rewriting the counts.")
    else:
        print(f"  every stage agrees at {truth_ref:.0f}/{truth_grp}, so this run did not")
        print("  reproduce the bug. Note the window is utcnow()-5 and so differs from the")
        print("  Aug 17 run; compare the versions and totals above against that run.")

    # An impossible value is worth calling out on its own: a group count can never exceed
    # the number of crashes in the group. This is what made the published output suspect.
    if group_total and final_grp is not None and final_grp > group_total + 0.5:
        print()
        print(f"  IMPOSSIBLE: final count_group {final_grp:.0f} exceeds the signature's")
        print(f"  own total of {group_total} crashes.")

    writes = [
        w for w in trace.get("writes", []) if _is_witness_item(w["items"])
    ]
    print(f"\nwrites to the witness item: {len(writes)}")
    for write in writes:
        previous = "new" if write["previous"] is None else f"{write['previous']:.0f}"
        flag = ""
        if write["previous"] is not None and write["count"] < write["previous"]:
            flag = "   <-- overwritten with a SMALLER value"
        print(f"  {write['df']:34} {previous:>10} -> {write['count']:<10.0f}{flag}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1])
