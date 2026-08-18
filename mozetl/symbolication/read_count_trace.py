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

# Verified with COUNTIF over the same window: 31,485 rows have moz_crash_reason NULL, and
# signature EdgePool::Iterator::operator* has 121 crashes, all 121 with it NULL.
TRUTH_REFERENCE = 31485
TRUTH_WITNESS_GROUP = 121

# What production published for the same item, which is what we're explaining.
PRODUCTION_REFERENCE = 7183
PRODUCTION_WITNESS_GROUP = 54


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
    rows = [
        ("truth (COUNTIF)", TRUTH_REFERENCE, TRUTH_WITNESS_GROUP),
        (
            "level1_collect (off the RDD)",
            stage_value(trace, "level1_collect", "reference"),
            stage_value(trace, "level1_collect", witness),
        ),
        (
            "level1_independent (groupBy)",
            independent_value(trace, "reference"),
            independent_value(trace, witness),
        ),
        (
            "before_final_filtering",
            stage_value(trace, "before_final_filtering", "reference"),
            stage_value(trace, "before_final_filtering", witness),
        ),
        ("final_output", final_ref, final_grp),
        ("production published", PRODUCTION_REFERENCE, PRODUCTION_WITNESS_GROUP),
    ]
    print(f"{'stage':32} {'count_reference':>16} {'count_group':>13}")
    for label, reference, group in rows:
        show = lambda v: "-" if v is None else f"{v:.0f}"  # noqa: E731
        print(f"{label:32} {show(reference):>16} {show(group):>13}")

    print("\nverdict:")
    collect_ref = stage_value(trace, "level1_collect", "reference")
    saved_ref = stage_value(trace, "before_final_filtering", "reference")
    if collect_ref is None:
        print("  the witness item never reached level1_collect. Either it was filtered by")
        print("  MIN_COUNT, or the column wasn't in the level 1 column list. Check")
        print("  '1 CANDIDATES' in the driver log and the requiredColumns pushdown.")
    elif abs(collect_ref - TRUTH_REFERENCE) > 0.5:
        independent = independent_value(trace, "reference")
        print(f"  wrong already at collect() ({collect_ref:.0f} vs {TRUTH_REFERENCE}).")
        if independent is not None and abs(independent - TRUTH_REFERENCE) <= 0.5:
            print(f"  but groupBy on the same dataframe gives {independent:.0f}, which is")
            print("  correct. So the dataframe is fine and the flatMap/reduceByKey path is")
            print("  where it breaks.")
        else:
            print(f"  and groupBy gives {independent}, also wrong, so the dataframe or the")
            print("  read is the problem rather than the RDD path.")
    elif saved_ref is not None and abs(saved_ref - TRUTH_REFERENCE) > 0.5:
        print(f"  correct at collect() ({collect_ref:.0f}) but wrong in saved_counts")
        print(f"  ({saved_ref:.0f}). Something between overwrote it; see the writes below.")
    elif final_ref is not None and abs(final_ref - TRUTH_REFERENCE) > 0.5:
        print(f"  correct in saved_counts ({saved_ref:.0f}) but wrong in the output")
        print(f"  ({final_ref:.0f}). The final filtering is rewriting the counts.")
    else:
        print("  every stage matches the truth, so this run did not reproduce the bug.")
        print("  Compare the run's window and versions against the production run.")

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
