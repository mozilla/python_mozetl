"""TEMPORARY. Instrumentation to find out why production undercounts NULL-valued items.

Production reports impossible numbers for items whose value is NULL. For signature
'EdgePool::Iterator::operator*' it reports count_group 54 for {'moz_crash_reason': None},
when that signature has 121 crashes and all 121 have the field NULL. count_reference is
wrong the same way, 7,183 against a true 31,485. Non-NULL counts on the same signature are
exact. Ten hypotheses were tested locally and all were ruled out, including running the
same counting pass on both Spark 2.4 and 3.5, so the fault is something about the real run
that a local reproduction doesn't recreate.

This narrows it to one of three places by recording the same numbers at three points:

  level1_collect         straight off the level 1 collect(), before saved_counts exists.
  level1_independent     the same counts computed a second way, with groupBy instead of
                         the flatMap/reduceByKey, in the same run on the same dataframe.
  before_final_filtering saved_counts as it stands after all the counting passes.

Read the results like this:

  collect() already wrong        -> Spark or the dataframe. Compare against
                                    level1_independent, which shares the dataframe but not
                                    the RDD path, to tell those apart.
  collect() right, saved wrong   -> something between, i.e. save_results or an overwrite.
                                    The `writes` list shows every write in order with its
                                    previous value, so an overwrite is visible directly.
  saved right, output wrong      -> the final filtering.

Remove this module and the four TRACE hooks in crashcorrelations/crash_deviations.py once
the cause is known.

Usage from the driver, after importing crash_deviations:

    from mozetl.symbolication import count_trace
    crash_deviations.TRACE = count_trace.Tracer(channel)
    ...
    crash_deviations.TRACE.flush(bucket="benwu-correlations-output")

Writes to a separate prefix, so it cannot disturb what Crash Stats reads.
"""

import json


# Columns to trace. The first group are ones production gets wrong; process_type and
# platform are controls that production gets right, so if they also drift the problem is
# broader than NULL handling.
TRACKED_COLUMNS = frozenset({
    "moz_crash_reason",
    "shutdown_progress",
    "startup_crash",
    "address",
    "reason",
    "cpu_microcode_version",
    "dom_ipc_enabled",
    "e10s_enabled",
    "process_type",
    "platform",
})

# The clearest case: 121 crashes, all 121 with moz_crash_reason NULL, production says 54.
WITNESS_SIGNATURE = "EdgePool::Iterator::operator*"

TRACE_PREFIX = "count-trace"


def _describe(candidate):
    """JSON-safe itemset, keeping None distinct from the string 'None'."""
    return [
        {
            "column": column,
            "value": None if value is None else str(value),
            "value_type": type(value).__name__,
            "is_none": value is None,
        }
        for column, value in sorted(candidate, key=lambda p: (p[0], repr(p[1])))
    ]


def _tracked(candidate):
    return any(column in TRACKED_COLUMNS for column, _ in candidate)


class Tracer(object):
    def __init__(self, channel):
        self.channel = channel
        self.stages = {}
        self.writes = []
        self.totals = {}

    # -- hooks called from crash_deviations ---------------------------------------

    def note_write(self, candidate, count, df, previous=None):
        """Every save_count write, with the value it replaced.

        An overwrite with a smaller number is the signature of a clobbering bug and shows
        up here as two entries for the same (df, itemset) with different counts.
        """
        if not _tracked(candidate):
            return
        if df != "reference" and df != WITNESS_SIGNATURE:
            return
        self.writes.append({
            "df": df,
            "count": float(count),
            "previous": None if previous is None else float(previous),
            "items": _describe(candidate),
        })

    def note_rdd(self, name, collected):
        """Counts straight off a collect(), before saved_counts is involved."""
        rows = []
        for key, count in collected:
            if isinstance(key, frozenset):
                candidate, group = key, "reference"
            else:
                group, candidate = key
            if not _tracked(candidate):
                continue
            if group != "reference" and group != WITNESS_SIGNATURE:
                continue
            rows.append({
                "group": group,
                "count": float(count),
                "items": _describe(candidate),
            })
        self.stages[name] = rows

    def note_independent_counts(self, name, df, columns):
        """The same counts a second way, via groupBy rather than flatMap/reduceByKey.

        Both run in the same job on the same dataframe, so a disagreement points at the RDD
        path while agreement points at the dataframe or the read.
        """
        rows = []
        for column in columns:
            if column not in TRACKED_COLUMNS:
                continue
            try:
                counts = (df.groupBy(column).count()
                          .rdd.map(lambda r: (r[0], r[1])).collect())
            except Exception as error:  # noqa: BLE001 - diagnostic only
                rows.append({"column": column, "error": repr(error)})
                continue
            for value, count in counts:
                rows.append({
                    "column": column,
                    "value": None if value is None else str(value),
                    "is_none": value is None,
                    "count": int(count),
                    "method": "groupBy",
                })
            # And the witness signature's own slice of the same column.
            try:
                witness = (df.filter(df["signature"] == WITNESS_SIGNATURE)
                           .groupBy(column).count()
                           .rdd.map(lambda r: (r[0], r[1])).collect())
            except Exception as error:  # noqa: BLE001 - diagnostic only
                rows.append({"column": column, "witness_error": repr(error)})
                continue
            for value, count in witness:
                rows.append({
                    "column": column,
                    "value": None if value is None else str(value),
                    "is_none": value is None,
                    "count": int(count),
                    "method": "groupBy",
                    "group": WITNESS_SIGNATURE,
                })
        self.stages[name] = rows

    def note_saved_counts(self, name, saved_counts, group_names, total_reference,
                          total_groups):
        """Snapshot saved_counts for the tracked items."""
        self.totals = {
            "reference": total_reference,
            WITNESS_SIGNATURE: total_groups.get(WITNESS_SIGNATURE),
            "group_count": len(group_names),
            "witness_in_groups": WITNESS_SIGNATURE in group_names,
        }
        rows = []
        for df in ("reference", WITNESS_SIGNATURE):
            for candidate, count in saved_counts.get(df, {}).items():
                if _tracked(candidate):
                    rows.append({
                        "group": df,
                        "count": float(count),
                        "size": len(candidate),
                        "items": _describe(candidate),
                    })
        self.stages[name] = rows

    def note_results(self, results, total_reference, total_groups):
        """The final output rows for the witness signature.

        This is what ends up in the JSON Crash Stats reads, so comparing it against
        before_final_filtering says whether the filtering changed the numbers.
        """
        self.totals.setdefault("reference", total_reference)
        self.totals.setdefault(WITNESS_SIGNATURE, total_groups.get(WITNESS_SIGNATURE))
        rows = []
        for result in results.get(WITNESS_SIGNATURE, []):
            rows.append({
                "item": {
                    key: (None if value is None else str(value))
                    for key, value in result["item"].items()
                },
                "count_reference": result["count_reference"],
                "count_group": result["count_group"],
                "has_prior": result.get("prior") is not None,
            })
        self.stages["final_output_witness"] = rows

    # -- output -------------------------------------------------------------------

    def flush(self, bucket, prefix=TRACE_PREFIX):
        from google.cloud import storage

        payload = {
            "channel": self.channel,
            "witness_signature": WITNESS_SIGNATURE,
            "tracked_columns": sorted(TRACKED_COLUMNS),
            "totals": self.totals,
            "stages": self.stages,
            "writes": self.writes,
            "write_count": len(self.writes),
        }
        path = "{}/{}/trace.json".format(prefix, self.channel)
        storage.Client().bucket(bucket).blob(path).upload_from_string(
            json.dumps(payload, indent=1), content_type="application/json"
        )
        print("count_trace: wrote gs://{}/{} ({} writes, stages: {})".format(
            bucket, path, len(self.writes), sorted(self.stages)))
