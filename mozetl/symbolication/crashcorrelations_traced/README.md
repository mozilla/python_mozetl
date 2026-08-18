# crashcorrelations_traced

TEMPORARY. A vendored copy of https://github.com/marco-c/crashcorrelations at commit
`5684259`, plus diagnostic hooks. Delete this directory once the NULL undercount in
`top_signatures_correlations` is understood.

## Why this exists

The production job clones upstream at runtime:

    os.system("git clone https://github.com/marco-c/crashcorrelations.git")

So the counting code that runs in production is upstream master, not the copy in
`../crashcorrelations/`. That makes the inside of `find_deviations` unobservable: there is
nowhere to put a hook. The external tracer (`../count_trace_prod.py`) worked around it by
measuring the dataframe handed to `find_deviations`, and showed the read is faithful, all
four measurement paths agreeing with BigQuery. But `find_deviations` doesn't count on that
dataframe. It counts on

    dfReference = drop_unneeded(augment(reference)).cache()

which `augment()` rebuilds, including renaming `dom_ipc_enabled` to `e10s_enabled`. Measuring
that step needs code inside the library, hence this copy.

## What was changed

Three additive hooks in `crash_deviations.py`, all no-ops unless the driver sets
`crash_deviations.TRACE`. Nothing else differs from upstream. `plot.py` was dropped because
it imports matplotlib, which isn't installed on the cluster, and nothing imports it.

To see the diff:

    git clone https://github.com/marco-c/crashcorrelations.git /tmp/cc
    diff -u /tmp/cc/crash_deviations.py crash_deviations.py

| location | hook | what it answers |
| --- | --- | --- |
| module level | `TRACE = None` | the switch; unset means zero overhead |
| `save_count` | `note_write` | every write to `saved_counts` with the value it replaced, so an overwrite with a smaller number is visible |
| level 1 `collect()` | `note_rdd` + `note_independent_counts` | whether the counts are already wrong coming off the RDD, and whether `groupBy` on the same `dfReference` disagrees |
| before final filtering | `note_saved_counts` | whether a count that was right after level 1 is wrong by the time filtering starts |

`save_count` is the only write path into `saved_counts`, so `note_write` sees every change.

## Reading the result

The trace lands at `gs://<bucket>/count-trace-inlib/release/trace.json`. Then:

    python ../read_count_trace.py gs://<bucket>/count-trace-inlib/release/trace.json

Interpretation, in the order the reader checks:

- wrong at `level1_collect` but right in `level1_independent`: the level 1
  `flatMap`/`reduceByKey` expression is the bug, since `groupBy` on the same dataframe
  disagrees with it.
- wrong in both: `dfReference` itself is wrong, so the fault is in `augment()` or
  `drop_unneeded`, not in the counting.
- right at collect, wrong in `before_final_filtering`: something overwrote it. The `writes`
  list names it.
- right there, wrong in the output: the final filtering.
