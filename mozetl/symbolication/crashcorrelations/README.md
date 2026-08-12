# crashcorrelations (vendored)

Vendored copy of https://github.com/marco-c/crashcorrelations at commit
`5684259becee9561c31fbdf545e5ec2c93bdd639` (2020-05-25, the current tip of `master`).

Used by `top_signatures_correlations_v2_2.py`. The original job cloned this repo at runtime
and added it to `sys.path`, which meant every run executed whatever was on `master` with no
review and no integrity check. It's vendored here instead so the code is pinned, reviewable,
and modifiable, which matters because it needs changes to run on Spark 3.

Upstream is unmaintained (no commits since May 2020) and the job is scheduled for retirement
in H1 2027, so this copy is not expected to be resynced with upstream.

## What was copied

`utils.py`, `versions.py`, `download_data.py`, `addons.py`, `app_notes.py`,
`gfx_critical_errors.py`, `crash_deviations.py`, and an empty `__init__.py`.

`comments.py` and `plot.py` were left out. `comments.py` held the top words analysis, which
read `user_comments`, a column that has been entirely null for years, so it never produced
anything. `plot.py` is only used from notebooks.

## Local changes

* `crash_deviations.py`: dropped `SQLContext` from the `pyspark.sql` import. It was never
  used and `SQLContext` no longer exists in Spark 3, so the import failed outright.

Further Spark 3 fixes are expected here. See the step 6 list in `../migration_plan.md` for
what to look at, in particular the use of `Row` as a type discriminator after `reduceByKey`
and a UDF that declares `StringType` while returning a bool.
