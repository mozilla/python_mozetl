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
* `crash_deviations.py` line 322: seeded `set.union` with an empty set. Called on the class
  it needs at least one argument, so a channel with no groups above the support threshold
  crashed the job. This took down production on 2026-08-13. The driver also skips empty
  channels now. See "Empty channel crash" in `../migration_plan.md`.

* `crash_deviations.py`: `create_get_addon_name_udf` now declares `BooleanType()` instead of
  `StringType()`, which is what the function actually returns. Under `StringType` the values
  arrive as `'true'`/`'false'` and the `elem_val is False` test in `ignore_rule` never
  matches.
* `crash_deviations.py`: narrowed the bare `except:` in `get_arch` to
  `(ValueError, TypeError)`.
* `crash_deviations.py`: the `else` branch of the modules block now sets `module_ids = {}`.
  `priors_graph` reads it unconditionally, so a dataset with no `json_dump` column raised
  `NameError`.

The `Row` type discriminator after `reduceByKey`, `dropDuplicates` on the exploded module
column, and the `crash_date` handling were all checked against PySpark 3.5 on Python 3.11 and
work unchanged. See the step 7 list in `../migration_plan.md`.
