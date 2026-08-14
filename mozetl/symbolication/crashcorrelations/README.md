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
* `utils.py`: removed `upload_results`, `remove_results` and the `import boto3` they needed.
  They wrote to the S3 bucket `telemetry-public-analysis-2` and were left behind when this job
  moved to GCS; nothing called them. The import ran at module load, so every cluster had to
  install boto3 to reach `write_json`. The drivers upload with `google-cloud-storage`.

The `Row` type discriminator after `reduceByKey`, `dropDuplicates` on the exploded module
column, and the `crash_date` handling were all checked against PySpark 3.5 on Python 3.11 and
work unchanged. See the step 7 list in `../migration_plan.md`.

## Known bugs, not fixed

Both of these are live in production and in the image 2.2 port. They were found while planning
the BigQuery rewrite and deliberately left alone for now, so that the 2.2 port can be validated
against production without a third source of differences. Neither is a Spark 3 problem, so
neither blocks the image bump.

### The esr channel analyses release crashes

`versions.py` reads `FIREFOX_ESR_NEXT` from product-details (`'153.0esr'`), strips the trailing
`esr` at line 39, and then `download_data.get_versions` sends esr down the same branch as
release. The result is that both channels ask for the same version list:

```
release -> ['153.0', '153.0.1', '153.0.3', '153.0.4']
esr     -> ['153.0', '153.0.1', '153.0.3', '153.0.4']
```

The crash data keeps the suffix (`140.13.0esr`, `153.0esr`), so the esr filter matches no esr
rows at all and collides with release instead. Over the 5 days ending 2026-08-14 that hid about
42k esr crashes, and the Correlations tab for esr on Crash Stats shows release correlations.
`all.json.gz` reporting the same total for `esr` and `release` is the visible symptom.

Note this depends on `FIREFOX_ESR_NEXT` being populated and sharing a major version with
release, which is the case during an ESR transition. When it's empty the code falls back to
`FIREFOX_ESR` and there's no collision, so the symptom comes and goes.

Fix is to keep the suffix when building the esr version list. Left for later because esr is not
a high priority channel.

### Addon correlations have never worked

The addon counting pass reports 0 on every channel, and it isn't because there are no addons.
Line 238 explodes `reference['addons']['list']`, which yields `Row(element='guid:version')`
rather than the string, because `addons.list` is a repeated `STRUCT<element STRING>`. The next
line passes that `Row` to `get_addon_name`, which tests `':' in addon_string`. On a `Row` the
`in` operator checks membership against the row's values, not substrings, so it is always
False, the function returns `None`, and the following
`.filter(lambda s_a: s_a[1] is not None)` drops every addon. Reproduced directly on PySpark 3.5.

So `all_addons` is always empty, `augment()` adds no addon columns, the addon entries in
`priors_graph` are dead, the `-version` handling in `ignore_rule` never fires, and
`addon_related_signatures.json.gz` is always `{}`. That last file is a documented output of the
job and is empty in both production and the 2.2 test output.

The data is there and clears the support threshold comfortably. On beta over the 5 days ending
2026-08-14, `webcompat@mozilla.org` has 0.51 support in the channel and 0.77 in its best
signature group, with six more values surviving, against the Spark job's zero.

Fix is `a['element']` rather than the `Row`. Worth checking at the same time whether
`addons.get_addon_name` still resolves: it calls a `services.addons.mozilla.org` API that may no
longer exist, with a bare `except` that would silently return the raw guid.
