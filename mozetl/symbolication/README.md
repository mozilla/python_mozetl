# README

This directory holds scripts that run in Airflow related to crash report data from the Mozilla crash ingestion pipeline (aka Socorro).

* https://socorro.readthedocs.org/
* https://crash-stats.mozilla.org/
* https://socorro.readthedocs.io/en/latest/telemetry_socorro_crash.html
* https://docs.telemetry.mozilla.org/datasets/other/socorro_crash/reference.html


# modules_with_missing_symbols

Uses:

* bq: `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`
* jupyter notebook: https://github.com/marco-c/missing_symbols.git

Outputs:

* email sent via AWS SES

When the crash ingestion pipeline processor processes an incoming crash report with a minidump, it runs the stackwalker rule. The stackwalker parses the minidump and downloads debug symbols files from the Mozilla Symbols Server which it uses to lookup symbols and unwind the stack.

When a crash report minidump has a stack with modules for which we have no symbols on Mozilla Symbols Server, we want to know. In some cases, we can find the binaries and extract symbols. We do this with symbols from Microsoft Windows.

The `modules_with_missing_symbols.py` script runs a Jupyter notebook from https://github.com/marco-c/missing_symbols.git which looks at the `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2` table for crash reports that had modules where the stackwalker couldn't find symbols. That table is populated with processed crash report data from the Mozilla crash ingestion pipeline.

It generates a table of missing symbols, generates an email, and sends the email using AWS SES to.


# top_signatures_correlations

Uses:

* bq: `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`
* python: https://github.com/marco-c/crashcorrelations.git

Outputs:

* correlations data available through analysis-output.telemetry.mozilla.org site

We get a lot of crash reports. Socorro generates a crash signature for each crash report which sort of buckets them by similar causes based on frames in the stack of the crashing thread. However, it helps to know what similarities the crash reports for a given crash signature have.

The `top_signatures_correlations.py` script runs a Jupyter notebook from https://github.com/marco-c/crashcorrelations.git which has a python module called `crashcorrelations`. That module has code that looks at the `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2` table for crash report data. It calculates top signatures by crash count and then for each signature, loads a bunch of crash reports and looks at various properties of those crash reports and generates correlations data. It saves this correlations data to an AWS S3 bucket (I think) that's available at:

https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/

The Crash Stats website has a crash signature view which has a Correlations tab that loads the correlations data from that site allowing Mozilla engineers to see correlations data for signatures.


## Things to know

In the past, we've had a few cases where top signatures can contain so many crash reports that the node this runs on runs out of memory. In those cases, we've dropped the `TOP_SIGNATURE_COUNT` value.

This uses pyspark and should get rewritten as a docker-etl thing.
