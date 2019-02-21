# Firefox Telemetry Python ETL

[![CircleCI](https://circleci.com/gh/mozilla/python_mozetl.svg?style=svg)](https://circleci.com/gh/mozilla/python_mozetl)
[![codecov](https://codecov.io/gh/mozilla/python_mozetl/branch/master/graph/badge.svg)](https://codecov.io/gh/mozilla/python_mozetl)

This repository is a collection of ETL jobs for Firefox Telemetry.

# Benefits

Jobs committed to python_mozetl can be **scheduled via
[airflow](https://github.com/mozilla/telemetry-airflow)
or
[ATMO](https://analysis.telemetry.mozilla.org/)**.
We provide a **testing suite** and **code review**, which makes your job more maintainable.
Centralizing our jobs in one repository allows for
**code reuse** and **easier collaboration**.

There are a host of benefits to moving your analysis out of a Jupyter notebook
and into a python package.
For more on this see the writeup at
[cookiecutter-python-etl](https://github.com/harterrt/cookiecutter-python-etl/blob/master/README.md#benefits).

# Tests
## Dependencies
First install the necessary runtime dependencies -- snappy and the java runtime
environment. These are used for the `pyspark` package. In ubuntu:
```bash
$ sudo apt-get install libsnappy-dev openjdk-8-jre-headless
```

## Calling the test runner
Run tests by calling `tox` in the root directory.

Arguments to `pytest` can be passed through tox using `--`.
```
tox -- -k test_main.py # runs tests only in the test_main module
```

Tests are configured in [tox.ini](tox.ini)

# Manual Execution
## ATMO

The first method of manual execution is the `mozetl-submit.sh` script located in `bin`.
This script is used with the `EMRSparkOperator` in `telemetry-airflow` to schedule execution of `mozetl` jobs.
It may be used with [ATMO](https://analysis.telemetry.mozilla.org/) to manually test jobs.

In an SSH session with an ATMO cluster, grab a copy of the script:
```
$ wget https://raw.githubusercontent.com/mozilla/python_mozetl/master/bin/mozetl-submit.sh
```
Push your code to your own fork, where the job has been added to `mozetl.cli`. Then run it.

```bash
$ ./mozetl-submit.sh \
    -p https://github.com/<USERNAME>/python_mozetl.git \
    -b <BRANCHNAME> \
    <COMMAND> \
        --first-argument foo \
        --second-argument bar
```

See comments in `bin/mozetl-submit.sh` for more details.

## Databricks

Jobs may also be executed on [Databricks](https://dbc-caf9527b-e073.cloud.databricks.com/).
They are scheduled via the `MozDatabricksSubmitRunOperator` in `telemetry-airflow`.

This script runs on your local machine and submits the job to a remote spark executor.
First, generate an API token in the User Settings page in Databricks.
Then run the script.

```bash
python bin/mozetl-databricks.py \
    --git-path https://github.com/<USERNAME>/python_mozetl.git \
    --git-branch <BRANCHNAME> \
    --token <TOKEN>  \
    <COMMAND> \
        --first-argument foo \
        --second-argument bar
```

Run `python bin/mozetl-databricks.py --help` for more options, including increasing the number of workers and using python 3.
Refer to this [pull request](https://github.com/mozilla/python_mozetl/pull/296) for more examples.

It is also possible to use this script for external mozetl-compatible modules by setting the `--git-path` and `--module-name` options appropriately.
See this [pull request](https://github.com/mozilla/python_mozetl/pull/316) for more information about building a mozetl-compatible repository that can be scheduled on Databricks.


# Scheduling

You can schedule your job on either
[ATMO](https://analysis.telemetry.mozilla.org/)
or
[airflow](https://github.com/mozilla/telemetry-airflow).

Scheduling a job on ATMO is easy and does not require review,
but is less maintainable.
Use ATMO to schedule jobs you are still prototyping
or jobs that have a limited lifespan.

Jobs scheduled on Airflow will be more robust.

* Airflow will automatically retry your job in the event of a failure.
* You can also alert other members of your team when jobs fail,
  while ATMO will only send an email to the job owner.
* If your job depends on other datasets,
  you can identify these dependencies in Airflow.
  This is useful if an upstream job fails.

## ATMO

To schedule a job on ATMO, take a look at the
[load_and_run notebook](scheduling/load_and_run.ipynb).
This notebook clones and installs the python_mozetl package.
You can then run your job from the notebook.

## Airflow

To schedule a job on Airflow,
you'll need to add a new Operator to the DAGs and provide a shell script for running your job.
Take a look at 
[this example shell script](https://github.com/mozilla/telemetry-airflow/blob/master/jobs/topline_dashboard.sh).
and
[this example Operator](https://github.com/mozilla/telemetry-airflow/blob/master/dags/topline.py#L31)
for templates.

# Early Stage ETL Jobs

We usually require tests before accepting new ETL jobs.
If you're still prototyping your job,
but you'd like to move your code out of a Jupyter notebook
take a look at
[cookiecutter-python-etl](https://github.com/harterrt/cookiecutter-python-etl).

This tool will initialize a new repository
with all of the necessary boilerplate for testing and packaging.
In fact, this project was created with
[cookiecutter-python-etl](https://github.com/harterrt/cookiecutter-python-etl).
