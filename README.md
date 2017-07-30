# Firefox Telemetry Python ETL

[![Build Status](https://travis-ci.org/mozilla/python_mozetl.svg?branch=master)](https://travis-ci.org/mozilla/python_mozetl)
[![codecov](https://codecov.io/gh/mozilla/python_mozetl/branch/master/graph/badge.svg)](https://codecov.io/gh/mozilla/python_mozetl)

This repository is a collection of ETL jobs for Firefox Telemetry.

# Benefits

Jobs committed to python_mozet can be **scheduled via
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

To test this package locally, it is recommended to set up the environment and execute tests within a docker container.
```
docker build -t mozetl .
./bin/run-tests.sh   # runs tests within docker container
```

A subset of tests can be specified by adding arguments to runtests.sh:
```
./bin/run-tests.sh -k tests/test_main.py # runs tests only in the test_main module
```

Tests are configured in [tox.ini](tox.ini)

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
