#!/bin/bash

# We use jupyter by default, but here we want to use python
unset PYSPARK_DRIVER_PYTHON

# Clone, install, and run
pip install git+https://github.com/mozilla/python_mozetl.git
spark-submit scheduling/airflow.py
