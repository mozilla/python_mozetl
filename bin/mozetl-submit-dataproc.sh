#!/bin/bash

# Test a pyspark job on dataproc
# This script assumes a cluster was already created with the given name
# and the correct configuration
# See mozetl/graphics/create_cluster.sh for an example

# Example Usage: ./mozetl-submit-dataproc graphics-test-cluster mozetl/graphics/graphics_telemetry_dashboard.py

cd "$(dirname "$0")/.."

gcloud dataproc jobs submit pyspark $2 \
    --cluster=$1 \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
