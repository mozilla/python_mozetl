#!/bin/bash

# Test a pyspark job on dataproc
# This script assumes a cluster was already created with the given name
# and the correct configuration
# See mozetl/graphics/create_cluster.sh for an example

# Example Usage: ./mozetl-submit-dataproc graphics-test-cluster mozetl/graphics/graphics_telemetry_dashboard.py

if [[ -z $1 ]]; then
    echo '$1 (cluster name) not defined'
    exit 1
fi
if [[ -z $2 ]]; then
    echo '$2 (job path) not defined'
    exit 1
fi

gcloud dataproc jobs submit pyspark $2 \
    --cluster=$1 \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar' \
    -- ${@:3:$#}
