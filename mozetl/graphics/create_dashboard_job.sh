#!/bin/bash

# Test the graphics_telemetry_dashboard.py job
# This assumes a cluster was created via create_cluster.sh

gcloud dataproc jobs submit pyspark graphics_telemetry_dashboard.py \
    --cluster=graphics-spark-test \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
