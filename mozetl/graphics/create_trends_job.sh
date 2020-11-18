#!/bin/bash

gcloud dataproc jobs submit pyspark graphics_telemetry_trends.py \
    --cluster=graphics-spark-test \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
