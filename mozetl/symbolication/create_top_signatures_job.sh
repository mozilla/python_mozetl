#!/bin/bash

gcloud dataproc jobs submit pyspark top_signatures_correlations.py \
    --cluster=symbolication \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
