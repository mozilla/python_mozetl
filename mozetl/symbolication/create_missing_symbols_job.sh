#!/bin/bash

gcloud dataproc jobs submit pyspark modules_with_missing_symbols.py \
    --cluster=symbolication \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
