#!/bin/bash

gcloud dataproc jobs submit pyspark bhr_collection.py \
    --cluster=bhr_collection \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar'
