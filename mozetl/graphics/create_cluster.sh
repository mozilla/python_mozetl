#!/bin/bash

# This scripts provisions a dataproc cluster to be used for testing
# An AWS key with write permissions to the telemetry-public-analysis-2 S3 bucket is required

if [[ -z $AWS_ACCESS_KEY_ID ]]; then
    echo '$AWS_ACCESS_KEY_ID not defined'
    exit 1
fi
if [[ -z $AWS_SECRET_ACCESS_KEY ]]; then
    echo '$AWS_SECRET_ACCESS_KEY not defined'
    exit 1
fi

gcloud dataproc clusters create graphics-test-cluster \
    --image-version=1.5 \
    --region=us-central1 \
    --metadata='PIP_PACKAGES=git+https://github.com/mozilla/python_moztelemetry.git@v0.10.2#egg=python-moztelemetry git+https://github.com/FirefoxGraphics/telemetry.git#egg=pkg&subdirectory=analyses/bigquery_shim boto3==1.16.20 six==1.15.0' \
    --num-workers=2 \
    --worker-machine-type=n2-highmem-4 \
    --properties "core:fs.s3.awsAccessKeyId=$AWS_ACCESS_KEY_ID,core:fs.s3.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,spark-env:AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,spark-env:AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    --initialization-actions='gs://dataproc-initialization-actions/python/pip-install.sh'
