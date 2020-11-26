#!/bin/bash

# Create cluster for testing

if [[ -z $AWS_ACCESS_KEY_ID ]]; then
    echo '$AWS_ACCESS_KEY_ID not defined'
    exit 1
fi
if [[ -z $AWS_SECRET_ACCESS_KEY ]]; then
    echo '$AWS_SECRET_ACCESS_KEY not defined'
    exit 1
fi

gcloud dataproc clusters create symbolication \
    --image-version=1.5 \
    --region=us-central1 \
    --metadata='PIP_PACKAGES=boto3==1.16.20 scipy==1.5.4' \
    --num-workers=2 \
    --worker-machine-type='n2-standard-4' \
    --properties "core:fs.s3.awsAccessKeyId=$AWS_ACCESS_KEY_ID,core:fs.s3.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY,spark-env:AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,spark-env:AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY" \
    --initialization-actions='gs://dataproc-initialization-actions/python/pip-install.sh'
