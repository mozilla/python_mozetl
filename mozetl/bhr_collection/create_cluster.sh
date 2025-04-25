#!/bin/bash

# Create cluster for testing

gcloud dataproc clusters create bhr-collection \
    --image-version=1.5 \
    --region=us-central1 \
    --metadata='PIP_PACKAGES=click==7.1.2 google-cloud-storage==2.7.0' \
    --num-workers=5 \
    --worker-machine-type='n2-highmem-4' \
    --initialization-actions='gs://dataproc-initialization-actions/python/pip-install.sh'
