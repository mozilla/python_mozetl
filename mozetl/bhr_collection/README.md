# bhr collection

To run this job manually, first create the dataproc cluster using the `create_cluster.sh` script.
This script requires `$AWS_ACCESS_KEY_ID` and `$AWS_SECRET_ACCESS_KEY` to be defined.

The job can be submitted to the cluster using gcloud:

```sh
gcloud dataproc jobs submit pyspark bhr_collection.py \
    --cluster=bhr-collection \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar' \
    -- --date=2020-12-31 --sample-size=0.01
```
