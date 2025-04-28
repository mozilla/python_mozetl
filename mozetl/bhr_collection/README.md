# bhr collection

## Running locally

This job can be run locally with pyspark. 

### Dependencies

The job was built for [Dataproc 1.5](https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.5)
which uses Java 8, Python 3.7, and Spark 2.4.8.  Additional Python dependencies are in `requirements.txt`.

The [`spark-bigquery-connector`](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) is also needed.
The jar file can be downloaded from https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar
(linked [here](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example)).
If the job complains about Scala version compatibility, try https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest_2.12.jar.

### Limiting data size

Using a large sample will likely cause the client to run out of memory so using the `--sample-size`
argument is required.  e.g. `--sample-size 0.0002` gives a 0.02% sample which should be a couple thousand pings.
Sampling is done in BigQuery with the 
[FARM_FINGERPRINT](https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#farm_fingerprint)
function on the document id.

### Executing the job

GCP authentication is needed because Bigquery is accessed: `gcloud auth application-default login`.

The job can either be run as a python script or a spark job:

```sh
python bhr_collection.py --date 2025-04-25 --bq-connector-jar=spark-bigquery-latest.jar --sample-size 0.0002
```
or
```sh
spark-submit --driver-class-path spark-bigquery-latest.jar bhr_collection.py --date 2025-04-25 --sample-size 0.0002
```

## Testing in dataproc (untested since 2020)

To run this job manually, first create the dataproc cluster using the `create_cluster.sh` script.

The job can be submitted to the cluster using gcloud:

```sh
gcloud dataproc jobs submit pyspark bhr_collection.py \
    --cluster=bhr-collection \
    --region=us-central1 \
    --jars 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar' \
    -- --date=2020-12-31 --sample-size=0.01
```

https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#connector-to-dataproc-image-compatibility-matrix