# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Landfill Sampler

Take a stratified sample of documents sent to ingestion from the
raw data store used for platform backfill.

Changelog:
v1 - Initial schema used for edge-validator integration
v2 - Addition of document version as a partition value
v3 - Retain whitelisted metadata fields and simplify schema
"""

import click
from moztelemetry.dataset import Dataset
from pyspark.sql import Window, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import StructType, StructField, StringType

# whitelist for fields to keep from the ingestion metadata
META_WHITELIST = {
    "Content-Length",
    "Date",
    "Host",
    "Timestamp",
    "Type",
    "User-Agent",
    "X-PingSender-Version",
    "args",
    "protocol",
    "uri",
}

# offsets into the URI specification where namespace is index 0
TELEMETRY_DOC_TYPE = 2
TELEMETRY_DOC_ID = 1
GENERIC_DOC_TYPE = 1
GENERIC_DOC_VER = 2
GENERIC_DOC_ID = 3


def extract(sc, submission_date, sample=0.01):
    landfill = (
        Dataset.from_source("landfill")
        .where(submissionDate=submission_date)
        .records(sc, sample=sample)
    )
    return landfill


# Detect document version from the payload itself.
# Should match with the logic here:
# https://github.com/mozilla-services/lua_sandbox_extensions/blob/master/moz_telemetry/io_modules/decoders/moz_ingest/telemetry.lua#L162
def _detect_telemetry_version(content):
    if "ver" in content:
        return str(content["ver"])
    if "version" in content:
        return str(content["version"])
    if "deviceinfo" in content:
        return "3"
    if "v" in content:
        return str(content["v"])
    return "1"


def _process(message):
    """Process the URI specification from the tagged metadata

    Telemetry URI Specification:
        /submit/<namespace>/<doc_id>/<doc_type>/<app_name>/<app_version>/<app_channel>/<app_build_id>
    Generic Ingestion URI Specification:
        /submit/<namespace>/<doc_type>/<doc_version>/<doc_id>
    """
    meta = {k: v for k, v in list(message["meta"].items()) if k in META_WHITELIST}

    # Parse the uri, start by setting the path relative to `/submit`
    # Some paths do not adhere to the spec, so append empty values to avoid index errors.
    path = meta["uri"].split("/")[2:] + [None, None, None, None]
    namespace = path[0]
    content = message.get("content")

    if namespace == "telemetry":
        doc_type = path[TELEMETRY_DOC_TYPE]
        doc_version = _detect_telemetry_version(content)
        doc_id = path[TELEMETRY_DOC_ID]
    else:
        doc_type = path[GENERIC_DOC_TYPE]
        doc_version = path[GENERIC_DOC_VER]
        doc_id = path[GENERIC_DOC_ID]

    return namespace, doc_type, doc_version, doc_id, meta, content


def transform(landfill, n_documents=1000):
    meta_schema = StructType(
        [StructField(k, StringType(), True) for k in META_WHITELIST]
    )

    schema = StructType(
        [
            StructField("namespace", StringType(), False),
            StructField("doc_type", StringType(), False),
            StructField("doc_version", StringType(), True),
            StructField("doc_id", StringType(), True),
            StructField("meta", meta_schema, False),
            StructField("content", StringType(), False),
        ]
    )

    documents = (
        landfill.map(_process)
        .filter(lambda x: x[0] and x[1] and x[-2] and x[-1])
        .toDF(schema)
    )

    window_spec = Window.partitionBy("namespace", "doc_type", "doc_version").orderBy(
        "doc_id"
    )

    df = (
        documents.fillna("0", "doc_version")
        .withColumn("row_id", row_number().over(window_spec))
        .where(col("row_id") <= n_documents)
        .drop("row_id")
    )

    return df


def save(submission_date, bucket, prefix, df):
    path = "s3://{}/{}/{}/submission_date_s3={}".format(
        bucket, prefix, "v3", submission_date
    )
    (
        df.write.partitionBy("namespace", "doc_type", "doc_version").json(
            path, mode="overwrite"
        )
    )


@click.command("sample-landfill")
@click.option(
    "--bucket", type=str, default="net-mozaws-prod-us-west-2-pipeline-analysis"
)
@click.option("--prefix", type=str, default="amiyaguchi/sanitized-landfill-sample")
@click.option("--submission-date", type=str, required=True)
@click.option("--sample", type=float, default=0.01)
def main(bucket, prefix, submission_date, sample):
    """Sample documents from landfill."""
    spark = SparkSession.builder.getOrCreate()
    rdd = extract(spark.sparkContext, submission_date, sample=sample)
    df = transform(rdd)
    save(submission_date, bucket, prefix, df)
