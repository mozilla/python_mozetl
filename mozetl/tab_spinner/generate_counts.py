import boto3
import json
from datetime import datetime, timedelta

from moztelemetry.dataset import Dataset

from .utils import get_short_and_long_spinners


def run_spinner_etl(sc):
    sample_size = 1.0

    start_date = (datetime.today() - timedelta(days=180)).strftime("%Y%m%d")
    end_date = datetime.today().strftime("%Y%m%d")

    def appBuildId_filter(b):
        return b >= start_date and (b.startswith(end_date) or b < end_date)

    print("Start Date: {}, End Date: {}".format(start_date, end_date))

    results = {}

    pings = (
        Dataset.from_source("telemetry")
        .where(docType="main")
        .where(submissionDate=lambda b: b >= start_date)
        .where(appBuildId=appBuildId_filter)
        .where(appUpdateChannel="nightly")
        .records(sc, sample=sample_size)
    )

    results = get_short_and_long_spinners(pings)

    s3_client = boto3.client("s3")
    filename = "severities_by_build_id_nightly.json"
    with open(filename, "w") as f:
        f.write(json.dumps(results, ensure_ascii=False))

    s3_client.upload_file(
        filename,
        "telemetry-public-analysis-2",
        "spinner-severity-generator/data/{}".format(filename),
    )
