import boto3
import json
import pandas as pd
from datetime import datetime, timedelta

from moztelemetry.dataset import Dataset

from .utils import get_short_and_long_spinners


# numpy.float64 is not JSON serializable, so an encoder is provided for
# serialization. In this particular case, the values are within a pandas series.
# See: https://stackoverflow.com/a/47626762
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        else:
            return json.JSONEncoder.default(self, obj)


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
        f.write(json.dumps(results, ensure_ascii=False, cls=CustomEncoder))

    s3_client.upload_file(
        filename,
        "telemetry-public-analysis-2",
        "spinner-severity-generator/data/{}".format(filename),
    )
