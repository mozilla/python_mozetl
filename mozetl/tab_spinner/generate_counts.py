import ujson as json

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from moztelemetry.dataset import Dataset
from .utils import get_short_and_long_spinners

def run_spinner_etl():
    spark = SparkSession
             .builder
             .appName("tab_spinner_etl")
             .getOrCreate()

    sc = spark.sparkContext()

    nightly_build_channels = ["nightly", "aurora"]
    sample_size = 1.0

    probe_available = datetime(2016, 9, 8)
    days_to_look_back = 180
    start_date = max(probe_available, datetime.today() - timedelta(days=days_to_look_back)).strftime("%Y%m%d")
    end_date = datetime.today().strftime("%Y%m%d")

    print "Start Date: {}, End Date: {}".format(start_date, end_date)

    build_results = {}

    for build_type in nightly_build_channels:
        # Bug 1341340 - if we're looking for pings from before 20161012, we need to query
        # old infra.
        old_infra_pings = Dataset.from_source("telemetry-oldinfra") \
            .where(docType='main') \
            .where(submissionDate=lambda b: b < "20161201") \
            .where(appBuildId=lambda b: (b.startswith(start_date) or b > start_date) and (b.startswith(end_date) or b < end_date)) \
            .where(appUpdateChannel=build_type) \
            .records(sc, sample=sample_size)

        new_infra_pings = Dataset.from_source("telemetry") \
            .where(docType='main') \
            .where(submissionDate=lambda b: (b.startswith("20161201") or b > "20161201")) \
            .where(appBuildId=lambda b: (b.startswith(start_date) or b > start_date) and (b.startswith(end_date) or b < end_date)) \
            .where(appUpdateChannel=build_type) \
            .records(sc, sample=sample_size)

        pings = old_infra_pings.union(new_infra_pings)
        build_results[build_type] = get_short_and_long_spinners(pings)

    for result_key, results in build_results.iteritems():
        filename = "./output/severities_by_build_id_%s.json" % result_key
        results_json = json.dumps(results, ensure_ascii=False)

        with open(filename, 'w') as f:
            f.write(results_json)

    spark.stop()

if __name__ == '__main__':
    run_spinner_etl()
