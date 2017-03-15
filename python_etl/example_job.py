from datetime import date, timedelta
from pyspark import Row

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset


def get_data(sc):
    pings = Dataset.from_source("telemetry") \
        .where(docType='main') \
        .where(submissionDate=(date.today() - timedelta(1)).strftime("%Y%m%d")) \
        .where(appUpdateChannel="nightly") \
        .records(sc, sample=0.1)

    return get_pings_properties(pings, ["clientId",
                                        "environment/system/os/name"])

def ping_to_row(ping):
    return Row(client_id = ping['clientId'],
               os = ping['environment/system/os/name'])

def transform_pings(pings):
    """Take a dataframe of main pings and summarize OS share"""
    out = pings.map(ping_to_row).distinct()\
        .map(lambda x: x.os).countByValue()

    return dict(out)

def etl_job(sc, sqlContext):
    """This is the function that will be executed on the cluster"""

    results = transform_pings(get_data(sc))

    # Display results:
    total = sum(map(lambda x: x[1], results.items()))
    # Print the OS and client_id counts in descending order:
    for pair in sorted(results.items(), key=lambda x: -x[1]):
        print "OS: {:<10} Percent: {:0.2f}%"\
            .format(pair[0], float(pair[1])/total*100)
