import click
import json
import glob
from .summarize_json import *
from click_datetime import Datetime
from datetime import datetime, timedelta
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

@click.command()
@click.option('--start_date',
              type=Datetime(format='%Y%d%m'),
              required=True,
              help='Start date (e.g. yyyy/mm/dd)')
@click.option('--end_date',
              type=Datetime(format='%Y%d%m'),
              default=None,
              help='End date (e.g. yyyy/mm/dd)')
@click.option('--bucket',
              required=True,
              help='Output bucket for JSON data')
def main(start_date, end_date, bucket):
    # Set default end date to a week after start date if end date not provided
    if not end_date:
        end_date = start_date + timedelta(days=6)

    spark = (SparkSession
         .builder
         .appName("hardware_report_dashboard")
         .enableHiveSupport()
         .getOrCreate())

    # Generate the report for the desired period.
    report = generate_report(start_date, end_date, spark)
    serialize_results(report)
    # Fetch the previous data from S3 and save it locally.
    fetch_previous_state("hwsurvey-weekly.json", "hwsurvey-weekly-prev.json", bucket)
    # Concat the json files into the output.
    print "Joining JSON files..."

    read_files = glob.glob("*.json")
    consolidated_json = []

    with open("hwsurvey-weekly.json", "w+") as report_json:
    # If we attempt to load invalid JSON from the assembled file,
    # the next function throws
        for f in read_files:
            with open(f, 'r+') as in_file:
                consolidated_json += json.load(in_file)
                data = json.dumps(consolidated_json, indent=2)
                report_json.write(data)
    # Store the new state to S3. Since S3 doesn't support symlinks, make two copy
    # of the file: one will always contain the latest data, the other for
    # archiving.
    archived_file_copy = "hwsurvey-weekly-" + \
        datetime.today().strftime("%Y%d%m") + ".json"
    store_new_state("hwsurvey-weekly.json", archived_file_copy, bucket)
    store_new_state("hwsurvey-weekly.json", "hwsurvey-weekly.json", bucket)


if __name__ == '__main__':
    main()
