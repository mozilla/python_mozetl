import click
import glob
from mozetl.hardware_report.summarize_json import *
from click_datetime import Datetime
from datetime import datetime, timedelta
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

@click.command()
@click.option('--start_date',
              type=Datetime(format='%Y%m%d'),
              required=True,
              help='Start date (e.g. yyyy/mm/dd)')
@click.option('--end_date',
              type=Datetime(format='%Y%m%d'),
              default=None,
              help='End date (e.g. yyyy/mm/dd)')
@click.option('--bucket',
              required=True,
              help='Output bucket for JSON data')
def main(start_date, end_date, bucket):
    # Set default end date to a week after start date if end date not provided
    if not end_date:
        end_date = start_date + timedelta(days=7)

    spark = (SparkSession
         .builder
         .appName("hardware_report_dashboard")
         .getOrCreate())

    # Generate the report for the desired period.
    report = generate_report(start_date, end_date, spark)
    serialize_results(report)
    # Fetch the previous data from S3 and save it locally.
    fetch_previous_state("hwsurvey-weekly.json", "hwsurvey-weekly-prev.json", bucket)
    # Concat the json files into the output.
    print "Joining JSON files..."

    read_files = glob.glob("*.json")
    with open("hwsurvey-weekly.json", "wb") as report_json:
        # If we attempt to load invalid JSON from the assembled file,
        # the next function throws.
        report_json.write('[{}'.format(
            ','.join([open(f, "rb").read() for f in read_files])))

        json.load(report_json)

    # Store the new state to S3. Since S3 doesn't support symlinks, make two copy
    # of the file: one will always contain the latest data, the other for
    # archiving.
    archived_file_copy = "hwsurvey-weekly-" + \
        datetime.date.today().strftime("%Y%d%m") + ".json"
    store_new_state("hwsurvey-weekly.json", archived_file_copy, bucket)
    store_new_state("hwsurvey-weekly.json", "hwsurvey-weekly.json", bucket)


if __name__ == '__main__':
    main()
