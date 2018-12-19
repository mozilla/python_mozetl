"""Hardware Report Generator.

This dashboard can be found at:
https://hardware.metrics.mozilla.com/
"""

import click
import json
import glob
import summarize_json
from check_output import check_output
from click_datetime import Datetime
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


datetime_type = Datetime(format='%Y%m%d')


@click.command()
@click.option('--start_date',
              type=datetime_type,
              required=True,
              help='Start date (e.g. yyyymmdd)')
@click.option('--end_date',
              type=datetime_type,
              default=None,
              help='End date (e.g. yyyymmdd)')
@click.option('--bucket',
              required=True,
              help='Output bucket for JSON data')
def main(start_date, end_date, bucket):
    """Generate this week's report and append it to previous report."""
    # Set default end date to a week after start date if end date not provided
    if not end_date:
        end_date = start_date + timedelta(days=6)

    spark = (SparkSession
             .builder
             .appName("hardware_report_dashboard")
             .enableHiveSupport()
             .getOrCreate())

    # Generate the report for the desired period.
    report = summarize_json.generate_report(start_date, end_date, spark)
    summarize_json.serialize_results(report)
    # Fetch the previous data from S3 and save it locally.
    summarize_json.fetch_previous_state("hwsurvey-weekly.json", "hwsurvey-weekly-prev.json",
                                        bucket)
    # Concat the json files into the output.
    print("Joining JSON files...")

    new_files = set(report.keys())
    read_files = set(glob.glob("*.json"))
    consolidated_json = []

    with open("hwsurvey-weekly.json", "w+") as report_json:
        # If we attempt to load invalid JSON from the assembled file,
        # the next function throws. We process new files first, which
        # in effect overwrites data on reruns.
        for f in list(new_files) + list(read_files - new_files):
            with open(f, 'r+') as in_file:
                for data in json.load(in_file):
                    if data['date'] not in [j['date'] for j in consolidated_json]:
                        consolidated_json.append(data)

        report_json.write(json.dumps(consolidated_json, indent=2))

    # Store the new state to S3. Since S3 doesn't support symlinks, make
    # two copies of the file: one will always contain the latest data,
    # the other for archiving.
    archived_file_copy = "hwsurvey-weekly-" + \
        datetime.today().strftime("%Y%d%m") + ".json"
    summarize_json.store_new_state("hwsurvey-weekly.json", archived_file_copy, bucket)
    summarize_json.store_new_state("hwsurvey-weekly.json", "hwsurvey-weekly.json", bucket)

    check_output()


if __name__ == '__main__':
    main()
