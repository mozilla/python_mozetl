import click
import glob
from mozetl.hardware_report.summarize_json import *
from click_datetime import Datetime
from datetime import datetime, timedelta


@click.command()
@click.option('--start_date',
              type=Datetime(format='%Y%m%d'),
              required=True,
              help='Start date, only use this when backfilling (e.g. yyyy/mm/dd)')
@click.option('--end_date',
              type=Datetime(format='%Y%m%d'),
              default=None,
              help='End date, only use this when backfilling (e.g. yyyy/mm/dd)')
@click.option('--bucket',
              required=True,
              help='S3 Public Bucket')
def main(start_date, end_date, bucket):
    if start_date:
        end_date = start_date + timedelta(days=7)

    # Generate the report for the desired period.
    generate_report(start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))
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
