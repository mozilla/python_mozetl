import click
from mozetl.hardware_report.summarize_json import *
from click_datetime import Datetime
from datetime import datetime, timedelta


@click.command()
@click.option('--start_date',
              type=Datetime(format='%Y/%m/%d'),
              default=None,
              help='Start date, only use this when backfilling (e.g. yyyy/mm/dd)')
@click.option('--end_date',
              type=Datetime(format='%Y/%m/%d'),
              default=None,
              help='End date, only use this when backfilling (e.g. yyyy/mm/dd)')
def main(start_date, end_date):
    if start_date:
        end_date = start_date + timedelta(days=7)

    # Generate the hardware report
    start_date = None  # Only use this when backfilling, e.g. dt.date(2016,2,1)
    end_date = None  # Only use this when backfilling, e.g. dt.date(2016,3,26)

    # Generate the report for the desired period.
    generate_report(start_date, end_date)
    # Fetch the previous data from S3 and save it locally.
    fetch_previous_state("hwsurvey-weekly.json", "hwsurvey-weekly-prev.json")
    # Concat the json files into the output.
    print "Joining JSON files..."
    get_ipython().system(u'jq -s "[.[]|.[]]" *.json > "hwsurvey-weekly.json"')

    with open("hwsurvey-weekly.json", "rt") as report_json:
        # If we attempt to load invalid JSON from the assembled file,
        # the next function throws.
        json.load(report_json)

    # Store the new state to S3. Since S3 doesn't support symlinks, make two copy
    # of the file: one will always contain the latest data, the other for
    # archiving.
    archived_file_copy = "hwsurvey-weekly-" + \
        datetime.date.today().strftime("%Y%d%m") + ".json"
    store_new_state("hwsurvey-weekly.json", archived_file_copy)
    store_new_state("hwsurvey-weekly.json", "hwsurvey-weekly.json")


if __name__ == '__main__':
    main()