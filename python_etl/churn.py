""" Firefox Desktop Churn and Retention Cohorts

Tracked in Bug 1226379 [1]. The underlying dataset is generated via
the telemetry-batch-view [2] code, and is generated once a day. The
aggregated churn data is updated weekly.

Due to the client reporting latency, we need to wait 10 days for the
data to stabilize. If the date is passed into report through the
environment, it is assumed that the date is at least a week greater
than the report start date.  For example, if today is `20170323`, then
this notebook `20170316` in the `environment.date`. This notebook will
set date back 10 days to `20170306`, and then pin the date to nearest
Sunday. This date happens to be a Monday, so the date will be set to
`20170305`.

Code is based on the previous FHR analysis code [3].  Details and
definitions are in Bug 1198537 [4].

The production location of this dataset can be found in the following
location: `s3://telemetry-parquet/churn/v2`.

[1] https://bugzilla.mozilla.org/show_bug.cgi?id=1226379
[2] https://git.io/vSBAt
[3] https://github.com/mozilla/churn-analysis
[4] https://bugzilla.mozilla.org/show_bug.cgi?id=1198537
"""

import re
from datetime import date, datetime, timedelta

import click
import requests
from moztelemetry.standards import snap_to_beginning_of_week

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

source_columns = [
    "app_version",
    "attribution",
    "channel",
    "client_id",
    "country",
    "default_search_engine",
    "distribution_id",
    "locale",
    "normalized_channel",
    "profile_creation_date",
    "submission_date_s3",
    "subsession_length",
    "subsession_start_date",
    "sync_configured",
    "sync_count_desktop",
    "sync_count_mobile",
    "timestamp",
    "total_uri_count",
    "unique_domains_count"
]

# Bug 1289573: Support values like "mozilla86" and "mozilla86-utility-existing"
funnelcake_pattern = re.compile("^mozilla[0-9]+.*$")


def get_effective_version(d2v, channel, day):
    """ Get the effective version on the given channel on the given day."""
    if day not in d2v:
        if day < "2015-01-01":
            return "older"
        else:
            return "newer"

    effective_version = d2v[day]
    return get_channel_version(channel, effective_version)


def get_channel_version(channel, version):
    """ Given a channel and an effective release-channel version, give the
    calculated channel-specific version."""
    if channel.startswith('release'):
        return version
    numeric_version = int(version[0:version.index('.')])
    offset = 0
    if channel.startswith('beta'):
        offset = 1
    elif channel.startswith('aurora'):
        offset = 2
    elif channel.startswith('nightly'):
        offset = 3
    return "{}.0".format(numeric_version + offset)


def make_d2v(release_info):
    """ Create a map of yyyy-mm-dd date to the effective Firefox version on the
    release channel.
    """
    # Combine major and minor releases into a map of day -> version
    # Keep only the highest version available for a day range.
    observed_dates = set(release_info["major"].values())
    observed_dates |= set(release_info["minor"].values())
    # Skip old versions.
    sd = [d for d in sorted(observed_dates) if d >= "2014-01-01"]
    start_date_str = sd[0]
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(sd[-1], "%Y-%m-%d")
    day_count = (end_date - start_date).days + 1
    d2v = {}
    # Start with all the available version release days:
    for t in ["major", "minor"]:
        for m, d in release_info[t].iteritems():
            if d < start_date_str:
                continue
            if d not in d2v or compare_ver(m, d2v[d]) > 0:
                d2v[d] = m
    last_ver = d2v[start_date_str]
    # Fill in all the gaps:
    for dt in (start_date + timedelta(n) for n in range(day_count)):
        d = datetime.strftime(dt, "%Y-%m-%d")
        if d in d2v:
            # Don't replace a higher version with a new release of an old
            # version (probably an ESR release)
            if compare_ver(d2v[d], last_ver) < 0:
                d2v[d] = last_ver
            else:
                last_ver = d2v[d]
        else:
            d2v[d] = last_ver
    return d2v


def fetch_json(uri):
    """ Perform an HTTP GET on the given uri, return the results as json. If
    there is an error fetching the data, raise it.
    """
    data = requests.get(uri)
    # Raise an exception if the fetch failed.
    data.raise_for_status()
    return data.json()


def compare_ver(a, b):
    """ Logically compare two Firefox version strings. Split the string into
    pieces, and compare each piece numerically.

    Returns -1, 0, or 1 depending on whether a is less than, equal to, or
    greater than b.
    """
    if a == b:
        return 0

    ap = [int(p) for p in a.split(".")]
    bp = [int(p) for p in b.split(".")]
    lap = len(ap)
    lbp = len(bp)

    # min # of pieces
    mp = lap
    if lbp < mp:
        mp = lbp

    for i in range(mp):
        if ap[i] < bp[i]:
            return -1
        if ap[i] > bp[i]:
            return 1

    if lap > lbp:
        # a has more pieces, common pieces are the same, a is greater
        return 1

    if lap < lbp:
        # a has fewer pieces, common pieces are the same, b is greater
        return -1

    # They are exactly the same.
    return 0


def get_release_info():
    """ Fetch information about Firefox release dates. Returns an object
    containing two sections:

    'major' - contains major releases (i.e. 41.0)
    'minor' - contains stability releases (i.e. 41.0.1)
    """
    major_info = fetch_json("https://product-details.mozilla.org/1.0/"
                            "firefox_history_major_releases.json")
    if major_info is None:
        raise Exception("Failed to fetch major version info")
    minor_info = fetch_json("https://product-details.mozilla.org/1.0/"
                            "firefox_history_stability_releases.json")
    if minor_info is None:
        raise Exception("Failed to fetch minor version info")
    return {"major": major_info, "minor": minor_info}


def daynum_to_date(daynum):
    """ Convert a number of days to a date. If it's out of range, default to a max date.
    :param daynum: A number of days since Jan 1, 1970
    """
    if daynum is None:
        return None
    if daynum < 0:
        return None
    daycount = int(daynum)
    if daycount > 1000000:
        # Some time in the 48th century, clearly bogus.
        daycount = 1000000
    return date(1970, 1, 1) + timedelta(daycount)


def sane_date(d):
    """ Check if the given date looks like a legitimate time on which activity
    could have happened.
    """
    if d is None:
        return False
    return d > date(2000, 1, 1) and d < datetime.utcnow().date() + timedelta(2)


def is_funnelcake(distro):
    """ Check if a given distribution_id appears to be a funnelcake build."""
    if distro is None:
        return False
    return funnelcake_pattern.match(distro) is not None


top_countries = set(
    ["US", "DE", "FR", "RU", "BR", "IN", "PL", "ID", "GB", "CN",
     "IT", "JP", "CA", "ES", "UA", "MX", "AU", "VN", "EG", "AR",
     "PH", "NL", "IR", "CZ", "HU", "TR", "RO", "GR", "AT", "CH"])


def top_country(country):
    global top_countries
    if(country in top_countries):
        return country
    return "ROW"


def get_week_num(creation, today):
    if creation is None or today is None:
        return None

    diff = (today.date() - creation).days
    if diff < 0:
        # Creation date is in the future. Bad data :(
        return -1
    # The initial week is week zero.
    return int(diff / 7)


# The number of seconds in a single hour, casted to float, so we get
# the fractional part when converting.
SECONDS_IN_HOUR = float(60 * 60)


def convert(d2v, week_start, datum):
    out = {"good": False}

    pcd = daynum_to_date(datum.profile_creation_date)
    if not sane_date(pcd):
        return out

    pcd_formatted = datetime.strftime(pcd, "%Y-%m-%d")

    out["client_id"] = datum.client_id
    channel = datum.normalized_channel
    out["is_funnelcake"] = is_funnelcake(datum.distribution_id)
    if out["is_funnelcake"]:
        channel = "{}-cck-{}".format(datum.normalized_channel,
                                     datum.distribution_id)
    out["channel"] = channel
    out["geo"] = top_country(datum.country)
    out["acquisition_period"] = snap_to_beginning_of_week(pcd, "Sunday")
    out["start_version"] = get_effective_version(d2v, channel, pcd_formatted)

    # bug 1337037 - stub attribution
    attribution_fields = ["source", "medium", "campaign", "content"]
    if datum.attribution:
        for field in attribution_fields:
            out[field] = datum.attribution[field]

    # bug 1323598
    out['distribution_id'] = datum.distribution_id
    out['default_search_engine'] = datum.default_search_engine
    out['locale'] = datum.locale

    deviceCount = 0
    if datum.sync_count_desktop is not None:
        deviceCount += datum.sync_count_desktop
    if datum.sync_count_mobile is not None:
        deviceCount += datum.sync_count_mobile

    if deviceCount > 1:
        out["sync_usage"] = "multiple"
    elif deviceCount == 1:
        out["sync_usage"] = "single"
    elif datum.sync_configured is not None:
        if datum.sync_configured:
            out["sync_usage"] = "single"
        else:
            out["sync_usage"] = "no"
    # Else we don't set sync_usage at all, and use a default value later.

    out["current_version"] = datum.version

    # The usage time is in seconds, but we really need hours.  Because
    # we filter out broken subsession_lengths, we could end up with
    # clients with no usage hours.
    out["usage_hours"] = ((datum.usage_seconds / SECONDS_IN_HOUR)
                          if datum.usage_seconds is not None
                          else 0.0)
    out["squared_usage_hours"] = out["usage_hours"] ** 2

    # d.get does not default to 0, so make sure that its an int here
    out["total_uri_count"] = datum.total_uri_count or 0
    out["unique_domains_count"] = datum.unique_domains_count or 0

    # Incoming subsession_start_date looks like "2016-02-22T00:00:00.0-04:00"
    client_date = None
    if datum.subsession_start_date is not None:
        try:
            client_date = datetime.strptime(
                datum.subsession_start_date[0:10], "%Y-%m-%d")
        except ValueError:
            # Bogus format
            pass
        except TypeError:
            # String contains null bytes or other weirdness. Example:
            # TypeError: must be string without null bytes, not unicode
            pass
    if client_date is None:
        # Fall back to submission date
        client_date = datetime.strptime(datum.submission_date_s3, "%Y%m%d")
    out["current_week"] = get_week_num(pcd, client_date)
    out["is_active"] = "yes"
    if client_date is not None:
        try:
            if datetime.strftime(client_date, "%Y%m%d") < week_start:
                out["is_active"] = "no"
        except ValueError:
            pass
    out["good"] = True
    return out


# Build the "effective version" cache:
d2v = make_d2v(get_release_info())


def fmt(d, date_format="%Y%m%d"):
    return datetime.strftime(d, date_format)


# ### Compute the aggregates
#
# Run the aggregation code, detecting files that are missing.
#
# The fields we want in the output are:
#  - channel (appUpdateChannel)
#  - geo (bucketed into top 30 countries + "rest of world")
#  - is_funnelcake (contains "-cck-"?)
#  - acquisition_period (cohort_week)
#  - start_version (effective version on profile creation date)
#  - sync_usage ("no", "single" or "multiple" devices)
#  - current_version (current appVersion)
#  - current_week (week)
#  - is_active (were the client_ids active this week or not)
#  - n_profiles (count of matching client_ids)
#  - usage_hours (sum of the per-client subsession lengths,
#        clamped in the[0, MAX_SUBSESSION_LENGTH] range)
#  - sum_squared_usage_hours (the sum of squares of the usage hours)

MAX_SUBSESSION_LENGTH = 60 * 60 * 48  # 48 hours in seconds.

record_columns = [
    'channel',
    'geo',
    'is_funnelcake',
    'acquisition_period',
    'start_version',
    'sync_usage',
    'current_version',
    'current_week',
    'source',
    'medium',
    'campaign',
    'content',
    'distribution_id',
    'default_search_engine',
    'locale',
    'is_active',
    'n_profiles',
    'usage_hours',
    'sum_squared_usage_hours',
    'total_uri_count',
    'unique_domains_count'
]


def get_newest_per_client(df):
    windowSpec = Window.partitionBy(
        F.col('client_id')).orderBy(F.col('timestamp').desc())
    rownum_by_timestamp = (F.row_number().over(windowSpec))
    selectable_by_client = df.select(
        rownum_by_timestamp.alias('row_number'),
        *[F.col(col) for col in df.columns]
    )
    return selectable_by_client.filter(selectable_by_client['row_number'] == 1)


def compute_churn_week(df, week_start):
    """Compute the churn data for this week. Note that it takes 10 days
    from the end of this period for all the activity to arrive. This data
    should be from Sunday through Saturday.

    df: DataFrame of the dataset relevant to computing the churn
    week_start: datestring of this time period"""

    week_start_date = datetime.strptime(week_start, "%Y%m%d")
    week_end_date = week_start_date + timedelta(6)
    week_start = fmt(week_start_date)
    week_end = fmt(week_end_date)

    # Verify that the start date is a Sunday
    if week_start_date.weekday() != 6:
        print("Week start date {} is not a Sunday".format(week_start))
        return

    # If the data for this week can still be coming, don't try to compute the
    # churn.
    week_end_slop = fmt(week_end_date + timedelta(10))
    today = fmt(datetime.utcnow())
    if week_end_slop >= today:
        print("Skipping week of {} to {} - Data is still arriving until {}."
              .format(week_start, week_end, week_end_slop))
        return

    print("Starting week from {} to {} at {}"
          .format(week_start, week_end, datetime.utcnow()))
    # the subsession_start_date field has a different form than
    # submission_date_s3, so needs to be formatted with hyphens.
    week_end_excl = fmt(week_end_date + timedelta(1), date_format="%Y-%m-%d")
    week_start_hyphenated = fmt(week_start_date, date_format="%Y-%m-%d")

    current_week = (
        df.filter(df['submission_date_s3'] >= week_start)
          .filter(df['submission_date_s3'] <= week_end_slop)
          .filter(df['subsession_start_date'] >= week_start_hyphenated)
          .filter(df['subsession_start_date'] < week_end_excl)
    )

    # clean some of the aggregate fields
    current_week = current_week.na.fill(
        0, ["total_uri_count", "unique_domains_count"])

    # Clamp broken subsession values in the [0, MAX_SUBSESSION_LENGTH] range.
    clamped_subsession = (
        current_week
        .select(
            F.col('client_id'),
            F.when(
                F.col('subsession_length') > MAX_SUBSESSION_LENGTH,
                MAX_SUBSESSION_LENGTH)
            .otherwise(
                F.when(F.col('subsession_length') < 0, 0)
                .otherwise(F.col('subsession_length')))
            .alias('subsession_length'))
    )

    # Compute the overall usage time for each client by summing the subsession
    # lengths.
    grouped_usage_time = (
        clamped_subsession
        .groupby('client_id')
        .sum('subsession_length')
        .withColumnRenamed('sum(subsession_length)', 'usage_seconds')
    )

    # Get the newest ping per client and append to original dataframe
    newest_per_client = get_newest_per_client(current_week)
    newest_with_usage = newest_per_client.join(
        grouped_usage_time, 'client_id', 'inner')

    converted = newest_with_usage.rdd.map(
        lambda x: convert(d2v, week_start, x))

    # Don't bother to filter out non-good records - they will appear
    # as 'unknown' in the output.
    countable = converted.map(
        lambda x: (
            (
                # attributes unique to a client
                x.get('channel', 'unknown'),
                x.get('geo', 'unknown'),
                "yes" if x.get('is_funnelcake', False) else "no",
                datetime.strftime(
                    x.get('acquisition_period', date(2000, 1, 1)), "%Y-%m-%d"),
                x.get('start_version', 'unknown'),
                x.get('sync_usage', 'unknown'),
                x.get('current_version', 'unknown'),
                x.get('current_week', 'unknown'),
                x.get('source', 'unknown'),
                x.get('medium', 'unknown'),
                x.get('campaign', 'unknown'),
                x.get('content', 'unknown'),
                x.get('distribution_id', 'unknown'),
                x.get('default_search_engine', 'unknown'),
                x.get('locale', 'unknown'),
                x.get('is_active', 'unknown')
            ), (
                1,  # active users
                x.get('usage_hours', 0),
                x.get('squared_usage_hours', 0),
                x.get('total_uri_count', 0),
                x.get('unique_domains_count', 0)
            )
        )
    )

    def reduce_func(x, y):
        return tuple(map(sum, zip(x, y)))

    aggregated = countable.reduceByKey(reduce_func)

    records_df = aggregated.map(lambda x: x[0] + x[1]).toDF(record_columns)

    # Apply some post-processing for other aggregates
    # (i.e. unique_domains_count). This needs to be done when you want
    # something other than just a simple sum
    def average(total, n):
        if not n:
            return 0.0
        return float(total) / n
    average_udf = F.udf(average, DoubleType())

    # Create new derived columns and any unecessary ones
    records_df = (
        records_df
        .withColumn('average_unique_domains_count',
                    average_udf(F.col('unique_domains_count'),
                                F.col('n_profiles')))
        .drop('unique_domains_count')
        .withColumnRenamed('average_unique_domains_count',
                           'unique_domains_count')
    )

    return records_df


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) // 7):
        yield (start_date + timedelta(n * 7)).strftime("%Y%m%d")


def process_backfill(start_date, end_date, callback):
    """ Import data from a start date to an end date.

    :start_date ds: starting date in yyyymmdd
    :end_date ds: ending date in yyymmdd
    :callback: a callback accepting a datestring in format yyyymmdd
    """
    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    for d in daterange(start_date, end_date):
        try:
            callback(d)
        except Exception as e:
            print e


def process_week(df, week_start, bucket, prefix):
    result_df = compute_churn_week(df, week_start)

    # Write to s3 as parquet, file size is on the order of 40MB. We
    # bump the version number because v1 is the path to the old
    # telemetry-batch-view churn data.
    s3_path = "s3://{}/{}/week_start={}".format(bucket, prefix, week_start)
    print("{}: Writing output as parquet to {}"
          .format(datetime.utcnow(), s3_path))

    result_df.repartition(1).write.parquet(s3_path, mode="overwrite")

    print("Finished week {} at {}".format(week_start, datetime.utcnow()))


@click.command()
@click.option('-d', '--date', 'start_date',
              default=fmt(datetime.now() - timedelta(7)),
              help="datestring in form YYYYmmdd")
@click.option('-b', '--bucket',
              default='net-mozaws-prod-us-west-2-pipeline-analysis',
              help="output s3 bucket")
@click.option('-p', '--prefix', default='telemetry-test-bucket/churn-test',
              help="output prefix associated with the s3 bucket")
@click.option('--lag/--no-lag', default=True,
              help="account for the 10 day collection period")
@click.option('--backfill/--no-backfill', default=False,
              help="backfill from the start date")
def main(start_date, bucket, prefix, lag, backfill):
    """Compute churn / retention information for unique segments of Firefox
users acquired during a specific period of time.
    """
    spark = (SparkSession
             .builder
             .appName("churn")
             .getOrCreate())

    version = 'v2'
    s3_path = "s3://{}/{}/{}".format(bucket, prefix, version)

    # If this job is scheduled, we need the input date to lag a total of
    # 10 days of slack for incoming data. Airflow subtracts 7 days to
    # account for the weekly nature of this report.
    week_start_date = start_date
    if lag:
        offset_start = datetime.strptime(start_date, "%Y%m%d") - timedelta(10)
        week_start_date = snap_to_beginning_of_week(offset_start, "Sunday")

    try:
        source_df = spark.read.option("mergeSchema", "true").parquet(s3_path)
        subset_df = (source_df
                     .select(source_columns)
                     .withColumnRenamed('app_version', 'version'))

        # Note that main_summary_v3 only goes back to 20160312
        if backfill:
            week_end_date = fmt(datetime.utcnow() - timedelta(1))
            process_backfill(
                fmt(week_start_date),
                week_end_date,
                lambda d: process_week(subset_df, d, bucket, prefix))
        else:
            process_week(subset_df, fmt(week_start_date), bucket, prefix)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
