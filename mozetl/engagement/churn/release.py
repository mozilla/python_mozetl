from datetime import datetime, timedelta
import requests
from pyspark.sql import Row, functions as F


def create_date_to_version(release_info):
    """Create a map of yyyy-mm-dd date to the effective Firefox version on the
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
    """Perform an HTTP GET on the given uri, return the results as json. If
    there is an error fetching the data, raise it.
    """
    data = requests.get(uri)
    # Raise an exception if the fetch failed.
    data.raise_for_status()
    return data.json()


def compare_ver(a, b):
    """Logically compare two Firefox version strings. Split the string into
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
    """Fetch information about Firefox release dates.

    :returns:  an object containing two sections:
                   'major' - contains major releases (i.e. 41.0)
                   'minor' - contains stability releases (i.e. 41.0.1)
    """
    major_info = fetch_json(
        "https://product-details.mozilla.org/1.0/" "firefox_history_major_releases.json"
    )
    if major_info is None:
        raise Exception("Failed to fetch major version info")
    minor_info = fetch_json(
        "https://product-details.mozilla.org/1.0/"
        "firefox_history_stability_releases.json"
    )
    if minor_info is None:
        raise Exception("Failed to fetch minor version info")

    return {"major": major_info, "minor": minor_info}


def create_effective_version_table(spark):
    """Create the effective version table.

    This will download the Firefox release history and map the date to a
    particular version.

    :param spark:   A SparkSession context.
    :returns:       DataFrame containing the effective version information
    """
    d2v = create_date_to_version(get_release_info())
    rows = [Row(date, version) for date, version in d2v.iteritems()]
    df = spark.createDataFrame(rows, ["date", "effective_version"])
    return df


def with_effective_version(dataframe, effective_version, join_key):
    """Calculate the effective version of Firefox in the wild given the date
    and channel.

    For example, if the date is 2017-11-14 and the channel is 'Release', then
    the effective Firefox version is 57.0.0, since this is the Firefox version
    that would be available for installation from official sources. This is
    used to determine the version a profile was acquired on.

    :param dataframe:           A dataframe containing a date and channel col.
    :param effective_version:   A table mapping dates to application versions.
    :param join_key:            Column name generate version number on.
    :returns:                   A with a calculated "start_version" column
    """

    in_columns = {"channel"}
    out_columns = set(dataframe.columns) | {"start_version"}
    assert in_columns <= set(dataframe.columns), "must contain channel"

    # Firefox releases follow a train model. Each channel is a major revision
    # ahead from the upstream channel. Nightly corresponds to the build on
    # mozilla-central, and is always the head of the train. For example, if
    # the release channel is "55", then beta will be "56".
    #
    # This logic will be affected by the decommissioning of aurora.
    version_offset = F.when(F.col("channel").startswith("beta"), F.lit(1)).otherwise(
        F.when(F.col("channel").startswith("aurora"), F.lit(2)).otherwise(
            F.when(F.col("channel").startswith("nightly"), F.lit(3)).otherwise(F.lit(0))
        )
    )

    # Original effective version column name
    ev_version = effective_version.columns[1]

    # Column aliases in the joined table
    version = F.col(ev_version)
    date = F.col(join_key)

    joined_df = (
        dataframe
        # Rename the date field to join against the left table
        .join(effective_version.toDF(join_key, ev_version), join_key, "left")
        # Major version number e.g. "57"
        .withColumn("_major", F.split(version, "\\.").getItem(0).cast("int"))
        # Offset into the release train
        .withColumn("_offset", version_offset)
    )

    # Build up operations to get the effective start version of a particular
    # channel and date.

    # There will be null values for the version if the date is not in
    # the right table. This sets the start_version to one of two values.
    fill_outer_range = F.when(
        date.isNull() | (date < "2015-01-01"), F.lit("older")
    ).otherwise(F.lit("newer"))

    calculate_channel_version = F.when(
        F.col("channel").startswith("release"), version
    ).otherwise(F.concat(F.col("_major") + F.col("_offset"), F.lit(".0")))

    start_version = F.when(version.isNull(), fill_outer_range).otherwise(
        calculate_channel_version
    )

    return (
        joined_df.withColumn("start_version", start_version)
        .fillna("unknown", ["start_version"])
        .select(*out_columns)
    )
