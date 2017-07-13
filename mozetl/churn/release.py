from datetime import datetime, timedelta
import requests
from pyspark.sql import Row


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


def create_effective_version_table(spark):
    d2v = make_d2v(get_release_info())
    rows = [Row(date, version) for date, version in d2v.iteritems()]
    df = spark.createDataFrame(rows, ["date", "effective_version"])
    return df
