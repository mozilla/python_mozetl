import pytest
from pyspark.sql import Row, functions as F

from mozetl.topline import topline_summary as topline


def search_row(engine='hooli', source='searchbar', count=1):
    return Row(
        engine=unicode(engine),
        source=unicode(source),
        count=count
    )

seconds_in_day = 60 * 60 * 24
nanoseconds_in_second = 10**9
# pcd = seconds_since_epoch(profile_creation_date) / seconds_in_day
# timestamp = seconds_since_epoch(submission_date) * nanoseconds_in_second

default_sample = {
    "client_id": "client",
    "timestamp": 0,
    "is_default_browser": False,
    "search_counts": [search_row()],
    "profile_creation_date": 0,
    "normalized_channel": "mozilla",
    "os": "linux",
    "hours": 1.0,
}