import arrow

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    LongType,
    IntegerType,
    BooleanType,
)

"""
Calendar for reference

    January 2017
Su Mo Tu We Th Fr Sa
 1  2  3  4  5  6  7
 8  9 10 11 12 13 14
15 16 17 18 19 20 21
22 23 24 25 26 27 28
29 30 31
"""

SPBE = "scalar_parent_browser_engagement_"

# variables for conversion
SECONDS_PER_DAY = 60 * 60 * 24

# Generate the datasets
# Sunday, also the first day in this collection period.
SUBSESSION_START = arrow.get(2017, 1, 15).replace(tzinfo="utc")
WEEK_START_DS = SUBSESSION_START.format("YYYYMMDD")


main_summary_schema = StructType(
    [
        StructField("app_version", StringType(), True),
        StructField(
            "attribution",
            StructType(
                [
                    StructField("source", StringType(), True),
                    StructField("medium", StringType(), True),
                    StructField("campaign", StringType(), True),
                    StructField("content", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("channel", StringType(), True),
        StructField("client_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("default_search_engine", StringType(), True),
        StructField("distribution_id", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("normalized_channel", StringType(), True),
        StructField("profile_creation_date", LongType(), True),
        StructField("submission_date_s3", StringType(), False),
        StructField("subsession_length", LongType(), True),
        StructField("subsession_start_date", StringType(), True),
        StructField("sync_configured", BooleanType(), True),
        StructField("sync_count_desktop", IntegerType(), True),
        StructField("sync_count_mobile", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField(SPBE + "total_uri_count", IntegerType(), True),
        StructField(SPBE + "unique_domains_count", IntegerType(), True),
    ]
)


new_profile_schema = StructType(
    [
        StructField("submission", StringType(), True),
        StructField(
            "environment",
            StructType(
                [
                    StructField(
                        "profile",
                        StructType([StructField("creation_date", LongType(), True)]),
                        True,
                    ),
                    StructField(
                        "build",
                        StructType([StructField("version", StringType(), True)]),
                        True,
                    ),
                    StructField(
                        "partner",
                        StructType(
                            [StructField("distribution_id", StringType(), True)]
                        ),
                        True,
                    ),
                    StructField(
                        "settings",
                        StructType(
                            [
                                StructField("locale", StringType(), True),
                                StructField("is_default_browser", StringType(), True),
                                StructField(
                                    "default_search_engine", StringType(), True
                                ),
                                StructField(
                                    "attribution",
                                    StructType(
                                        [
                                            StructField("source", StringType(), True),
                                            StructField("medium", StringType(), True),
                                            StructField("campaign", StringType(), True),
                                            StructField("content", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("client_id", StringType(), True),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("geo_country", StringType(), True),
                    StructField("timestamp", LongType(), True),
                    StructField("normalized_channel", StringType(), True),
                    StructField("creation_timestamp", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)

main_summary_sample = {
    "app_version": "57.0.0",
    "attribution": {
        "source": "source-value",
        "medium": "medium-value",
        "campaign": "campaign-value",
        "content": "content-value",
    },
    "channel": "release",
    "client_id": "client-id",
    "country": "US",
    "default_search_engine": "wikipedia",
    "distribution_id": "mozilla42",
    "locale": "en-US",
    "normalized_channel": "release",
    "profile_creation_date": int(SUBSESSION_START.timestamp / SECONDS_PER_DAY),
    "submission_date_s3": SUBSESSION_START.format("YYYYMMDD"),
    "subsession_length": 3600,
    "subsession_start_date": str(SUBSESSION_START),
    "sync_configured": False,
    "sync_count_desktop": 1,
    "sync_count_mobile": 1,
    "timestamp": SUBSESSION_START.timestamp * 10 ** 9,  # nanoseconds
    SPBE + "total_uri_count": 20,
    SPBE + "unique_domains_count": 3,
}


new_profile_sample = {
    "submission": SUBSESSION_START.format("YYYYMMDD"),
    "environment": {
        "profile": {"creation_date": int(SUBSESSION_START.timestamp / SECONDS_PER_DAY)},
        "build": {"version": "57.0.0"},
        "partner": {"distribution_id": "mozilla57"},
        "settings": {
            "locale": "en-US",
            "is_default_browser": True,
            "default_search_engine": "google",
            "attribution": {
                "source": "source-value",
                "medium": "medium-value",
                "campaign": "campaign-value",
                "content": "content-value",
            },
        },
    },
    "client_id": "new-profile",
    "metadata": {
        "geo_country": "US",
        "timestamp": (SUBSESSION_START.timestamp + 3600) * 10 ** 9,
        "normalized_channel": "release",
        "creation_timestamp": SUBSESSION_START.timestamp * 10 ** 9,
    },
}


def format_ssd(arrow_date):
    """Format an arrow date into the submission_start_date"""
    return str(arrow_date.replace(tzinfo="utc"))


def format_pcd(arrow_date):
    """Format an arrow date into the profile_creation_date"""
    return int(arrow_date.timestamp / SECONDS_PER_DAY)


def generate_dates(subsession_date, submission_offset=0, creation_offset=0):
    """ Generate a tuple containing information about all pertinent dates
    in the input for the churn dataset.

    :date datetime.date: date as seen by the client
    :submission_offset int: offset into the future for submission_date_s3
    :creation_offset int: offset into the past for the profile creation date
    """

    submission_date = subsession_date.replace(days=submission_offset)
    profile_creation_date = subsession_date.replace(days=-creation_offset)

    date_snippet = {
        "subsession_start_date": format_ssd(subsession_date),
        "submission_date_s3": submission_date.format("YYYYMMDD"),
        "profile_creation_date": format_pcd(profile_creation_date),
        "timestamp": submission_date.timestamp * 10 ** 9,
    }

    return date_snippet
