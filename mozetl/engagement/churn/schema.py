from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

churn_schema = StructType(
    [
        StructField(
            "channel",
            StringType(),
            True,
            metadata={"description": "The application update channel."},
        ),
        StructField(
            "geo",
            StringType(),
            True,
            metadata={
                "description": "Bucketed by the top 30 countries and rest of the world (ROW)."
            },
        ),
        StructField(
            "is_funnelcake",
            StringType(),
            True,
            metadata={"description": 'Does the channel contain "-cck-"?'},
        ),
        StructField(
            "acquisition_period",
            StringType(),
            True,
            metadata={
                "description": "The starting week of a cohort, defined by the "
                "profile creation date."
            },
        ),
        StructField(
            "start_version",
            StringType(),
            True,
            metadata={
                "description": "The version of the application when the profile was created"
            },
        ),
        StructField(
            "sync_usage",
            StringType(),
            True,
            metadata={
                "description": 'Sync usage broken down into "no", "single" or "multiple" devices'
            },
        ),
        StructField(
            "current_version",
            StringType(),
            True,
            metadata={"description": "The current application version"},
        ),
        StructField(
            "current_week",
            LongType(),
            True,
            metadata={"description": "The current week"},
        ),
        StructField(
            "source",
            StringType(),
            True,
            metadata={"description": "Attribution: origin of the install traffic"},
        ),
        StructField(
            "medium",
            StringType(),
            True,
            metadata={
                "description": "Attribution: general category of the traffic source"
            },
        ),
        StructField(
            "campaign",
            StringType(),
            True,
            metadata={"description": "Attribution: campaign"},
        ),
        StructField(
            "content",
            StringType(),
            True,
            metadata={
                "description": "Attribution: groups of several sources with the same medium"
            },
        ),
        StructField(
            "distribution_id",
            StringType(),
            True,
            metadata={"description": "Funnelcake associated with profile"},
        ),
        StructField(
            "default_search_engine",
            StringType(),
            True,
            metadata={"description": "The default search engine"},
        ),
        StructField(
            "locale", StringType(), True, metadata={"description": "The locale"}
        ),
        StructField(
            "is_active",
            StringType(),
            True,
            metadata={
                "description": "Used to determine if the profile seen during processing is active"
                "during the particular week of churn."
            },
        ),
        StructField(
            "n_profiles",
            LongType(),
            True,
            metadata={"description": "Count of matching client_ids"},
        ),
        StructField(
            "usage_hours",
            DoubleType(),
            True,
            metadata={
                "description": "sum of the per-client subsession lengths, "
                "clamped in the [0, MAX_SUBSESSION_LENGTH] range"
            },
        ),
        StructField(
            "sum_squared_usage_hours",
            DoubleType(),
            True,
            metadata={"description": "The sum of squares of the usage hours"},
        ),
        StructField(
            "total_uri_count",
            LongType(),
            True,
            metadata={"description": "The sum of per-client uri counts"},
        ),
        StructField(
            "unique_domains_count_per_profile",
            DoubleType(),
            True,
            metadata={
                "description": "The average of the average unique domains per-client"
            },
        ),
    ]
)
