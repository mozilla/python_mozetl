from pyspark.sql import types, functions as F
from pyspark.sql.window import Window
from datetime import datetime
import operator

countries = set([
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS",
    "AT", "AU", "AW", "AX", "AZ", "BA", "BB", "BD", "BE", "BF", "BG",
    "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS", "BT",
    "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI",
    "CK", "CL", "CM", "CN", "CO", "CR", "CU", "CV", "CW", "CX", "CY",
    "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EH",
    "ER", "ES", "ET", "FI", "FJ", "FK", "FM", "FO", "FR", "GA", "GB",
    "GD", "GE", "GF", "GG", "GH", "GI", "GL", "GM", "GN", "GP", "GQ",
    "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM", "HN", "HR", "HT",
    "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT",
    "JE", "JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP",
    "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI", "LK", "LR", "LS",
    "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH",
    "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU",
    "MV", "MW", "MX", "MY", "MZ", "NA", "NC", "NE", "NF", "NG", "NI",
    "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG",
    "PH", "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA",
    "RE", "RO", "RS", "RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG",
    "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST",
    "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK",
    "TL", "TM", "TN", "TO", "TR", "TT", "TV", "TW", "TZ", "UA", "UG",
    "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU",
    "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW"
])

seconds_per_hour = 60 * 60
seconds_per_day = seconds_per_hour * 24


def column_like(name, patterns, default):
    """ patterns: dict[label, list[match_expr]]"""
    # start with the pyspark.sql.functions
    op = F
    for label in patterns:
        cond = reduce(
            operator.__or__,
            [F.col(name).like(pat) for pat in patterns[label]]
        )
        op = op.when(cond, label)
    return op.otherwise(default)


def clean_input(dataframe, start, end):
    input_columns = [
        "client_id",
        "submission_date",
        "is_default_browser",
        "search_counts",
        "country",
        "profile_creation_date",
        "channel",
        "os",
        "hours",
    ]
    columns = {col: F.col(col) for col in input_columns}

    # normalize countries against a whitelist
    columns["country"] = (
        F.when(F.col("country").isin(countries), F.col("country"))
        .otherwise("Other")
        .alias("country")
    )

    # clean operating system based on CEP naming scheme
    pattern = {
        "Windows": ["Windows%", "%WINNT%"],
        "Mac": ["Darwin%"],
        "Linux": ["%Linux%", "%BSD%", "%SunOS%"],
    }
    columns["os"] = column_like("os", pattern, "Other")

    # rename normalized channel to channel
    columns["channel"] = F.col("normalized_channel")

    # convert profile creation date into seconds (day -> seconds)
    columns["profile_creation_date"] = (
        F.when(F.col("profile_creation_date") >= 0,
               F.col("profile_creation_date") * seconds_per_day)
        .otherwise(0.0)
        .cast(types.DoubleType())
    )

    # generate hours of usage from subsession length (seconds -> hours)
    columns["hours"] = (
        F.when((F.col("subsession_length") >= 0) &
               (F.col("subsession_length") < 180 * seconds_per_day),
               F.col("subsession_length") / seconds_per_hour)
        .otherwise(0.0)
        .cast(types.DoubleType())
    )

    # clean the dataset
    clean = (
        dataframe
        .where(F.col("submission_date_s3") >= start)
        .where(F.col("submission_date_s3") < end)
        .select(*[columns[col].alias(col) for col in input_columns])
    )

    return clean


def search_aggregates(dataframe, attributes):

    # search engines to pivot against
    search_labels = ["google", "bing", "yahoo", "other"]

    # patterns to filter search engines
    patterns = {
        "google": ["%Google%", "%google%"],
        "bing": ["%Bing%", "%bing%"],
        "yahoo": ["%Yahoo%", "%yahoo%"],
    }

    s_engine = (
        column_like("search_count.engine", patterns, "other")
        .alias("engine")
    )
    s_count = (
        F.when("search_count.count" > 0, "search_count.count")
        .otherwise(0)
        .alias("count")
    )

    # generate the search aggregates by exploding and pivoting
    search = (
        dataframe
        .withColumn("search_count", F.explode("search_counts"))
        .select("country", "channel", "os", s_engine, s_count)
        .groupBy(attributes)
        .pivot("engine", search_labels)
        .agg(F.sum("count"))
        .na.fill(0, search_labels)
    )

    return search


def hours_aggregates(dataframe, attributes):
    # simple aggregate
    hours = (
        dataframe
        .groupBy(attributes)
        .agg(F.sum("hours").alias("hours"))
    )
    return hours


def client_aggregates(dataframe, timestamp, attributes):

    clients = (
        dataframe
        .withColumn("new_client",
                    F.when(F.col("profile_creation_date") >= timestamp, 1)
                    .otherwise(0))
        .withColumn("default_client",
                    F.when(F.col("is_default_browser"), 1)
                    .otherwise(0))
        .withColumn("clientid_rank", F.row_number()
                    .over(Window
                          .partitionBy("client_id")
                          .orderBy(F.desc("submission_date"))))
        .where("clientid_rank = 1")
        .groupBy(attributes)
        .agg(
            F.count("*").alias("actives"),
            F.sum("new_client").alias("new_records"),
            F.sum("default_client").alias("default"))
    )

    return clients


def generate_aggregates(dataframe, start, end):

    # attributes that break down the aggregates
    attributes = ["country", "channel", "os"]

    # clean the dataset
    df = clean_input(dataframe, start, end)

    # find the timestamp in days to find new profiles
    report_delta = datetime.strptime(start, "%Y%m%d") - datetime(1970, 1, 1)
    report_timestamp = report_delta.total_seconds() / seconds_per_day

    # generate aggregates
    clients = client_aggregates(df, report_timestamp, attributes)
    searches = search_aggregates(df, attributes)
    hours = hours_aggregates(df, attributes)

    # take the outer join of all aggregates and fix holes
    return (
        clients
        .join(searches, attributes, "outer")
        .join(hours, attributes, "outer")
        .na.fill(0)
    )
