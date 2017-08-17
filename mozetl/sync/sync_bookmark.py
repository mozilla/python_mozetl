# coding: utf-8

# # Unnest engine and validation data
#
# Flatten the nested engine and engine validation data into a table, and filter out failed syncs. We won't have validation results for these.

# In[ ]:

from os import environ
from datetime import datetime, timedelta

manual = False
submission_start_window = None
submission_end_window = None
if manual:
    submission_start_window = "20170516"
    submission_end_window = "20170611"
else:
    date = (datetime.strptime(env["date"], "%Y%m%d") if "date" in environ else datetime.now()) - timedelta(10)
    submission_start_window = submission_end_window = datetime.strftime(date, "%Y%m%d")


# In[ ]:

from IPython.display import display

dataset_path = "s3://telemetry-parquet/sync_summary/v1/"
summary = spark.read.parquet(dataset_path)
summary.createOrReplaceTempView("sync_summary")

all_engine_validation_results = spark.sql("""
SELECT s.app_build_id, s.app_version, s.app_display_version, s.app_name,
       s.app_channel, s.uid, s.deviceID as device_id,
       s.submission_date_s3 AS submission_day,
       date_format(from_unixtime(s.when / 1000), 'YYYYMMdd') AS sync_day,
       s.when,
       s.status,
       e.name AS engine_name,
       e.status AS engine_status,
       e.failureReason AS engine_failure_reason,
       e.validation.problems IS NOT NULL AS engine_has_problems,
       e.validation.version AS engine_validation_version,
       e.validation.checked AS engine_validation_checked,
       e.validation.took AS engine_validation_took,
       p.name AS engine_validation_problem_name,
       p.count AS engine_validation_problem_count
FROM sync_summary s
LATERAL VIEW explode(s.engines) AS e
LATERAL VIEW OUTER explode(e.validation.problems) AS p
WHERE s.failureReason IS NULL
""")

# Summary pings are partitioned by submission day. Most pings are
# collected within the first 8 hours of a client running for a day;
# it takes about 10 days after the submission date for all pings to
# arrive for that day.
submission_day_col = all_engine_validation_results["submission_day"]
engine_validation_results = all_engine_validation_results.filter(
    submission_day_col == submission_start_window
).filter(
    submission_day_col <= submission_end_window
)
engine_validation_results.createOrReplaceTempView("sync_engine_validation_results")


# In[ ]:

bmk_validation_results = engine_validation_results.filter(
    engine_validation_results["engine_name"] == "bookmarks"
)


# In[ ]:

# Bookmark validations with problems.
bmk_validation_problems = bmk_validation_results.filter(
    bmk_validation_results["engine_has_problems"]
)


# In[ ]:

from pyspark.sql import functions as F

# All bookmark validations, including without problems.
bmk_total_per_day = bmk_validation_results.filter(
    bmk_validation_results["engine_validation_checked"].isNotNull()
).groupBy("sync_day").agg(
    F.countDistinct("uid", "device_id", "when").alias("total_bookmark_validations"),
    F.countDistinct("uid").alias("total_validated_users"),
    F.sum("engine_validation_checked").alias("total_bookmarks_checked")
)


# In[ ]:

problems_path = "s3://net-mozaws-prod-us-west-2-pipeline-analysis/kit/sync_bmk_validation_problems/v1/run_start_date=%s" % submission_start_window
bmk_validation_problems.repartition(250).write.format("parquet").mode("overwrite").save(problems_path)

total_per_day_path = "s3://net-mozaws-prod-us-west-2-pipeline-analysis/kit/sync_bmk_total_per_day/v1/run_start_date=%s" % submission_start_window
bmk_total_per_day.repartition(250).write.format("parquet").mode("overwrite").save(total_per_day_path)
