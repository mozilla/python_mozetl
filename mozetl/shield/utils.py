from datetime import date, timedelta
from moztelemetry.dataset import Dataset


def shield_etl_boilerplate(transform_func, s3_path):
    def etl_job(sc, sqlContext, submission_date=None, save=True):
        if submission_date is None:
            submission_date = (date.today() - timedelta(1)).strftime("%Y%m%d")

        pings = (
            Dataset.from_source("telemetry")
            .where(
                docType="shield-study",
                submissionDate=submission_date,
                appName="Firefox",
            )
            .records(sc)
        )

        transformed_pings = transform_func(sqlContext, pings)

        if save:
            path = s3_path + "/submission_date={}".format(submission_date)
            transformed_pings.repartition(1).write.mode("overwrite").parquet(path)

        return transformed_pings

    return etl_job
