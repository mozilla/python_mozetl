import os
import pytest
from click.testing import CliRunner
from pyspark.sql import Row
from mozetl import system_check


def test_job(spark, monkeypatch, tmpdir):
    ds_nodash = "20190201"

    test_bucket = str(tmpdir)
    test_prefix = "main_summary/v4"
    test_output_prefix = "mozetl_system_check"

    test_data = spark.createDataFrame(
        [
            Row(
                submission_date_s3=ds_nodash,
                sample_id=1,
                memory_mb=2048,
                cpu_cores=2,
                subsession_length=1,
            ),
            Row(
                submission_date_s3=ds_nodash,
                sample_id=2,
                memory_mb=2048,
                cpu_cores=2,
                subsession_length=1,
            ),
            Row(
                submission_date_s3="20180101",
                sample_id=1,
                memory_mb=2048,
                cpu_cores=2,
                subsession_length=1,
            ),
        ]
    )
    test_data.write.parquet(
        "file://{}/{}/".format(test_bucket, test_prefix),
        partitionBy=["submission_date_s3", "sample_id"],
    )

    def mock_format_spark_path(bucket, prefix):
        return "file://{}/{}".format(bucket, prefix)

    monkeypatch.setattr(system_check, "format_spark_path", mock_format_spark_path)

    runner = CliRunner()
    args = [
        "--submission-date-s3",
        ds_nodash,
        "--input-bucket",
        test_bucket,
        "--input-prefix",
        test_prefix,
        "--output-bucket",
        test_bucket,
        "--output-prefix",
        test_output_prefix,
    ]
    result = runner.invoke(system_check.main, args)
    assert result.exit_code == 0

    # assert path was written
    assert os.path.isdir(
        "{}/{}/submission_date_s3={}".format(test_bucket, test_output_prefix, ds_nodash)
    )

    # assert data can be read
    path = mock_format_spark_path(test_bucket, test_output_prefix)
    df = spark.read.parquet(path)
    df.show()
    assert df.count() > 0
