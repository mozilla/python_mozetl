import json
import os

from click.testing import CliRunner

import pytest
from mozetl.topline import historical_backfill as backfill
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    spark = (SparkSession
             .builder
             .appName("test_topline_historical_backfill")
             .getOrCreate())

    # teardown
    request.addfinalizer(lambda: spark.stop())

    return spark


default_sample = {
    "geo": "US",
    "channel": "nightly",
    "os": "Windows",
    "date": "2016-01-01",
    "actives": "1",
    "hours": "1",
    "inactives": "1",
    "new_records": "1",
    "five_of_seven": "1",
    "total_records": "1",
    "crashes": "1",
    "default": "1",
    "google": "1",
    "bing": "1",
    "yahoo": "1",
    "other": "1"
}


def generate_samples(snippets, base_sample):
    if snippets is None:
        return [json.dumps(base_sample)]

    samples = []
    for snippet in snippets:
        sample = base_sample.copy()
        sample.update(snippet)
        samples.append(json.dumps(sample))
    return samples


def snippets_to_df(spark, snippets, base_sample, schema):
    samples = generate_samples(snippets, base_sample)
    jsonRDD = spark.sparkContext.parallelize(samples)
    return spark.read.json(jsonRDD, schema=schema)


# does not include all rows from original data
def test_excludes_all_rows(spark, tmpdir):
    snippets = [
        {'geo': 'all'},
        {'os': 'all'},
        {'channel': 'all'},
        {}  # There must be a single data point
    ]
    input_df = snippets_to_df(spark, snippets, default_sample,
                              backfill.historical_schema)
    path = str(tmpdir.join('test/mode=weekly/'))
    backfill.backfill_topline_summary(input_df, path)

    df = spark.read.parquet(path)
    assert df.where("geo = 'all'").count() == 0
    assert df.where("os = 'all'").count() == 0
    assert df.where("channel = 'all'").count() == 0


# writes out partitions by report_date
def test_partitions_by_report_date(spark, tmpdir):
    snippets = [
        {'date': '2016-01-01'},
        {'date': '2016-01-08'}
    ]
    input_df = snippets_to_df(spark, snippets, default_sample,
                              backfill.historical_schema)
    outdir = tmpdir.join('test/mode=weekly')
    path = str(outdir)
    backfill.backfill_topline_summary(input_df, path)

    # number of folders are correct
    partitions = [s for s in os.listdir(path) if s.startswith('report_start')]
    assert len(partitions) == 2

    # folder names are correct
    dates = set([p.split('=')[-1] for p in partitions])
    assert dates == set(['20160101', '20160108'])


# data is correctly written to the correct location given default prefix
def test_cli_monthly(spark, tmpdir, monkeypatch):
    # generate test data
    snippets = [
        {'date': '2016-01-01'},
        {'date': '2016-01-08'}
    ]
    input_df = snippets_to_df(spark, snippets, default_sample,
                              backfill.historical_schema)

    # add a csv file to the test folder
    toplevel = tmpdir
    input_csv = toplevel.join('test.csv')

    csv_data = ','.join(input_df.columns) + '\n'
    for row in input_df.collect():
        csv_data += ','.join([unicode(x) for x in row]) + '\n'

    input_csv.write(csv_data)

    # create the output directory
    testdir = toplevel.join('test')
    output_path = str(testdir)

    # change s3_path to use file:// protocol
    def mock_create_s3_path(bucket, prefix):
        return "file://{}/{}".format(bucket, prefix)
    monkeypatch.setattr(backfill, 'create_s3_path', mock_create_s3_path)

    # Run the application via the cli
    runner = CliRunner()
    args = [
        'file://' + str(input_csv),
        'monthly',
        output_path
    ]
    result = runner.invoke(backfill.main, args)
    assert result.exit_code == 0

    # test that the results can be read via spark
    path = str(testdir.join('topline_summary/v1/mode=monthly'))
    df = spark.read.parquet(path)
    assert df.count() == 2
