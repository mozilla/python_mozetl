import datetime as DT
import os
import pytest
import tempfile
from pyspark.sql.types import StructField, StructType, StringType
from mozetl.maudau import maudau as M
from mozetl.utils import format_as_submission_date

NARROW_SCHEMA = StructType([
    StructField("client_id",             StringType(),  True),
    StructField("submission_date_s3",    StringType(),  False),
    StructField("subsession_start_date", StringType(), True)])

generated = format_as_submission_date(DT.date.today())


@pytest.fixture
def make_frame(spark):
    cols = ['client_id', 'subsession_start_date', 'submission_date_s3']
    values = [
        ('a', '2017-05-01', '20170508'),
        ('b', '2017-05-01', '20170501'),
        ('c', '2017-05-01', '20170510'),
        ('a', '2017-05-02', '20170503'),
        ('b', '2017-05-03', '20170503'),
        ('b', '2017-05-04', '20170511'),
        ('a', '2017-05-05', '20170507'),
    ]
    return spark.createDataFrame(
        [dict(list(zip(cols, tup))) for tup in values],
        schema=NARROW_SCHEMA)


def test_generate_counts(spark):
    frame = make_frame(spark)
    since = DT.date(2017, 5, 1)
    counts = M.generate_counts(frame, since, DT.date(2017, 5, 6))
    expected = {'day': '20170505', 'dau': 1, 'mau': 3,
                'generated_on': generated}
    assert counts[-1] == expected, str(counts[-1])
    counts2 = M.generate_counts(frame, since, DT.date(2017, 5, 4))
    expected2 = {'day': '20170503', 'dau': 1, 'mau': 3,
                 'generated_on': generated}
    assert counts2[-1] == expected2, str(counts2[-1])


def test_parse_last_rollup():
    tempdir = tempfile.mkdtemp()
    cwd = os.getcwd()
    basename = "engagement_ratio.csv"
    # Block1: test that we keep data > 10 days but regenerate newer data.
    data = '\r\n'.join([
        "day,dau,mau,generated_on",
        "20170503,1,3,20170620",
        "20170504,1,3,20170620",
        ""
        ])
    expected = (
        DT.date(2017, 5, 4),
        [{'dau': '1', 'mau': '3', 'day': '20170503',
          'generated_on': '20170620'}]
    )
    try:
        os.chdir(tempdir)
        with open(basename, 'w') as f:
            f.write(data)
        actual = M.parse_last_rollup(basename, DT.date(2017, 5, 14))
        assert actual == expected
    finally:
        os.chdir(cwd)
    # Block2: test that we regenerate old data if we hit a gap.
    # Here, 2017-05-02 is missing.
    data2 = '\r\n'.join([
        "day,dau,mau,generated_on",
        "20170501,1,3,20170620",
        "20170503,1,3,20170620",
        "20170504,1,3,20170620",
        ""
        ])
    expected2 = (
        DT.date(2017, 5, 2),
        [{'dau': '1', 'mau': '3', 'day': '20170501',
          'generated_on': '20170620'}]
    )
    try:
        os.chdir(tempdir)
        with open(basename, 'w') as f:
            f.write(data2)
        actual2 = M.parse_last_rollup(basename, DT.date(2017, 5, 14))
        assert actual2 == expected2
    finally:
        os.chdir(cwd)


def test_write_locally():
    tempdir = tempfile.mkdtemp()
    cwd = os.getcwd()
    results = [
        {'day': '20170503', 'dau': 1, 'mau': 3,
         'generated_on': generated},
        {'day': '20170504', 'dau': 1, 'mau': 3,
         'generated_on': generated}
    ]
    expected = '\r\n'.join([
        "day,dau,mau,generated_on",
        "20170503,1,3,{}".format(generated),
        "20170504,1,3,{}".format(generated),
        ""
        ])
    basename = "engagement_ratio.{}.csv".format(generated)
    try:
        os.chdir(tempdir)
        M.write_locally(results)
        output = open(basename).read()
        assert output == expected
    finally:
        os.chdir(cwd)
