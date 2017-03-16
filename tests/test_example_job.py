import pytest
from pyspark import SparkConf, SparkContext
from python_etl.example_job import *

# Initialize a spark context:
@pytest.fixture(scope="session")
def spark_context(request):
    conf = SparkConf().setMaster("local")\
        .setAppName("python_etl" + "_test")
    sc = SparkContext(conf=conf)

    # teardown
    request.addfinalizer(lambda: sc.stop())
    
    return sc


# Generate some data
def create_row(client_id, os):
    return {'clientId': client_id, 'environment/system/os/name': os}

def simple_data():
    raw_data = [('a', 'windows'),
                ('b', 'darwin'),
                ('c', 'linux'),
                ('d', 'windows')]

    return map(lambda raw: create_row(*raw), raw_data)

def duplicate_data():
    return simple_data() + simple_data()

@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize(simple_data())

@pytest.fixture
def duplicate_rdd(spark_context):
    return spark_context.parallelize(duplicate_data())

# Tests
def test_simple_transform(simple_rdd):
    actual = transform_pings(simple_rdd)
    expected = {'windows':2, 'darwin': 1, 'linux':1}

    assert actual == expected

def test_duplicate_transform(duplicate_rdd):
    actual = transform_pings(duplicate_rdd)
    expected = {'windows':2, 'darwin': 1, 'linux':1}

    assert actual == expected
