from copy import deepcopy

import pytest
from pyspark.sql import functions as F

from mozetl.landfill import sampler


@pytest.fixture
def sample_document():
    return {
        'content': {"payload": {"foo": "bar"}},
        'meta': {
            u'Content-Length': u'7094',
            u'Date': u'Sun, 19 Aug 2018 15:08:00 GMT',
            u'Host': u'incoming.telemetry.mozilla.org',
            'Hostname': u'ip-1.1.1.1',
            'Timestamp': 1534691279765301222,
            'Type': u'telemetry-raw',
            u'User-Agent': u'pingsender/1.0',
            u'X-Forwarded-For': u'127.0.0.1',
            u'X-PingSender-Version': u'1.0',
            u'args': u'v=4',
            u'protocol': u'HTTP/1.1',
            u'remote_addr': u'1.1.1.1',
            u'uri': u'/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231'
        }
    }


@pytest.fixture
def generate_data(spark, sample_document):
    """Update the meta-field using provided snippets."""
    def _update_meta(snippet):
        doc = deepcopy(sample_document)
        doc["meta"].update(snippet)
        return doc

    def _generate_data(snippets):
        return spark.sparkContext.parallelize(list(map(_update_meta, snippets)))

    return _generate_data


def build_generic_uri(namespace, doc_type, doc_version, doc_id):
    return "/submit/{}/{}/{}/{}".format(namespace, doc_type, doc_version, doc_id)


def test_process_namespace_telemetry(sample_document):
    # /submit/<namespace>/<doc_id>/<doc_type>/<app_name>/<app_version>/<app_channel>/<app_build_id>
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    row = sampler._process(sample_document)
    assert row[:4] == ("telemetry", "main", "4", "doc-id")


def test_process_namespace_generic(sample_document):
    # /submit/<namespace>/<doc_type>/<doc_version>/<doc_id>
    uri = "/submit/namespace/doc-type/doc-version/doc-id"
    sample_document["meta"]["uri"] = uri
    row = sampler._process(sample_document)
    assert row[:4] == ("namespace", "doc-type", "doc-version", "doc-id")


def test_process_meta_ignores_identifiable_information(sample_document):
    row = sampler._process(sample_document)
    meta = row[4]
    intersection = {"Hostname", "remote_addr", "X-Forwarded-For"} & set(meta.keys())
    assert len(intersection) == 0


def test_transform_contains_at_most_n_documents(generate_data):
    n = 3
    params = [
        # (namespace, doc_type, doc_version, n_documents)
        ("custom", "main", 4, n+1),
        ("custom", "main", 3, n-1),
        ("custom", "crash", 4, n)
    ]
    snippets = []
    for ns, dt, dv, n in params:
        # generate n different generic uri's for each of the parameters
        snippets += [{"uri": build_generic_uri(ns, dt, dv, uid)} for uid in range(n)]

    df = sampler.transform(generate_data(snippets), n)

    # assert there are at most n documents
    res = (
        df
        .groupBy("namespace", "doc_type", "doc_version")
        .agg(F.count("*").alias("n_documents"))
        .withColumn("at_most_n", F.col("n_documents") <= n)
        .collect()
    )

    # check that the different combinations are the same as the generated ones
    assert len(res) == len(params)
    # check that all of the combinations have at most n documents
    assert all([row.at_most_n for row in res])
