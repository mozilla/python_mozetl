from copy import deepcopy

import json
import pytest
from pyspark.sql import functions as F

from mozetl.landfill import sampler


@pytest.fixture
def sample_document():
    return {
        # The "content" field is provided here as a dict for
        # convenience, but should be serialized to a JSON
        # string before passing through to the sampler code.
        "content": {"payload": {"foo": "bar"}},
        "meta": {
            "Content-Length": "7094",
            "Date": "Sun, 19 Aug 2018 15:08:00 GMT",
            "Host": "incoming.telemetry.mozilla.org",
            "Hostname": "ip-1.1.1.1",
            "Timestamp": 1534691279765301222,
            "Type": "telemetry-raw",
            "User-Agent": "pingsender/1.0",
            "X-Forwarded-For": "127.0.0.1",
            "X-PingSender-Version": "1.0",
            "args": "v=4",
            "protocol": "HTTP/1.1",
            "remote_addr": "1.1.1.1",
            "uri": "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231",
        },
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


def serialize_content(unserialized):
    """Encode the content field as JSON"""
    doc = deepcopy(unserialized)
    doc["content"] = json.dumps(doc["content"])
    return doc


def test_detect_version_bad_json():
    v = sampler._detect_telemetry_version("bla")
    assert v == "0"


def test_detect_version_good_json():
    v = sampler._detect_telemetry_version("{}")
    assert v == "1"


def test_process_namespace_telemetry(sample_document):
    # /submit/<namespace>/<doc_id>/<doc_type>/<app_name>/<app_version>/<app_channel>/<app_build_id>
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "1", "doc-id")


def test_process_namespace_telemetry_deviceinfo(sample_document):
    uri = "/submit/telemetry/doc-id/appusage/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    sample_document["content"]["deviceinfo"] = "foo"
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "appusage", "3", "doc-id")


def test_process_namespace_telemetry_version(sample_document):
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    sample_document["content"]["version"] = 4
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "4", "doc-id")
    sample_document["content"]["version"] = 5
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "5", "doc-id")


def test_process_namespace_telemetry_ver(sample_document):
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    sample_document["content"]["ver"] = 6
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "6", "doc-id")


def test_process_namespace_telemetry_v(sample_document):
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    sample_document["content"]["v"] = 7
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "7", "doc-id")


def test_process_namespace_telemetry_ver_version_v(sample_document):
    uri = "/submit/telemetry/doc-id/main/Firefox/61.0.2/release/20180807170231"
    sample_document["meta"]["uri"] = uri
    # Populate all the version-related fields.
    sample_document["content"]["ver"] = 8
    sample_document["content"]["version"] = 9
    sample_document["content"]["v"] = 10
    sample_document["content"]["deviceinfo"] = "foo"
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("telemetry", "main", "8", "doc-id")


def test_process_namespace_generic(sample_document):
    # /submit/<namespace>/<doc_type>/<doc_version>/<doc_id>
    uri = "/submit/namespace/doc-type/doc-version/doc-id"
    sample_document["meta"]["uri"] = uri
    row = sampler._process(serialize_content(sample_document))
    assert row[:4] == ("namespace", "doc-type", "doc-version", "doc-id")


def test_process_meta_ignores_identifiable_information(sample_document):
    row = sampler._process(serialize_content(sample_document))
    meta = row[4]
    intersection = {"Hostname", "remote_addr", "X-Forwarded-For"} & set(meta.keys())
    assert len(intersection) == 0


def test_transform_contains_at_most_n_documents(generate_data):
    n = 3
    params = [
        # (namespace, doc_type, doc_version, n_documents)
        ("custom", "main", 4, n + 1),
        ("custom", "main", 3, n - 1),
        ("custom", "crash", 4, n),
    ]
    snippets = []
    for ns, dt, dv, n in params:
        # generate n different generic uri's for each of the parameters
        snippets += [{"uri": build_generic_uri(ns, dt, dv, uid)} for uid in range(n)]

    df = sampler.transform(generate_data(snippets), n)

    # assert there are at most n documents
    res = (
        df.groupBy("namespace", "doc_type", "doc_version")
        .agg(F.count("*").alias("n_documents"))
        .withColumn("at_most_n", F.col("n_documents") <= n)
        .collect()
    )

    # check that the different combinations are the same as the generated ones
    assert len(res) == len(params)
    # check that all of the combinations have at most n documents
    assert all([row.at_most_n for row in res])
