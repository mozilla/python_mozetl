import arrow
import pytest
from pyspark.sql import functions as F

from mozetl.engagement.retention import job, schema
from . import data
from .data import WEEK_START_DS, SUBSESSION_START


@pytest.fixture()
def test_transform(generate_main_summary_data):
    def _test_transform(snippets, week_start=WEEK_START_DS):
        return job.transform(
            generate_main_summary_data(snippets),
            week_start
        )

    return _test_transform


def test_transform_adheres_to_schema(test_transform):
    df = test_transform(None)

    def column_types(schema):
        return {
            col.name: col.dataType.typeName()
            for col in schema.fields
        }

    assert column_types(df.schema) == column_types(schema.retention_schema)


def test_transform_valid_profile_creation(test_transform):
    ssd = 'subsession_start_date'
    pcd = 'profile_creation_date'

    df = test_transform([
        {
            'client_id': '1',
            ssd: data.format_ssd(arrow.get(1999, 1, 1))
        },
        {
            'client_id': '2',
            pcd: None
        },
        {
            'client_id': '3',
            ssd: data.format_ssd(SUBSESSION_START),
            pcd: data.format_pcd(SUBSESSION_START.shift(days=3))
        },
        {
            'client_id': '4',
            ssd: data.format_ssd(SUBSESSION_START),
            pcd: data.format_pcd(SUBSESSION_START.shift(days=-3))
        }
    ])

    assert df.count() == 4
    assert (
        df
        .where(F.col("profile_creation") == F.lit(job.DEFAULT_DATE))
        .count()
    ) == 3
    assert (
        df.where(df.client_id == '4').first().profile_creation
    ) == '2017-01-12'
