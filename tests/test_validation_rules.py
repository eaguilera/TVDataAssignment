# tests/test_validation_rules.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from scripts.ingest_and_validate import apply_validation_rules


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("tvs-assignment-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def base_schema() -> StructType:
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("content_id", StringType(), True),
            StructField("tenant", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("device_type", StringType(), True),
        ]
    )


def collect_single(df):
    rows = df.collect()
    assert len(rows) == 1
    return rows[0]


def test_missing_end_time_flagged(spark, base_schema):
    data = [
        ("e1", "u1", "c1", "swe", "linear", "2024-01-01T10:00:00", None, "tv"),
    ]
    df = spark.createDataFrame(data, schema=base_schema)

    result = apply_validation_rules(df)
    row = collect_single(result)

    assert row.is_valid is False
    assert "MISSING_END_TIME" in row.validation_reasons


def test_negative_duration_flagged(spark, base_schema):
    data = [
        ("e2", "u1", "c1", "swe", "linear",
         "2024-01-01T11:00:00", "2024-01-01T10:00:00", "tv"),
    ]
    df = spark.createDataFrame(data, schema=base_schema)
    result = apply_validation_rules(df)
    row = collect_single(result)

    assert row.is_valid is False
    assert "NEGATIVE_DURATION" in row.validation_reasons


def test_invalid_tenant_flagged(spark, base_schema):
    data = [
        ("e3", "u1", "c1", "deu", "linear",
         "2024-01-01T10:00:00", "2024-01-01T11:00:00", "tv"),
    ]
    df = spark.createDataFrame(data, schema=base_schema)
    result = apply_validation_rules(df)
    row = collect_single(result)

    assert row.is_valid is False
    assert "INVALID_TENANT" in row.validation_reasons


def test_no_schedule_match_flagged_for_linear(spark, base_schema):
    # This assumes apply_validation_rules has already added has_schedule_match
    # (e.g. via a previous join). Here we add it manually for the test.
    data = [
        ("e4", "u1", "c1", "swe", "linear",
         "2024-01-01T10:00:00", "2024-01-01T11:00:00", "tv"),
    ]
    df = spark.createDataFrame(data, schema=base_schema)
    df = df.withColumn("has_schedule_match", F.lit(False))

    result = apply_validation_rules(df)
    row = collect_single(result)

    assert row.is_valid is False
    assert "NO_SCHEDULE_MATCH" in row.validation_reasons


def test_valid_event_has_no_reasons(spark, base_schema):
    data = [
        ("e5", "u1", "c1", "swe", "linear",
         "2024-01-01T10:00:00", "2024-01-01T11:00:00", "tv"),
    ]
    df = spark.createDataFrame(data, schema=base_schema)
    df = df.withColumn("has_schedule_match", F.lit(True))

    result = apply_validation_rules(df)
    row = collect_single(result)

    assert row.is_valid is True
    assert row.validation_reasons == [] or row.validation_reasons is None
    assert row.validation_reason in (None, "")
