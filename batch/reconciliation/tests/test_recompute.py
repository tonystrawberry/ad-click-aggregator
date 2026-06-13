"""Unit tests for the pure reconciliation transform (SC-004).

Run: cd batch/reconciliation && python -m pytest -q
Requires a local pyspark (requirements-dev.txt).
"""

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

import sys, os  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from job import recompute  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("recompute-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def _raw(spark, rows):
    cols = ["impression_id", "campaign_id", "minute_bucket", "ingest_ts"]
    return spark.createDataFrame(rows, cols)


def test_dedups_by_impression_id(spark):
    # Same impression delivered three times → counts once (SC-005).
    rows = [
        ("imp_1", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:01Z"),
        ("imp_1", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:02Z"),
        ("imp_1", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:03Z"),
    ]
    out = {(r["campaign_id"], r["minute_bucket"]): r["click_count"]
           for r in recompute(_raw(spark, rows)).collect()}
    assert out[("c1", "2026-06-13T14:07:00Z")] == 1


def test_counts_per_campaign_and_minute(spark):
    rows = [
        ("imp_1", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:01Z"),
        ("imp_2", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:02Z"),
        ("imp_3", "c1", "2026-06-13T14:08:00Z", "2026-06-13T14:08:01Z"),
        ("imp_4", "c2", "2026-06-13T14:07:00Z", "2026-06-13T14:07:05Z"),
    ]
    out = {(r["campaign_id"], r["minute_bucket"]): r["click_count"]
           for r in recompute(_raw(spark, rows)).collect()}
    assert out[("c1", "2026-06-13T14:07:00Z")] == 2
    assert out[("c1", "2026-06-13T14:08:00Z")] == 1
    assert out[("c2", "2026-06-13T14:07:00Z")] == 1


def test_late_event_lands_in_its_own_minute(spark):
    # An out-of-order/late click still attributes to its minute_bucket (FR-016).
    rows = [
        ("imp_1", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:09:00Z"),  # arrived late
        ("imp_2", "c1", "2026-06-13T14:07:00Z", "2026-06-13T14:07:02Z"),
    ]
    out = {(r["campaign_id"], r["minute_bucket"]): r["click_count"]
           for r in recompute(_raw(spark, rows)).collect()}
    assert out[("c1", "2026-06-13T14:07:00Z")] == 2
