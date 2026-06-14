"""MiniCluster smoke test for the 1-minute tumbling aggregation (FR-016 / SC-007).

An out-of-order event must still land in the correct minute window. We assert on
the per-campaign set of window counts (not timestamps) so the test is robust to
how PyFlink renders window-start values.

Run: cd stream/flink-aggregator && pip install -r requirements.txt && python -m pytest -q
(apache-flink bundles the Flink runtime + planner; the synthetic source needs no
external connector jars.)
"""

import pytest

pytest.importorskip("pyflink")

from pyflink.common import Duration, Types, WatermarkStrategy  # noqa: E402
from pyflink.common.watermark_strategy import TimestampAssigner  # noqa: E402
from pyflink.datastream import StreamExecutionEnvironment  # noqa: E402
from pyflink.table import Schema, StreamTableEnvironment  # noqa: E402
from pyflink.table.expressions import col  # noqa: E402  (ensures table api is importable)

from datetime import datetime, timezone  # noqa: E402


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


class _EpochMillisAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


def test_counts_per_minute_window_with_out_of_order_events():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)

    # f0 = campaign_id, f1 = event time (epoch millis).
    rows = [
        ("camp_1", ms("2026-06-13T14:07:10Z")),
        ("camp_1", ms("2026-06-13T14:07:50Z")),
        ("camp_1", ms("2026-06-13T14:07:05Z")),  # out of order
        ("camp_1", ms("2026-06-13T14:08:01Z")),
        ("camp_2", ms("2026-06-13T14:07:30Z")),
    ]
    ds = env.from_collection(rows, type_info=Types.ROW([Types.STRING(), Types.LONG()]))
    ds = ds.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(_EpochMillisAssigner())
    )

    schema = (
        Schema.new_builder()
        .column_by_expression("campaign_id", "f0")
        .column_by_expression("click_time", "TO_TIMESTAMP_LTZ(f1, 3)")
        .watermark("click_time", "SOURCE_WATERMARK()")
        .build()
    )
    table = t_env.from_data_stream(ds, schema)
    t_env.create_temporary_view("clicks_in", table)

    windowed = t_env.sql_query(
        """
        SELECT campaign_id, window_start, COUNT(*) AS c
        FROM TABLE(TUMBLE(TABLE clicks_in, DESCRIPTOR(click_time), INTERVAL '1' MINUTE))
        GROUP BY campaign_id, window_start, window_end
        """
    )

    per_campaign = {}
    with windowed.execute().collect() as it:
        for row in it:
            per_campaign.setdefault(row[0], []).append(row[2])

    # camp_1: one 14:07 window with 3 (incl. the out-of-order click) + one 14:08 window with 1.
    assert sorted(per_campaign["camp_1"]) == [1, 3]
    # camp_2: a single 14:07 window with 1.
    assert sorted(per_campaign["camp_2"]) == [1]
