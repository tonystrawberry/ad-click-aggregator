package com.adclick;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * MiniCluster smoke test: out-of-order events still land in the correct minute
 * bucket and counts per (campaign, minute) are correct (FR-016 / SC-007).
 */
class WindowingTest {

    @Test
    void countsPerMinuteWindowAreCorrectWithOutOfOrderEvents() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Three clicks for camp_1 in minute 14:07 (one arrives out of order),
        // one click for camp_1 in minute 14:08, one for camp_2 in 14:07.
        // A deterministic VALUES table stands in for the Kinesis source.
        Table input = tEnv.fromValues(
            org.apache.flink.table.api.DataTypes.ROW(
                org.apache.flink.table.api.DataTypes.FIELD("campaign_id",
                    org.apache.flink.table.api.DataTypes.STRING()),
                org.apache.flink.table.api.DataTypes.FIELD("click_time",
                    org.apache.flink.table.api.DataTypes.TIMESTAMP(3))),
            org.apache.flink.table.api.Expressions.row("camp_1", LocalDateTime.parse("2026-06-13T14:07:10")),
            org.apache.flink.table.api.Expressions.row("camp_1", LocalDateTime.parse("2026-06-13T14:07:50")),
            org.apache.flink.table.api.Expressions.row("camp_1", LocalDateTime.parse("2026-06-13T14:07:05")), // out of order
            org.apache.flink.table.api.Expressions.row("camp_1", LocalDateTime.parse("2026-06-13T14:08:01")),
            org.apache.flink.table.api.Expressions.row("camp_2", LocalDateTime.parse("2026-06-13T14:07:30")));

        tEnv.createTemporaryView("clicks_in", input);

        Table windowed = tEnv.sqlQuery(
            "SELECT campaign_id, window_start, COUNT(*) AS c "
          + "FROM TABLE(TUMBLE(TABLE clicks_in, DESCRIPTOR(click_time), INTERVAL '1' MINUTE)) "
          + "GROUP BY campaign_id, window_start, window_end");

        Map<String, Long> got = new HashMap<>();
        try (CloseableIterator<Row> it = windowed.execute().collect()) {
            while (it.hasNext()) {
                Row r = it.next();
                String key = r.getField("campaign_id") + "@" + r.getField("window_start");
                got.put(key, ((Number) r.getField("c")).longValue());
            }
        }

        assertEquals(3L, got.get("camp_1@2026-06-13T14:07"));
        assertEquals(1L, got.get("camp_1@2026-06-13T14:08"));
        assertEquals(1L, got.get("camp_2@2026-06-13T14:07"));
    }
}
