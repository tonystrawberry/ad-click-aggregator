package com.adclick;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

/**
 * MiniCluster smoke test: out-of-order events still land in the correct minute
 * bucket and counts per (campaign, minute) are correct (FR-016 / SC-007).
 *
 * The synthetic source is a DataStream with an assigned event-time watermark
 * (30s bounded out-of-orderness, mirroring the job), converted to a Table with a
 * rowtime attribute so the TUMBLE window function can be applied — the same shape
 * the Kinesis source produces in production.
 */
class WindowingTest {

    private static long ms(String iso) {
        return Instant.parse(iso).toEpochMilli();
    }

    @Test
    void countsPerMinuteWindowAreCorrectWithOutOfOrderEvents() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Three clicks for camp_1 in minute 14:07 (one out of order), one for
        // camp_1 in 14:08, one for camp_2 in 14:07. Field f0=campaign, f1=epoch ms.
        DataStream<Tuple2<String, Long>> ds = env.fromElements(
                Tuple2.of("camp_1", ms("2026-06-13T14:07:10Z")),
                Tuple2.of("camp_1", ms("2026-06-13T14:07:50Z")),
                Tuple2.of("camp_1", ms("2026-06-13T14:07:05Z")), // out of order
                Tuple2.of("camp_1", ms("2026-06-13T14:08:01Z")),
                Tuple2.of("camp_2", ms("2026-06-13T14:07:30Z")))
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((e, t) -> e.f1));

        Table input = tEnv.fromDataStream(ds, Schema.newBuilder()
            .columnByExpression("campaign_id", "f0")
            .columnByExpression("click_time", "TO_TIMESTAMP_LTZ(f1, 3)")
            .watermark("click_time", "SOURCE_WATERMARK()")
            .build());
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

        assertEquals(3L, got.get("camp_1@" + Instant.parse("2026-06-13T14:07:00Z")));
        assertEquals(1L, got.get("camp_1@" + Instant.parse("2026-06-13T14:08:00Z")));
        assertEquals(1L, got.get("camp_2@" + Instant.parse("2026-06-13T14:07:00Z")));
    }
}
