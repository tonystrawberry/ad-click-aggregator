package com.adclick;

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;

/**
 * Stream aggregator (research D4). Reads click events from Kinesis, maintains
 * 1-minute event-time tumbling windows keyed by campaign_id, and upserts the
 * per-window counts into Redshift via {@link RedshiftUpsertSink}.
 *
 * Event-time + a bounded watermark attribute late/out-of-order clicks to the
 * correct minute (FR-016). Tumbling 1-minute windows match the minimum
 * aggregation granularity (FR-003).
 *
 * Runtime properties (Managed Service for Apache Flink "FlinkAppProperties" group):
 *   stream.name, aws.region, scan.initpos, redshift.jdbc.url,
 *   redshift.user, redshift.password, sink.batch.size
 */
public class ClickAggregatorJob {

    public static void main(String[] args) throws Exception {
        Properties cfg = loadConfig();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Checkpoint so failures resume without losing/duplicating counts (Principle IV).
        env.enableCheckpointing(60_000L);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(sourceDdl(cfg));

        // 1-minute tumbling window per campaign over event time.
        Table windowed = tEnv.sqlQuery(
            "SELECT campaign_id, "
          + "       window_start AS minute_bucket, "
          + "       COUNT(*) AS click_count "
          + "FROM TABLE("
          + "  TUMBLE(TABLE click_events, DESCRIPTOR(click_time), INTERVAL '1' MINUTE)"
          + ") "
          + "GROUP BY campaign_id, window_start, window_end");

        DataStream<MinuteCount> counts = tEnv.toDataStream(windowed)
            .map(row -> new MinuteCount(
                (String) row.getField("campaign_id"),
                java.sql.Timestamp.valueOf(
                    ((java.time.LocalDateTime) row.getField("minute_bucket"))),
                ((Number) row.getField("click_count")).longValue()))
            .returns(MinuteCount.class);

        counts.addSink(new RedshiftUpsertSink(
            cfg.getProperty("redshift.jdbc.url"),
            cfg.getProperty("redshift.user"),
            cfg.getProperty("redshift.password"),
            Integer.parseInt(cfg.getProperty("sink.batch.size", "100"))))
            .name("redshift-upsert");

        env.execute("ad-click-minute-aggregator");
    }

    /**
     * Kinesis source table. JSON click events; event time is click_ts with a
     * 30-second bounded-out-of-orderness watermark (tolerates modest lateness;
     * reconciliation corrects anything beyond it — research D8).
     */
    static String sourceDdl(Properties cfg) {
        return "CREATE TABLE click_events ("
             + "  impression_id STRING,"
             + "  ad_id STRING,"
             + "  campaign_id STRING,"
             + "  advertiser_id STRING,"
             + "  click_ts STRING,"
             + "  click_time AS TO_TIMESTAMP(REPLACE(REPLACE(click_ts,'T',' '),'Z','')),"
             + "  WATERMARK FOR click_time AS click_time - INTERVAL '30' SECOND"
             + ") WITH ("
             + "  'connector' = 'kinesis',"
             + "  'stream' = '" + cfg.getProperty("stream.name") + "',"
             + "  'aws.region' = '" + cfg.getProperty("aws.region", "us-east-1") + "',"
             + "  'scan.stream.initpos' = '" + cfg.getProperty("scan.initpos", "LATEST") + "',"
             + "  'format' = 'json',"
             + "  'json.ignore-parse-errors' = 'true'"
             + ")";
    }

    /** Read config from Managed Flink runtime properties, falling back to CLI args. */
    static Properties loadConfig() throws Exception {
        Properties props = new Properties();
        Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
        if (groups != null && groups.containsKey("FlinkAppProperties")) {
            props.putAll(groups.get("FlinkAppProperties"));
        }
        // Allow local/CLI overrides for testing.
        ParameterTool params = ParameterTool.fromSystemProperties();
        props.putAll(params.getProperties());
        return props;
    }
}
