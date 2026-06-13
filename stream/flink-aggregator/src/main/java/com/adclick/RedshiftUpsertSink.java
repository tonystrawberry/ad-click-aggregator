package com.adclick;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Upserts per-(campaign, minute) counts into Redshift with source='stream'
 * (contracts/redshift-schema.sql). Redshift has no MERGE/ON CONFLICT, so each
 * row is applied as UPDATE-then-conditional-INSERT inside one transaction. Counts
 * are ADDED so successive window firings accumulate; reconciliation later
 * overwrites the period authoritatively with source='batch' (FR-013/FR-014).
 *
 * Batches up to batchSize rows per flush to keep Redshift write volume sane.
 */
public class RedshiftUpsertSink extends RichSinkFunction<MinuteCount> {

    private static final String UPDATE_SQL =
        "UPDATE click_aggregates SET click_count = click_count + ?, "
      + "source = 'stream', updated_at = GETDATE() "
      + "WHERE campaign_id = ? AND minute_bucket = ?";

    private static final String INSERT_SQL =
        "INSERT INTO click_aggregates (campaign_id, minute_bucket, click_count, source, updated_at) "
      + "SELECT ?, ?, ?, 'stream', GETDATE() "
      + "WHERE NOT EXISTS (SELECT 1 FROM click_aggregates "
      + "                  WHERE campaign_id = ? AND minute_bucket = ?)";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final int batchSize;

    private transient Connection conn;
    private transient List<MinuteCount> buffer;

    public RedshiftUpsertSink(String jdbcUrl, String user, String password, int batchSize) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.batchSize = Math.max(1, batchSize);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.amazon.redshift.jdbc42.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, password);
        conn.setAutoCommit(false);
        buffer = new ArrayList<>();
    }

    @Override
    public void invoke(MinuteCount value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }
        try (PreparedStatement update = conn.prepareStatement(UPDATE_SQL);
             PreparedStatement insert = conn.prepareStatement(INSERT_SQL)) {
            for (MinuteCount mc : buffer) {
                update.setLong(1, mc.clickCount);
                update.setString(2, mc.campaignId);
                update.setTimestamp(3, mc.minuteBucket);
                update.executeUpdate();

                insert.setString(1, mc.campaignId);
                insert.setTimestamp(2, mc.minuteBucket);
                insert.setLong(3, mc.clickCount);
                insert.setString(4, mc.campaignId);
                insert.setTimestamp(5, mc.minuteBucket);
                insert.executeUpdate();
            }
            conn.commit();
        } catch (Exception e) {
            conn.rollback();
            throw e;
        }
        buffer.clear();
    }

    @Override
    public void close() throws Exception {
        if (conn != null && !conn.isClosed()) {
            try {
                flush();
            } finally {
                conn.close();
            }
        }
    }
}
