package com.adclick;

import java.sql.Timestamp;

/**
 * One aggregated row: clicks for a campaign within a UTC minute window.
 * Mirrors the Redshift click_aggregates key (campaign_id, minute_bucket).
 */
public class MinuteCount {
    public String campaignId;
    public Timestamp minuteBucket;
    public long clickCount;

    public MinuteCount() {}

    public MinuteCount(String campaignId, Timestamp minuteBucket, long clickCount) {
        this.campaignId = campaignId;
        this.minuteBucket = minuteBucket;
        this.clickCount = clickCount;
    }

    @Override
    public String toString() {
        return "MinuteCount{campaignId=" + campaignId
                + ", minuteBucket=" + minuteBucket
                + ", clickCount=" + clickCount + '}';
    }
}
