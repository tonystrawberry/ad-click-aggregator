-- Redshift OLAP aggregate store for the Ad Click Aggregator.
-- Single read surface for advertiser queries. Written by BOTH the Flink stream
-- aggregator (source='stream', approximate, near-real-time) and the Spark
-- reconciliation job (source='batch', authoritative/exact).

CREATE TABLE IF NOT EXISTS click_aggregates (
    campaign_id    VARCHAR(64)  NOT NULL,
    minute_bucket  TIMESTAMP    NOT NULL,   -- UTC minute floor
    click_count    BIGINT       NOT NULL,
    source         VARCHAR(8)   NOT NULL,   -- 'stream' | 'batch'
    updated_at     TIMESTAMP    NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (campaign_id, minute_bucket)
)
DISTSTYLE KEY
DISTKEY (campaign_id)
COMPOUND SORTKEY (campaign_id, minute_bucket);

-- Staging table the Spark reconciliation job loads before the atomic swap.
CREATE TABLE IF NOT EXISTS click_aggregates_stage (
    campaign_id    VARCHAR(64)  NOT NULL,
    minute_bucket  TIMESTAMP    NOT NULL,
    click_count    BIGINT       NOT NULL
);

-- ---------------------------------------------------------------------------
-- PyFlink stream sink: one MERGE per (campaign, minute) window firing, via JdbcSink.
-- REPLACE semantics (set, not add) — a closed window emits its final count once, so
-- this is idempotent under checkpoint replay. Reconciliation later overwrites the
-- period authoritatively with source='batch'.
-- ---------------------------------------------------------------------------
--   MERGE INTO click_aggregates t
--   USING (SELECT :c AS campaign_id, :m AS minute_bucket, :n AS click_count) s
--      ON t.campaign_id = s.campaign_id AND t.minute_bucket = s.minute_bucket
--   WHEN MATCHED THEN UPDATE
--     SET click_count = s.click_count, source = 'stream', updated_at = GETDATE()
--   WHEN NOT MATCHED THEN INSERT (campaign_id, minute_bucket, click_count, source, updated_at)
--     VALUES (s.campaign_id, s.minute_bucket, s.click_count, 'stream', GETDATE());

-- ---------------------------------------------------------------------------
-- Reconciliation swap (Spark): authoritative overwrite for a closed period
-- [:period_start, :period_end). Run inside a single transaction.
-- ---------------------------------------------------------------------------
-- BEGIN;
--   -- (stage table already populated by Spark with exact counts for the period)
--   DELETE FROM click_aggregates
--    WHERE minute_bucket >= :period_start AND minute_bucket < :period_end;
--   INSERT INTO click_aggregates (campaign_id, minute_bucket, click_count, source, updated_at)
--   SELECT campaign_id, minute_bucket, click_count, 'batch', GETDATE()
--     FROM click_aggregates_stage;
--   TRUNCATE click_aggregates_stage;
-- COMMIT;

-- ---------------------------------------------------------------------------
-- Advertiser query (query-service Lambda). :gran in ('minute','hour','day').
-- Zero-fill of empty buckets is done in the Lambda after the scan (FR-010).
-- ---------------------------------------------------------------------------
-- SELECT date_trunc(:gran, minute_bucket) AS bucket_start,
--        SUM(click_count)                 AS click_count,
--        MIN(source)                      AS source   -- 'batch' < 'stream' lexically
--   FROM click_aggregates
--  WHERE campaign_id = :campaign_id
--    AND minute_bucket >= :from
--    AND minute_bucket <  :to
--  GROUP BY 1
--  ORDER BY 1;
