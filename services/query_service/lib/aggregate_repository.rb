# frozen_string_literal: true

require "time"

module QueryService
  # Reads pre-aggregated counts from Redshift click_aggregates
  # (contracts/redshift-schema.sql). Buckets are rolled up at query time with
  # date_trunc (FR-008); zero-fill of empty buckets is the bucketizer's job.
  class AggregateRepository
    GRANULARITY_UNIT = {"minute" => "minute", "hour" => "hour", "day" => "day"}.freeze

    # @param conn an object responding to #exec_params(sql, params) -> array of
    #   hashes with "bucket_start", "click_count", "source" (pg-compatible).
    def initialize(conn:)
      @conn = conn
    end

    # @return [Array<Hash>] rows {bucket_start: Time, click_count: Integer, source: String}
    def fetch(campaign_id:, from:, to:, granularity:)
      unit = GRANULARITY_UNIT.fetch(granularity) {
        raise ArgumentError, "unsupported granularity: #{granularity}"
      }
      # `unit` is allowlisted above, so safe to interpolate (Redshift date_trunc
      # requires a literal unit, not a bind parameter).
      sql = <<~SQL
        SELECT date_trunc('#{unit}', minute_bucket) AS bucket_start,
               SUM(click_count)                      AS click_count,
               MIN(source)                           AS source
          FROM click_aggregates
         WHERE campaign_id = $1
           AND minute_bucket >= $2
           AND minute_bucket <  $3
         GROUP BY 1
         ORDER BY 1
      SQL

      rows = @conn.exec_params(sql, [campaign_id, from.utc.iso8601, to.utc.iso8601])
      rows.map do |r|
        {
          bucket_start: Time.parse(r["bucket_start"].to_s).utc,
          click_count: Integer(r["click_count"]),
          source: r["source"]
        }
      end
    end
  end
end
