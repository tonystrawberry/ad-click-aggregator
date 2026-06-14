# frozen_string_literal: true

require "time"

module Shared
  # UTC minute-bucket helpers. The minute is the minimum aggregation granularity
  # (spec FR-003) and the canonical bucket key used across Kinesis events, Flink,
  # Spark, and Redshift. See data-model.md.
  module TimeBucket
    FORMAT = "%Y-%m-%dT%H:%M:00Z"

    module_function

    # Floor an arbitrary time/ISO8601 string to its UTC minute bucket string.
    # @param ts [Time, String, nil] defaults to now when nil
    # @return [String] e.g. "2026-06-13T14:07:00Z"
    def minute_floor(ts = nil)
      t = coerce(ts).utc
      Time.utc(t.year, t.month, t.day, t.hour, t.min, 0).strftime(FORMAT)
    end

    # @return [Time] parsed UTC time
    def coerce(ts)
      case ts
      when nil then Time.now
      when Time then ts
      when String then Time.parse(ts)
      else raise ArgumentError, "unsupported timestamp: #{ts.class}"
      end
    end

    # @return [Boolean] true if the string is a valid minute-bucket key
    def valid_bucket?(str)
      !!(str =~ /\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z\z/)
    end
  end
end
