# frozen_string_literal: true

require "time"

module QueryService
  # Turns sparse aggregate rows into a dense, zero-filled, ordered series over
  # [from, to) at a chosen granularity (FR-010 / SC-007).
  #
  # Resolves analysis finding I1: instead of rejecting very large ranges, it
  # auto-coarsens the granularity (minute -> hour -> day) until the bucket count
  # is within MAX_BUCKETS, matching the spec edge case ("return results at an
  # appropriate granularity without timing out"). The effective granularity is
  # reported back so the caller knows what it got.
  class Bucketizer
    MAX_BUCKETS = 1500
    ORDER = %w[minute hour day].freeze
    STEP = {"minute" => 60, "hour" => 3600, "day" => 86_400}.freeze

    Result = Struct.new(:granularity, :buckets)

    # @param fetcher [#call] called as fetcher.call(granularity:) -> repo rows
    #   (lets the bucketizer re-query if it must coarsen). Alternatively pass
    #   pre-fetched rows via `rows:` together with a fixed `granularity:`.
    def initialize(from:, to:, granularity:)
      @from = floor(from, granularity)
      @to = to
      @requested = granularity
    end

    # @param rows [Array<Hash>] {bucket_start:, click_count:, source:}
    # @return [Result] effective granularity + dense ordered buckets
    def fill(rows, granularity: @requested)
      gran = effective_granularity(granularity)
      counts = index_rows(rows, gran)
      buckets = []
      cursor = floor(@from, gran)
      step = STEP.fetch(gran)
      while cursor < @to
        hit = counts[cursor.utc.to_i]
        buckets << {
          bucket_start: cursor.utc.iso8601,
          click_count: hit ? hit[:count] : 0,
          source: hit ? hit[:source] : "stream"
        }
        cursor += step
      end
      Result.new(granularity: gran, buckets: buckets)
    end

    # The granularity actually used after coarsening to stay under MAX_BUCKETS.
    def effective_granularity(requested = @requested)
      idx = ORDER.index(requested) or raise ArgumentError, "bad granularity"
      while idx < ORDER.size - 1 && bucket_count(ORDER[idx]) > MAX_BUCKETS
        idx += 1
      end
      ORDER[idx]
    end

    def bucket_count(gran)
      ((@to.to_i - floor(@from, gran).to_i) / STEP.fetch(gran).to_f).ceil
    end

    private

    def index_rows(rows, gran)
      acc = {}
      rows.each do |r|
        key = floor(r[:bucket_start], gran).utc.to_i
        entry = (acc[key] ||= {count: 0, all_batch: true})
        entry[:count] += r[:click_count]
        # A rolled-up bucket is exact ('batch') only if every contributing row is.
        entry[:all_batch] &&= (r[:source] == "batch")
      end
      acc.transform_values { |e| {count: e[:count], source: e[:all_batch] ? "batch" : "stream"} }
    end

    def floor(t, gran)
      t = t.utc
      case gran
      when "minute" then Time.utc(t.year, t.month, t.day, t.hour, t.min, 0)
      when "hour" then Time.utc(t.year, t.month, t.day, t.hour, 0, 0)
      when "day" then Time.utc(t.year, t.month, t.day, 0, 0, 0)
      else raise ArgumentError, "bad granularity: #{gran}"
      end
    end
  end
end
