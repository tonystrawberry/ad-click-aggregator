# frozen_string_literal: true

require "spec_helper"
require "time"
require "bucketizer"

RSpec.describe QueryService::Bucketizer do
  def t(str) = Time.parse(str).utc

  describe "#fill (minute granularity, zero-fill FR-010)" do
    it "produces a dense series with zeros for empty minutes" do
      b = described_class.new(
        from: t("2026-06-13T14:00:00Z"), to: t("2026-06-13T14:03:00Z"),
        granularity: "minute"
      )
      rows = [
        {bucket_start: t("2026-06-13T14:00:00Z"), click_count: 5, source: "batch"},
        {bucket_start: t("2026-06-13T14:02:00Z"), click_count: 3, source: "stream"}
      ]
      result = b.fill(rows)

      expect(result.granularity).to eq("minute")
      expect(result.buckets.map { |x| x[:click_count] }).to eq([5, 0, 3])
      expect(result.buckets.map { |x| x[:bucket_start] }).to eq(
        ["2026-06-13T14:00:00Z", "2026-06-13T14:01:00Z", "2026-06-13T14:02:00Z"]
      )
    end

    it "marks an empty bucket as stream and a batch-only bucket as batch" do
      b = described_class.new(
        from: t("2026-06-13T14:00:00Z"), to: t("2026-06-13T14:02:00Z"),
        granularity: "minute"
      )
      rows = [{bucket_start: t("2026-06-13T14:00:00Z"), click_count: 1, source: "batch"}]
      buckets = b.fill(rows).buckets
      expect(buckets[0][:source]).to eq("batch")
      expect(buckets[1][:source]).to eq("stream")
    end
  end

  describe "#fill (hour rollup FR-008)" do
    it "rolls minute rows into hour buckets and sums counts" do
      b = described_class.new(
        from: t("2026-06-13T14:00:00Z"), to: t("2026-06-13T16:00:00Z"),
        granularity: "hour"
      )
      rows = [
        {bucket_start: t("2026-06-13T14:00:00Z"), click_count: 2, source: "batch"},
        {bucket_start: t("2026-06-13T14:30:00Z"), click_count: 4, source: "batch"},
        {bucket_start: t("2026-06-13T15:10:00Z"), click_count: 1, source: "stream"}
      ]
      buckets = b.fill(rows).buckets
      expect(buckets.size).to eq(2)
      expect(buckets[0]).to include(bucket_start: "2026-06-13T14:00:00Z", click_count: 6, source: "batch")
      expect(buckets[1]).to include(bucket_start: "2026-06-13T15:00:00Z", click_count: 1, source: "stream")
    end
  end

  describe "auto-coarsening large windows (analysis finding I1)" do
    it "coarsens minute -> hour -> day to stay under MAX_BUCKETS" do
      # 90 days: minute ≈ 129600 buckets and hour = 2160 buckets both exceed
      # MAX_BUCKETS (1500), so it must coarsen all the way to day (90 buckets).
      b = described_class.new(
        from: t("2026-01-01T00:00:00Z"), to: t("2026-04-01T00:00:00Z"),
        granularity: "minute"
      )
      expect(b.effective_granularity).to eq("day")
    end

    it "keeps minute granularity for small windows" do
      b = described_class.new(
        from: t("2026-06-13T14:00:00Z"), to: t("2026-06-13T15:00:00Z"),
        granularity: "minute"
      )
      expect(b.effective_granularity).to eq("minute")
    end
  end
end
