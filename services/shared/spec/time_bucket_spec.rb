# frozen_string_literal: true

require "spec_helper"

RSpec.describe Shared::TimeBucket do
  describe ".minute_floor" do
    it "floors a Time to the UTC minute" do
      t = Time.utc(2026, 6, 13, 14, 7, 42)
      expect(described_class.minute_floor(t)).to eq("2026-06-13T14:07:00Z")
    end

    it "converts non-UTC input to UTC before flooring" do
      t = Time.new(2026, 6, 13, 9, 7, 42, "-05:00") # 14:07 UTC
      expect(described_class.minute_floor(t)).to eq("2026-06-13T14:07:00Z")
    end

    it "parses ISO8601 strings" do
      expect(described_class.minute_floor("2026-06-13T14:07:42Z"))
        .to eq("2026-06-13T14:07:00Z")
    end

    it "zero-pads single-digit components" do
      t = Time.utc(2026, 1, 2, 3, 4, 5)
      expect(described_class.minute_floor(t)).to eq("2026-01-02T03:04:00Z")
    end
  end

  describe ".valid_bucket?" do
    it "accepts a well-formed bucket" do
      expect(described_class.valid_bucket?("2026-06-13T14:07:00Z")).to be(true)
    end

    it "rejects buckets with non-zero seconds" do
      expect(described_class.valid_bucket?("2026-06-13T14:07:42Z")).to be(false)
    end

    it "rejects garbage" do
      expect(described_class.valid_bucket?("not-a-time")).to be(false)
    end
  end
end
