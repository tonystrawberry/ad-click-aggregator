# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Shared::ClickEvent do
  let(:attrs) do
    {
      impression_id: "imp_001", ad_id: "ad_1", campaign_id: "camp_1",
      advertiser_id: "adv_1", click_ts: "2026-06-13T14:07:42Z",
      now: "2026-06-13T14:07:43Z"
    }
  end

  describe ".build" do
    it "derives minute_bucket from click_ts" do
      event = described_class.build(**attrs)
      expect(event.minute_bucket).to eq("2026-06-13T14:07:00Z")
    end

    it "assigns a unique event_id" do
      a = described_class.build(**attrs)
      b = described_class.build(**attrs)
      expect(a.event_id).not_to eq(b.event_id)
    end

    it "serializes to JSON matching the click-event schema fields" do
      event = described_class.build(**attrs)
      parsed = JSON.parse(event.to_json)
      expect(parsed.keys).to include(
        "event_id", "impression_id", "ad_id", "campaign_id",
        "advertiser_id", "click_ts", "minute_bucket", "ingest_ts"
      )
      expect(parsed["impression_id"]).to eq("imp_001")
    end

    it "omits user_agent when not provided" do
      parsed = JSON.parse(described_class.build(**attrs).to_json)
      expect(parsed).not_to have_key("user_agent")
    end
  end

  describe "validation" do
    it "rejects a blank impression_id" do
      expect { described_class.build(**attrs.merge(impression_id: "")) }
        .to raise_error(ArgumentError, /impression_id is required/)
    end

    it "rejects a malformed minute_bucket on direct construction" do
      expect {
        described_class.new(
          event_id: "e", impression_id: "i", ad_id: "a", campaign_id: "c",
          advertiser_id: "adv", click_ts: "2026-06-13T14:07:42Z",
          minute_bucket: "2026-06-13T14:07:42Z", ingest_ts: "2026-06-13T14:07:43Z"
        )
      }.to raise_error(ArgumentError, /minute_bucket/)
    end
  end
end
