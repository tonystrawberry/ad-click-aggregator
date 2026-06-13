# frozen_string_literal: true

require "spec_helper"

RSpec.describe Shared::Aws::Kinesis do
  describe "#partition_key" do
    it "formats as <ad_id>:<salt> within the salt range" do
      kinesis = described_class.new(stream_name: "s", salt_factor: 8, client: :unused)
      100.times do
        key = kinesis.partition_key("ad_42")
        ad, salt = key.split(":")
        expect(ad).to eq("ad_42")
        expect(Integer(salt)).to be_between(0, 7)
      end
    end

    it "treats a salt_factor below 1 as 1 (single bucket)" do
      kinesis = described_class.new(stream_name: "s", salt_factor: 0, client: :unused)
      expect(kinesis.partition_key("ad_1")).to eq("ad_1:0")
    end
  end

  describe "#put_click" do
    it "puts the serialized event and returns the sequence number" do
      fake_client = instance_double("Aws::Kinesis::Client")
      kinesis = described_class.new(stream_name: "click-events", salt_factor: 4, client: fake_client)
      event = Shared::ClickEvent.build(
        impression_id: "imp_1", ad_id: "ad_1", campaign_id: "c", advertiser_id: "a"
      )

      expect(fake_client).to receive(:put_record).with(
        hash_including(stream_name: "click-events", data: event.to_json)
      ).and_return(double(sequence_number: "seq-123"))

      expect(kinesis.put_click(event)).to eq("seq-123")
    end
  end
end
