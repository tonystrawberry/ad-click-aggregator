# frozen_string_literal: true

require "spec_helper"
require "json"
require "handler"

RSpec.describe ClickProcessor::Handler do
  let(:ad) do
    Shared::Aws::DynamoDB::Ad.new(
      ad_id: "ad_1", campaign_id: "camp_1", advertiser_id: "adv_1",
      destination_url: "https://advertiser.example/landing", active: true
    )
  end
  let(:ad_repository) { instance_double(ClickProcessor::AdRepository) }
  let(:deduper) { instance_double(ClickProcessor::Deduper) }
  let(:kinesis) { instance_double(Shared::Aws::Kinesis) }
  subject(:handler) { described_class.new(ad_repository: ad_repository, deduper: deduper, kinesis: kinesis) }

  def event(params)
    {"queryStringParameters" => params}
  end

  it "emits one click and 302-redirects on a first-time click" do
    allow(ad_repository).to receive(:active_ad).with("ad_1").and_return(ad)
    allow(deduper).to receive(:first_click?).with("imp_1").and_return(true)
    expect(kinesis).to receive(:put_click).once.and_return("seq-1")

    resp = handler.call(event("ad_id" => "ad_1", "impression_id" => "imp_1"))

    expect(resp[:statusCode]).to eq(302)
    expect(resp[:headers]["Location"]).to eq("https://advertiser.example/landing")
  end

  it "redirects but emits nothing for a duplicate impression (SC-005)" do
    allow(ad_repository).to receive(:active_ad).with("ad_1").and_return(ad)
    allow(deduper).to receive(:first_click?).with("imp_1").and_return(false)
    expect(kinesis).not_to receive(:put_click)

    resp = handler.call(event("ad_id" => "ad_1", "impression_id" => "imp_1"))

    expect(resp[:statusCode]).to eq(302)
  end

  it "returns 404 with no emit for an unknown/inactive ad (FR-005)" do
    allow(ad_repository).to receive(:active_ad).with("nope").and_return(nil)
    expect(deduper).not_to receive(:first_click?)
    expect(kinesis).not_to receive(:put_click)

    resp = handler.call(event("ad_id" => "nope", "impression_id" => "imp_1"))

    expect(resp[:statusCode]).to eq(404)
    expect(JSON.parse(resp[:body])["error"]).to eq("unknown_ad")
  end

  it "returns 400 when required params are missing" do
    resp = handler.call(event("ad_id" => "ad_1"))
    expect(resp[:statusCode]).to eq(400)
    expect(JSON.parse(resp[:body])["error"]).to eq("missing_parameter")
  end

  it "returns 400 when params are blank" do
    resp = handler.call(event("ad_id" => "  ", "impression_id" => ""))
    expect(resp[:statusCode]).to eq(400)
  end

  it "returns 502 (no silent drop) when the Kinesis put fails (Principle IV)" do
    allow(ad_repository).to receive(:active_ad).with("ad_1").and_return(ad)
    allow(deduper).to receive(:first_click?).with("imp_1").and_return(true)
    allow(kinesis).to receive(:put_click).and_raise(StandardError.new("stream throttled"))

    resp = handler.call(event("ad_id" => "ad_1", "impression_id" => "imp_1"))

    expect(resp[:statusCode]).to eq(502)
    expect(JSON.parse(resp[:body])["error"]).to eq("enqueue_failed")
  end

  it "handles a nil queryStringParameters defensively" do
    resp = handler.call({})
    expect(resp[:statusCode]).to eq(400)
  end
end
