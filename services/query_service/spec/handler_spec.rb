# frozen_string_literal: true

require "spec_helper"
require "json"
require "handler"

RSpec.describe QueryService::Handler do
  let(:ownership) { instance_double(QueryService::Ownership) }
  let(:repository) { instance_double(QueryService::AggregateRepository) }
  subject(:handler) { described_class.new(ownership: ownership, repository: repository) }

  def event(params, token: "adv_1")
    {
      "queryStringParameters" => params,
      "headers" => token ? {"authorization" => "Bearer #{token}"} : {}
    }
  end

  let(:valid_params) do
    {"campaign_id" => "camp_1", "from" => "2026-06-13T14:00:00Z",
     "to" => "2026-06-13T14:02:00Z", "granularity" => "minute"}
  end

  it "returns zero-filled buckets for an owned campaign" do
    allow(ownership).to receive(:owns?).and_return(true)
    allow(repository).to receive(:fetch).and_return(
      [{bucket_start: Time.parse("2026-06-13T14:00:00Z").utc, click_count: 7, source: "batch"}]
    )

    resp = handler.call(event(valid_params))
    body = JSON.parse(resp[:body])

    expect(resp[:statusCode]).to eq(200)
    expect(body["granularity"]).to eq("minute")
    expect(body["buckets"].size).to eq(2)
    expect(body["buckets"][0]["click_count"]).to eq(7)
    expect(body["buckets"][1]["click_count"]).to eq(0)
  end

  it "denies a campaign the caller does not own (FR-009)" do
    allow(ownership).to receive(:owns?).and_return(false)
    expect(repository).not_to receive(:fetch)

    resp = handler.call(event(valid_params))
    expect(resp[:statusCode]).to eq(403)
  end

  it "rejects an unauthenticated request" do
    resp = handler.call(event(valid_params, token: nil))
    expect(resp[:statusCode]).to eq(401)
  end

  it "derives advertiser_id from the bearer token, not the query string" do
    captured = nil
    allow(ownership).to receive(:owns?) { |advertiser_id:, campaign_id:|
      captured = advertiser_id
      true
    }
    allow(repository).to receive(:fetch).and_return([])

    handler.call(event(valid_params.merge("advertiser_id" => "spoofed"), token: "adv_real"))
    expect(captured).to eq("adv_real")
  end

  it "returns 400 on an inverted range (to <= from)" do
    allow(ownership).to receive(:owns?).and_return(true)
    bad = valid_params.merge("from" => "2026-06-13T15:00:00Z", "to" => "2026-06-13T14:00:00Z")
    resp = handler.call(event(bad))
    expect(resp[:statusCode]).to eq(400)
    expect(JSON.parse(resp[:body])["error"]).to eq("bad_range")
  end

  it "returns 400 on an invalid granularity" do
    resp = handler.call(event(valid_params.merge("granularity" => "weekly")))
    expect(resp[:statusCode]).to eq(400)
    expect(JSON.parse(resp[:body])["error"]).to eq("bad_granularity")
  end

  it "auto-coarsens a huge range instead of erroring (I1) and reports the granularity" do
    allow(ownership).to receive(:owns?).and_return(true)
    allow(repository).to receive(:fetch) do |granularity:, **_|
      expect(granularity).to eq("day") # repository queried at coarsened granularity
      []
    end

    huge = valid_params.merge("from" => "2026-01-01T00:00:00Z", "to" => "2026-04-01T00:00:00Z")
    resp = handler.call(event(huge))
    body = JSON.parse(resp[:body])
    expect(resp[:statusCode]).to eq(200)
    expect(body["granularity"]).to eq("day")
  end
end
