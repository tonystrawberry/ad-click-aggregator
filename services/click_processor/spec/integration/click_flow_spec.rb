# frozen_string_literal: true

# Integration test against LocalStack (DynamoDB + Kinesis) and a local Redis.
# Run with:  RUN_INTEGRATION=1 bundle exec rspec spec/integration
# Requires `docker compose -f docker-compose.test.yml up -d` from repo root.

require "spec_helper"
require "handler"
require "aws-sdk-dynamodb"
require "aws-sdk-kinesis"
require "redis"

module IntegrationConfig
  ENDPOINT = ENV.fetch("AWS_ENDPOINT_URL", "http://localhost:4566")
  REGION = "us-east-1"
  TABLE = "it-ads"
  STREAM = "it-click-events"
end

RSpec.describe "click flow (integration)", :integration do
  let(:table) { IntegrationConfig::TABLE }
  let(:stream) { IntegrationConfig::STREAM }

  let(:ddb) do
    Aws::DynamoDB::Client.new(endpoint: IntegrationConfig::ENDPOINT, region: IntegrationConfig::REGION,
      access_key_id: "test", secret_access_key: "test")
  end
  let(:kinesis_client) do
    Aws::Kinesis::Client.new(endpoint: IntegrationConfig::ENDPOINT, region: IntegrationConfig::REGION,
      access_key_id: "test", secret_access_key: "test")
  end
  let(:redis) { Redis.new(host: ENV.fetch("REDIS_HOST", "localhost"), port: 6379) }

  before(:all) do
    setup_table
    setup_stream
  end

  before(:each) { redis.flushdb }

  it "writes exactly one Kinesis record per impression across replays" do
    handler = build_handler

    3.times { handler.call(click_event("ad_demo_1", "imp_xyz")) }

    records = read_all_records
    matching = records.select { |r| JSON.parse(r)["impression_id"] == "imp_xyz" }
    expect(matching.size).to eq(1)
  end

  # --- helpers ---------------------------------------------------------------

  def build_handler
    dynamodb = Shared::Aws::DynamoDB.new(table_name: table, client: ddb)
    deduper = ClickProcessor::Deduper.new(
      redis_deduper: Shared::Aws::RedisDeduper.new(client: redis, ttl_seconds: 3600)
    )
    kinesis = Shared::Aws::Kinesis.new(stream_name: stream, salt_factor: 4, client: kinesis_client)
    ClickProcessor::Handler.new(
      ad_repository: ClickProcessor::AdRepository.new(dynamodb: dynamodb),
      deduper: deduper, kinesis: kinesis
    )
  end

  def click_event(ad_id, impression_id)
    {"queryStringParameters" => {"ad_id" => ad_id, "impression_id" => impression_id}}
  end

  def setup_table
    ddb.create_table(
      table_name: table,
      attribute_definitions: [{attribute_name: "ad_id", attribute_type: "S"}],
      key_schema: [{attribute_name: "ad_id", key_type: "HASH"}],
      billing_mode: "PAY_PER_REQUEST"
    )
    ddb.wait_until(:table_exists, table_name: table)
    ddb.put_item(table_name: table, item: {
      "ad_id" => "ad_demo_1", "campaign_id" => "camp_demo",
      "advertiser_id" => "adv_demo", "destination_url" => "https://example.com/x",
      "active" => true
    })
  rescue Aws::DynamoDB::Errors::ResourceInUseException
    # table already exists from a prior run
  end

  def setup_stream
    kinesis_client.create_stream(stream_name: stream, shard_count: 2)
    kinesis_client.wait_until(:stream_exists, stream_name: stream)
  rescue Aws::Kinesis::Errors::ResourceInUseException
    nil
  end

  def read_all_records
    shards = kinesis_client.describe_stream(stream_name: stream).stream_description.shards
    shards.flat_map do |shard|
      iter = kinesis_client.get_shard_iterator(
        stream_name: stream, shard_id: shard.shard_id, shard_iterator_type: "TRIM_HORIZON"
      ).shard_iterator
      kinesis_client.get_records(shard_iterator: iter).records.map { |r| r.data }
    end
  end
end
