# frozen_string_literal: true

require "bundler/setup" # load the vendored gems packaged alongside this handler
require "json"
require "redis"
require "shared"
require_relative "ad_repository"
require_relative "deduper"

module ClickProcessor
  # Lambda handler for GET /click (contracts/click-api.yaml).
  #
  # Flow (server-side redirect, research D1):
  #   validate params -> look up active ad -> dedup impression ->
  #   on first click: emit ClickEvent to Kinesis -> 302 to destination.
  # Duplicates redirect without emitting. Unknown ad -> 404. Enqueue failure -> 502
  # (never a silent drop, Constitution Principle IV).
  class Handler
    def initialize(ad_repository:, deduper:, kinesis:)
      @ad_repository = ad_repository
      @deduper = deduper
      @kinesis = kinesis
    end

    # @param event [Hash] API Gateway HTTP API (payload v2) event
    # @return [Hash] API Gateway response
    def call(event)
      params = event["queryStringParameters"] || {}
      ad_id = presence(params["ad_id"])
      impression_id = presence(params["impression_id"])
      click_ts = presence(params["click_ts"])

      return error(400, "missing_parameter", "ad_id and impression_id are required") unless ad_id && impression_id

      ad = @ad_repository.active_ad(ad_id)
      return error(404, "unknown_ad", "ad #{ad_id} not found or inactive") if ad.nil?

      if @deduper.first_click?(impression_id)
        emit_click(ad, impression_id, click_ts)
      end

      redirect(ad.destination_url)
    rescue => e
      # Enqueue or downstream failure: surface as 502 so the edge/client retries.
      error(502, "enqueue_failed", e.message)
    end

    private

    def emit_click(ad, impression_id, click_ts)
      event = Shared::ClickEvent.build(
        impression_id: impression_id,
        ad_id: ad.ad_id,
        campaign_id: ad.campaign_id,
        advertiser_id: ad.advertiser_id,
        click_ts: click_ts
      )
      @kinesis.put_click(event)
    end

    def redirect(location)
      {statusCode: 302, headers: {"Location" => location}, body: ""}
    end

    def error(code, error, detail)
      {
        statusCode: code,
        headers: {"Content-Type" => "application/json"},
        body: JSON.generate({error: error, detail: detail})
      }
    end

    def presence(value)
      return nil if value.nil?
      s = value.to_s.strip
      s.empty? ? nil : s
    end
  end

  # Build the handler from environment configuration (cold-start memoized).
  def self.build_from_env
    dynamodb = Shared::Aws::DynamoDB.new(table_name: ENV.fetch("ADS_TABLE"))
    redis = Redis.new(host: ENV.fetch("REDIS_HOST"), port: Integer(ENV.fetch("REDIS_PORT", "6379")))
    deduper = Deduper.new(
      redis_deduper: Shared::Aws::RedisDeduper.new(
        client: redis,
        ttl_seconds: Integer(ENV.fetch("IMPRESSION_TTL_SECONDS", "172800"))
      )
    )
    kinesis = Shared::Aws::Kinesis.new(
      stream_name: ENV.fetch("STREAM_NAME"),
      salt_factor: Integer(ENV.fetch("KINESIS_SALT_FACTOR", "8"))
    )
    Handler.new(
      ad_repository: AdRepository.new(dynamodb: dynamodb),
      deduper: deduper,
      kinesis: kinesis
    )
  end
end

# Lambda entrypoint: `handler.ClickProcessor.lambda_handler`
module ClickProcessor
  HANDLER = build_from_env unless ENV["SKIP_HANDLER_BOOT"]

  def self.lambda_handler(event:, context: nil)
    HANDLER.call(event)
  end
end
