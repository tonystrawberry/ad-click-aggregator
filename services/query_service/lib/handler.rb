# frozen_string_literal: true

require "json"
require "time"
require "shared"
require_relative "ownership"
require_relative "aggregate_repository"
require_relative "bucketizer"

module QueryService
  # Lambda handler for GET /metrics (contracts/query-api.yaml).
  #
  # Derives advertiser_id from the bearer principal (never from the query string),
  # enforces campaign ownership (FR-009), reads pre-aggregated counts from Redshift,
  # and returns a dense zero-filled series (FR-010). Very large windows are served
  # at a coarser granularity rather than rejected (analysis finding I1).
  class Handler
    VALID_GRANULARITY = %w[minute hour day].freeze

    def initialize(ownership:, repository:)
      @ownership = ownership
      @repository = repository
    end

    def call(event)
      params = event["queryStringParameters"] || {}
      advertiser_id = principal_advertiser_id(event)
      return error(401, "unauthenticated", "missing bearer token") unless advertiser_id

      campaign_id = presence(params["campaign_id"])
      granularity = presence(params["granularity"]) || "minute"
      return error(400, "missing_parameter", "campaign_id is required") unless campaign_id
      unless VALID_GRANULARITY.include?(granularity)
        return error(400, "bad_granularity", "granularity must be one of #{VALID_GRANULARITY.join(", ")}")
      end

      from, to = parse_range(params)
      return error(400, "bad_range", "from/to must be valid ISO8601 with to > from") unless from && to && to > from

      unless @ownership.owns?(advertiser_id: advertiser_id, campaign_id: campaign_id)
        return error(403, "forbidden", "campaign not owned by caller")
      end

      bucketizer = Bucketizer.new(from: from, to: to, granularity: granularity)
      effective = bucketizer.effective_granularity
      rows = @repository.fetch(campaign_id: campaign_id, from: from, to: to, granularity: effective)
      result = bucketizer.fill(rows, granularity: effective)

      ok(campaign_id: campaign_id, from: from, to: to, result: result)
    rescue => e
      error(500, "query_failed", e.message)
    end

    private

    # Demo principal model (see research.md D9 / quickstart): the bearer token
    # value IS the advertiser_id. A real deployment would validate a JWT and read
    # the advertiser_id claim; building an IdP is out of scope (spec Assumptions).
    def principal_advertiser_id(event)
      headers = event["headers"] || {}
      auth = headers["authorization"] || headers["Authorization"]
      return nil unless auth&.start_with?("Bearer ")

      presence(auth.sub("Bearer ", ""))
    end

    def parse_range(params)
      [parse_time(params["from"]), parse_time(params["to"])]
    end

    def parse_time(str)
      return nil unless str
      Time.parse(str).utc
    rescue ArgumentError
      nil
    end

    def ok(campaign_id:, from:, to:, result:)
      body = {
        campaign_id: campaign_id,
        granularity: result.granularity,
        from: from.iso8601,
        to: to.iso8601,
        buckets: result.buckets
      }
      {statusCode: 200, headers: {"Content-Type" => "application/json"}, body: JSON.generate(body)}
    end

    def error(code, error, detail)
      {statusCode: code, headers: {"Content-Type" => "application/json"},
       body: JSON.generate({error: error, detail: detail})}
    end

    def presence(value)
      return nil if value.nil?
      s = value.to_s.strip
      s.empty? ? nil : s
    end
  end

  def self.build_from_env
    require "pg"
    dynamodb = Shared::Aws::DynamoDB.new(
      table_name: ENV.fetch("ADS_TABLE"),
      gsi_name: ENV.fetch("ADS_GSI", "advertiser-campaign-index")
    )
    conn = PG.connect(
      host: ENV.fetch("REDSHIFT_HOST"),
      port: Integer(ENV.fetch("REDSHIFT_PORT", "5439")),
      dbname: ENV.fetch("REDSHIFT_DB", "adclick"),
      user: ENV.fetch("REDSHIFT_USER"),
      password: ENV.fetch("REDSHIFT_PASSWORD")
    )
    Handler.new(
      ownership: Ownership.new(dynamodb: dynamodb),
      repository: AggregateRepository.new(conn: conn)
    )
  end

  HANDLER = build_from_env unless ENV["SKIP_HANDLER_BOOT"]

  def self.lambda_handler(event:, context: nil)
    HANDLER.call(event)
  end
end
