# frozen_string_literal: true

require "aws-sdk-dynamodb"

module Shared
  module Aws
    # Thin wrapper over the DynamoDB ads catalog (contracts/dynamodb-ads.md).
    class DynamoDB
      Ad = Struct.new(:ad_id, :campaign_id, :advertiser_id, :destination_url, :active)

      def initialize(table_name:, gsi_name: nil, client: ::Aws::DynamoDB::Client.new)
        @table = table_name
        @gsi = gsi_name
        @client = client
      end

      # Hot-path point lookup for click processing.
      # @return [Ad, nil] nil when the ad does not exist
      def get_ad(ad_id)
        resp = @client.get_item(table_name: @table, key: {"ad_id" => ad_id})
        item = resp.item
        return nil unless item

        Ad.new(
          ad_id: item["ad_id"],
          campaign_id: item["campaign_id"],
          advertiser_id: item["advertiser_id"],
          destination_url: item["destination_url"],
          active: item["active"]
        )
      end

      # Ownership check for the query service: does advertiser own this campaign?
      # Uses the advertiser-campaign GSI (KEYS_ONLY).
      def owns_campaign?(advertiser_id:, campaign_id:)
        raise ArgumentError, "gsi_name required for ownership checks" unless @gsi

        resp = @client.query(
          table_name: @table,
          index_name: @gsi,
          key_condition_expression: "advertiser_id = :a AND campaign_id = :c",
          expression_attribute_values: {":a" => advertiser_id, ":c" => campaign_id},
          limit: 1
        )
        !resp.items.empty?
      end
    end
  end
end
