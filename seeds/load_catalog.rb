#!/usr/bin/env ruby
# frozen_string_literal: true

# Load sample advertisers/campaigns/ads into the DynamoDB ads catalog so the
# quickstart demos have data (contracts/dynamodb-ads.md).
#
# Usage:  ADS_TABLE=ad-click-dev-ads ruby seeds/load_catalog.rb

require "aws-sdk-dynamodb"
require "json"

TABLE = ENV.fetch("ADS_TABLE")
CATALOG = JSON.parse(File.read(File.expand_path("catalog.json", __dir__)))

def main
  client = Aws::DynamoDB::Client.new
  ads = CATALOG.fetch("ads")
  ads.each do |ad|
    client.put_item(
      table_name: TABLE,
      item: {
        "ad_id" => ad.fetch("ad_id"),
        "campaign_id" => ad.fetch("campaign_id"),
        "advertiser_id" => ad.fetch("advertiser_id"),
        "destination_url" => ad.fetch("destination_url"),
        "active" => ad.fetch("active"),
        "created_at" => "2026-06-13T00:00:00Z"
      }
    )
    puts "[seed] put ad #{ad["ad_id"]} (#{ad["active"] ? "active" : "inactive"})"
  end
  puts "[seed] loaded #{ads.size} ads into #{TABLE}"
end

main if $PROGRAM_NAME == __FILE__
