#!/usr/bin/env ruby
# frozen_string_literal: true

# Drive synthetic click traffic at the click API to exercise the pipeline and the
# hot-shard path (SC-001 / SC-006). Each click uses a unique impression_id so it
# counts exactly once; pass --replay to also re-send a fraction (dedup check).
#
# Usage:
#   ruby seeds/click_simulator.rb --url "$CLICK_API_URL" \
#        --rps 500 --duration 60 --hot-ad ad_viral_1 --hot-share 0.5
#
# Ads default to the seeded catalog. The script prints how many clicks it sent
# (accepted = HTTP 302) so you can compare against COUNT(DISTINCT impression_id)
# in the S3 raw archive to verify no-loss (quickstart §4).

require "net/http"
require "uri"
require "optparse"
require "securerandom"

DEFAULT_ADS = %w[ad_demo_1 ad_demo_2 ad_viral_1].freeze

options = {
  rps: 100, duration: 10, hot_ad: nil, hot_share: 0.0, replay: 0.0,
  ads: DEFAULT_ADS
}
OptionParser.new do |o|
  o.on("--url URL") { |v| options[:url] = v }
  o.on("--rps N", Integer) { |v| options[:rps] = v }
  o.on("--duration SECONDS", Integer) { |v| options[:duration] = v }
  o.on("--hot-ad AD_ID") { |v| options[:hot_ad] = v }
  o.on("--hot-share F", Float) { |v| options[:hot_share] = v }
  o.on("--replay F", Float) { |v| options[:replay] = v }
end.parse!

abort "--url is required" unless options[:url]

def pick_ad(options)
  if options[:hot_ad] && rand < options[:hot_share]
    options[:hot_ad]
  else
    options[:ads].sample
  end
end

def send_click(base, ad_id, impression_id)
  uri = URI("#{base}/click")
  uri.query = URI.encode_www_form(ad_id: ad_id, impression_id: impression_id)
  res = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == "https") do |http|
    req = Net::HTTP::Get.new(uri)
    http.request(req)
  end
  res.code.to_i
end

accepted = 0
sent = 0
recent = []
interval = 1.0 / options[:rps]
deadline = Time.now + options[:duration]

while Time.now < deadline
  ad_id = pick_ad(options)
  impression_id =
    if !recent.empty? && rand < options[:replay]
      recent.sample # deliberate duplicate
    else
      id = SecureRandom.uuid
      recent << id
      recent.shift if recent.size > 1000
      id
    end

  code = send_click(options[:url], ad_id, impression_id)
  sent += 1
  accepted += 1 if code == 302
  sleep interval
end

puts "[sim] sent=#{sent} accepted(302)=#{accepted} hot_ad=#{options[:hot_ad]} hot_share=#{options[:hot_share]}"
puts "[sim] verify: COUNT(DISTINCT impression_id) in S3 raw should equal the number"
puts "[sim] of DISTINCT impressions among accepted clicks (no-loss, SC-001/SC-006)."
