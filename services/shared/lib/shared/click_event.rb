# frozen_string_literal: true

require "json"
require "securerandom"
require_relative "time_bucket"

module Shared
  # The canonical click event placed on Kinesis and archived to S3.
  # Schema mirrors contracts/click-event.schema.json (data-model.md).
  class ClickEvent
    REQUIRED = %i[event_id impression_id ad_id campaign_id advertiser_id
      click_ts minute_bucket ingest_ts].freeze

    attr_reader :event_id, :impression_id, :ad_id, :campaign_id, :advertiser_id,
      :click_ts, :minute_bucket, :user_agent, :ingest_ts

    # Build an event from an accepted click. click_ts defaults to now; minute_bucket
    # is always derived from click_ts so stream and batch agree (FR-003/FR-016).
    def self.build(impression_id:, ad_id:, campaign_id:, advertiser_id:,
      click_ts: nil, user_agent: nil, now: nil)
      ts = Shared::TimeBucket.coerce(click_ts).utc
      new(
        event_id: SecureRandom.uuid,
        impression_id: impression_id,
        ad_id: ad_id,
        campaign_id: campaign_id,
        advertiser_id: advertiser_id,
        click_ts: ts.iso8601,
        minute_bucket: Shared::TimeBucket.minute_floor(ts),
        user_agent: user_agent,
        ingest_ts: Shared::TimeBucket.coerce(now).utc.iso8601
      )
    end

    def initialize(event_id:, impression_id:, ad_id:, campaign_id:, advertiser_id:,
      click_ts:, minute_bucket:, ingest_ts:, user_agent: nil)
      @event_id = event_id
      @impression_id = impression_id
      @ad_id = ad_id
      @campaign_id = campaign_id
      @advertiser_id = advertiser_id
      @click_ts = click_ts
      @minute_bucket = minute_bucket
      @user_agent = user_agent
      @ingest_ts = ingest_ts
      validate!
    end

    def to_h
      h = {
        event_id: event_id, impression_id: impression_id, ad_id: ad_id,
        campaign_id: campaign_id, advertiser_id: advertiser_id, click_ts: click_ts,
        minute_bucket: minute_bucket, ingest_ts: ingest_ts
      }
      h[:user_agent] = user_agent if user_agent
      h
    end

    def to_json(*) = to_h.to_json

    private

    def validate!
      REQUIRED.each do |field|
        value = public_send(field)
        raise ArgumentError, "#{field} is required" if value.nil? || value.to_s.empty?
      end
      unless Shared::TimeBucket.valid_bucket?(minute_bucket)
        raise ArgumentError, "minute_bucket must match YYYY-MM-DDTHH:MM:00Z, got #{minute_bucket.inspect}"
      end
    end
  end
end
