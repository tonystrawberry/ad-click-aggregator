# frozen_string_literal: true

require "shared"

module ClickProcessor
  # Ad lookup + validation for the hot path (FR-005).
  # Thin adapter over Shared::Aws::DynamoDB that encodes the acceptance rule:
  # an ad must exist AND be active to be clickable.
  class AdRepository
    def initialize(dynamodb:)
      @dynamodb = dynamodb
    end

    # @return [Shared::Aws::DynamoDB::Ad, nil] the ad iff it exists and is active
    def active_ad(ad_id)
      ad = @dynamodb.get_ad(ad_id)
      return nil if ad.nil?
      return nil unless ad.active == true

      ad
    end
  end
end
