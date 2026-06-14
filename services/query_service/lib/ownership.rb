# frozen_string_literal: true

require "shared"

module QueryService
  # Campaign ownership enforcement (FR-009). Wraps the DynamoDB GSI lookup.
  class Ownership
    def initialize(dynamodb:)
      @dynamodb = dynamodb
    end

    def owns?(advertiser_id:, campaign_id:)
      @dynamodb.owns_campaign?(advertiser_id: advertiser_id, campaign_id: campaign_id)
    end
  end
end
