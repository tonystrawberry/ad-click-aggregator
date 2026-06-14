# frozen_string_literal: true

require "shared"

module ClickProcessor
  # Impression de-duplication (FR-004 / SC-005). Wraps Shared::Aws::RedisDeduper.
  class Deduper
    def initialize(redis_deduper:)
      @redis_deduper = redis_deduper
    end

    # @return [Boolean] true if this is the first counted click for the impression
    def first_click?(impression_id)
      @redis_deduper.first_click?(impression_id)
    end
  end
end
