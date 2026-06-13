# frozen_string_literal: true

require "redis"

module Shared
  module Aws
    # Impression-ID de-duplication against ElastiCache Redis (research D2, FR-004).
    #
    # first_click? performs an atomic `SET imp:<id> 1 NX EX <ttl>`. The first caller
    # for an impression gets true (and should emit a click event); every subsequent
    # caller gets false (redirect only, no count) — guaranteeing at most one count
    # per impression (SC-005), even under concurrency.
    class RedisDeduper
      KEY_PREFIX = "imp:"

      def initialize(client:, ttl_seconds: 172_800)
        @client = client
        @ttl = ttl_seconds
      end

      # @return [Boolean] true if this is the first time we've seen the impression
      def first_click?(impression_id)
        result = @client.set("#{KEY_PREFIX}#{impression_id}", "1", nx: true, ex: @ttl)
        # redis-rb returns true/false for SET with NX.
        result == true || result == "OK"
      end
    end
  end
end
