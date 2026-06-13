# frozen_string_literal: true

require "aws-sdk-kinesis"
require "securerandom"

module Shared
  module Aws
    # Wrapper over the click-events Kinesis stream.
    #
    # Hot-shard mitigation (research D3): the partition key is "<ad_id>:<salt>"
    # where salt is a random bucket in [0, salt_factor). A viral ad therefore
    # fans out across up to salt_factor shards instead of pinning one. Flink
    # re-aggregates by campaign_id, so salting does not affect correctness.
    class Kinesis
      def initialize(stream_name:, salt_factor: 8, client: ::Aws::Kinesis::Client.new)
        @stream = stream_name
        @salt_factor = [salt_factor.to_i, 1].max
        @client = client
      end

      # @param click_event [Shared::ClickEvent]
      # @return [String] the shard sequence number on success
      # @raise on put failure — caller surfaces this (no silent drop, Principle IV)
      def put_click(click_event)
        resp = @client.put_record(
          stream_name: @stream,
          partition_key: partition_key(click_event.ad_id),
          data: click_event.to_json
        )
        resp.sequence_number
      end

      def partition_key(ad_id)
        "#{ad_id}:#{SecureRandom.random_number(@salt_factor)}"
      end
    end
  end
end
