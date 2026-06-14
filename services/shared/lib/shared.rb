# frozen_string_literal: true

# Shared primitives for the ad click aggregator services.
# See specs/001-ad-click-aggregator/data-model.md and research.md.
module Shared
  autoload :TimeBucket, "shared/time_bucket"
  autoload :ClickEvent, "shared/click_event"

  module Aws
    autoload :DynamoDB, "shared/aws/dynamodb"
    autoload :Kinesis, "shared/aws/kinesis"
    autoload :RedisDeduper, "shared/aws/redis"
  end
end
