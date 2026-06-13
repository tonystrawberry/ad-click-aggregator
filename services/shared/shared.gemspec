# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "shared"
  spec.version = "0.1.0"
  spec.authors = ["Tony Duong"]
  spec.summary = "Shared primitives for the ad click aggregator Lambdas"
  spec.description = "Entities, AWS client wrappers, and time-bucket helpers used by " \
                     "the click-processor and query-service Lambdas."
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.files = Dir["lib/**/*.rb"]
  spec.require_paths = ["lib"]

  spec.add_dependency "aws-sdk-dynamodb", "~> 1"
  spec.add_dependency "aws-sdk-kinesis", "~> 1"
  spec.add_dependency "redis", "~> 5"
end
