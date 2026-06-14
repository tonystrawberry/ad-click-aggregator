# frozen_string_literal: true

# Prevent the handler file from booting real AWS clients at require time.
ENV["SKIP_HANDLER_BOOT"] = "1"

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :expect }
  config.disable_monkey_patching!
  config.order = :random
  # Integration tests (LocalStack/Redis) are opt-in via --tag integration.
  config.filter_run_excluding(:integration) unless ENV["RUN_INTEGRATION"]
end
