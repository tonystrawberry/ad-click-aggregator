# frozen_string_literal: true

require "spec_helper"

# Minimal in-memory stand-in for redis-rb's SET ... NX EX semantics.
class FakeRedis
  def initialize = @store = {}

  def set(key, _val, nx: false, ex: nil)
    return false if nx && @store.key?(key)

    @store[key] = true
    true
  end
end

RSpec.describe Shared::Aws::RedisDeduper do
  it "returns true the first time an impression is seen, false thereafter (SC-005)" do
    deduper = described_class.new(client: FakeRedis.new, ttl_seconds: 100)
    expect(deduper.first_click?("imp_1")).to be(true)
    expect(deduper.first_click?("imp_1")).to be(false)
    expect(deduper.first_click?("imp_1")).to be(false)
  end

  it "treats distinct impressions independently" do
    deduper = described_class.new(client: FakeRedis.new, ttl_seconds: 100)
    expect(deduper.first_click?("imp_1")).to be(true)
    expect(deduper.first_click?("imp_2")).to be(true)
  end
end
