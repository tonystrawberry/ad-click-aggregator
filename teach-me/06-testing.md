# Chapter 6: Testing

A four-language pipeline (Ruby, PyFlink, PySpark, Terraform) that talks to six AWS
services sounds untestable. It isn't, because the production code was written with
seams. This chapter shows the seams, the harnesses that exploit them, and — just as
important — what the maintainers chose *not* to test and why.

## The shape of the suite

```
services/shared/spec/            18 examples — pure helpers + AWS wrappers (fakes)
services/click_processor/spec/    7 unit + 1 integration (LocalStack)
services/query_service/spec/     12 unit (bucketizer + handler)
stream/flink-aggregator/tests/    1 PyFlink MiniCluster windowing test
batch/reconciliation/tests/       3 PySpark pure-function tests
infra/terraform/                  fmt + validate (no apply)
```

Everything fast and deterministic runs on every push via
[.github/workflows/ci.yml](.github/workflows/ci.yml); the slow/credentialed parts
(LocalStack integration, a real `terraform apply`) are opt-in.

## Seam 1: don't boot AWS at import time

The Lambdas construct real AWS clients in `build_from_env`, called at the bottom of
the file:

```ruby
HANDLER = build_from_env unless ENV["SKIP_HANDLER_BOOT"]
```

If that ran during a test, every spec would need DynamoDB/Redis/Kinesis credentials
just to `require` the file. The `spec_helper.rb` sets the flag first:

```ruby
# services/click_processor/spec/spec_helper.rb
ENV["SKIP_HANDLER_BOOT"] = "1"
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)

RSpec.configure do |config|
  config.filter_run_excluding(:integration) unless ENV["RUN_INTEGRATION"]
end
```

Two moves in one file: skip the real boot, and **exclude integration specs by
default** unless `RUN_INTEGRATION` is set. So a bare `bundle exec rspec` is fast and
offline.

## Seam 2: constructor injection

`Handler.new(ad_repository:, deduper:, kinesis:)` takes its collaborators as
arguments, so a test passes doubles instead of AWS clients
([services/click_processor/spec/handler_spec.rb](services/click_processor/spec/handler_spec.rb)):

```ruby
let(:ad_repository) { instance_double(ClickProcessor::AdRepository) }
let(:deduper)       { instance_double(ClickProcessor::Deduper) }
let(:kinesis)       { instance_double(Shared::Aws::Kinesis) }
subject(:handler)   { described_class.new(ad_repository:, deduper:, kinesis:) }

it "redirects but emits nothing for a duplicate impression (SC-005)" do
  allow(ad_repository).to receive(:active_ad).with("ad_1").and_return(ad)
  allow(deduper).to receive(:first_click?).with("imp_1").and_return(false)
  expect(kinesis).not_to receive(:put_click)        # the assertion that matters
  resp = handler.call(event("ad_id" => "ad_1", "impression_id" => "imp_1"))
  expect(resp[:statusCode]).to eq(302)
end
```

`expect(kinesis).not_to receive(:put_click)` is the test: a duplicate must redirect
but never emit. No network, no Redis, just a contract on the collaborator. Every
guard branch from [Chapter 2](02-click-capture-path.md) (400/404/502/dup) has a
matching example.

## Seam 3: pure functions need no seam at all

Two of the trickiest pieces — the `Bucketizer` and `recompute` — are pure
transforms, so their tests are just input→output with no doubles. From
[services/query_service/spec/bucketizer_spec.rb](services/query_service/spec/bucketizer_spec.rb):

```ruby
rows = [
  {bucket_start: t("2026-06-13T14:00:00Z"), click_count: 5, source: "batch"},
  {bucket_start: t("2026-06-13T14:02:00Z"), click_count: 3, source: "stream"}
]
result = b.fill(rows)
expect(result.buckets.map { |x| x[:click_count] }).to eq([5, 0, 3])
```

That `[5, 0, 3]` zero-fill is verified with a literal — no mocks because there's
nothing to mock. The lesson is general: the pure-function split from
[Chapter 5](05-reconciliation.md) (`recompute` takes a DataFrame, returns a
DataFrame) is *why* the reconciliation logic is trivially testable while the AWS
swap around it is not.

## Seam 4: hand-rolled fakes where a double is awkward

For the Redis dedup, a `instance_double` would force you to script every `set` call.
Instead there's a tiny in-memory stand-in that implements the *real* `SET NX`
semantics
([services/shared/spec/aws/redis_spec.rb](services/shared/spec/aws/redis_spec.rb)):

```ruby
class FakeRedis
  def initialize = @store = {}
  def set(key, _val, nx: false, ex: nil)
    return false if nx && @store.key?(key)
    @store[key] = true
    true
  end
end

it "returns true the first time ... false thereafter (SC-005)" do
  deduper = described_class.new(client: FakeRedis.new, ttl_seconds: 100)
  expect(deduper.first_click?("imp_1")).to be(true)
  expect(deduper.first_click?("imp_1")).to be(false)
end
```

The fake encodes the one behaviour that matters (first-write-wins) and nothing else.
That's higher-fidelity than a mock for this case, because the *atomicity* is the
thing under test.

## The integration test: LocalStack, opt-in

One spec actually exercises real AWS APIs — against LocalStack, not the cloud
([services/click_processor/spec/integration/click_flow_spec.rb](services/click_processor/spec/integration/click_flow_spec.rb)).
It creates a DynamoDB table and a Kinesis stream in LocalStack, fires the same
impression three times, and reads the stream back:

```ruby
3.times { handler.call(click_event("ad_demo_1", "imp_xyz")) }
records = read_all_records
matching = records.select { |r| JSON.parse(r)["impression_id"] == "imp_xyz" }
expect(matching.size).to eq(1)
```

This catches things unit tests can't: that the partition key is accepted, that the
JSON round-trips, that dedup holds end-to-end across the real client. It's gated by
`RUN_INTEGRATION` and a `docker compose -f docker-compose.test.yml up -d`, so it
never runs in the fast loop.

## Engine tests: real MiniClusters, synthetic sources

The PyFlink and PySpark tests run the *actual* engines locally. `apache-flink`
bundles a Flink MiniCluster; `pyspark` runs a `local[1]` Spark. Both feed
**synthetic in-memory data**, not Kinesis/S3 — so they validate the *logic*
(windowing, dedup) without external systems. The PyFlink test's source is a
`from_collection` DataStream with an assigned watermark, standing in for the real
Kinesis table from [Chapter 3](03-stream-aggregation-pyflink.md).

## What's deliberately not tested

| Not tested | Why | Caught instead by |
|------------|-----|-------------------|
| `terraform apply` | Costs money, needs creds | `fmt` + `validate` in CI; manual T053 |
| The Flink→Redshift `MERGE` sink end-to-end | Needs a live Redshift | The windowing logic is unit-tested; the swap SQL is reviewed in the contract |
| Full 10k clicks/sec (SC-001) | A sizing target, not a contractual SLA in an educational build | `seeds/click_simulator.rb` for a manual smoke |
| The query→Redshift SQL against real Redshift | Needs a cluster | `aggregate_repository` is thin; bucketizer (the logic) is pure-tested |

This is an honest matrix. The risk is concentrated exactly where the repo says it
is — the AWS wiring you only see at deploy time
([Chapter 7](07-deployment-and-infrastructure.md)), which is why `quickstart.md`
exists as a manual end-to-end checklist.

## Try it out

Try each step yourself first — expand the solution only when stuck.

1. Run the whole fast suite from the repo root in one command.

   <details>
   <summary><b>Solution</b></summary>

   ```bash
   make test        # Ruby (shared, click_processor, query_service) + PySpark transform
   ```
   `make test-flink` is separate because it needs the Python 3.11 + JRE setup from
   Chapter 3. Check the `Makefile` targets with `make help`.
   </details>

2. Add a unit test that an unknown ad returns 404 *and* never calls the deduper.

   <details>
   <summary><b>Solution</b></summary>

   It exists ("returns 404 with no emit for an unknown/inactive ad"):
   ```ruby
   allow(ad_repository).to receive(:active_ad).with("nope").and_return(nil)
   expect(deduper).not_to receive(:first_click?)
   ```
   The `not_to receive` proves ordering: validation happens before dedup, so we
   never spend a Redis call on a bogus ad.
   </details>

3. Make a bucketizer test fail on purpose, then read the diff in the failure output.

   <details>
   <summary><b>Solution</b></summary>

   Change the expected zero-fill to `[5, 1, 3]` in `bucketizer_spec.rb` and run
   `bundle exec rspec spec/bucketizer_spec.rb`. RSpec prints `expected: [5, 1, 3] got:
   [5, 0, 3]` — the literal output a pure-function test gives you for free. Revert.
   </details>

4. Verify CI excludes integration specs by removing the env guard mentally — what
   would break locally?

   <details>
   <summary><b>Solution</b></summary>

   Without `filter_run_excluding(:integration)`, a bare `bundle exec rspec` would try
   to reach `http://localhost:4566` (LocalStack) and time out. Confirm the guard:
   `grep -n "integration" services/click_processor/spec/spec_helper.rb`. The CI Ruby
   job runs `--tag ~integration` too — belt and suspenders.
   </details>

Next: [Chapter 7](07-deployment-and-infrastructure.md) is where all of this becomes
real infrastructure — the Terraform modules, the no-NAT networking trick, the build
scripts that package each runtime, and how a click physically reaches a Lambda.
