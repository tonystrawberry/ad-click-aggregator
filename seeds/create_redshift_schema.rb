#!/usr/bin/env ruby
# frozen_string_literal: true

# Idempotently create the Redshift aggregate tables by running the DDL from
# contracts/redshift-schema.sql via the Redshift Data API.
#
# Usage:
#   WORKGROUP=ad-click-dev-wg SECRET_ARN=arn:... DATABASE=adclick \
#     ruby seeds/create_redshift_schema.rb
#
# The DDL uses CREATE TABLE IF NOT EXISTS, so re-running is safe.

require "aws-sdk-redshiftdataapiservice"

WORKGROUP = ENV.fetch("WORKGROUP")
SECRET_ARN = ENV.fetch("SECRET_ARN")
DATABASE = ENV.fetch("DATABASE", "adclick")
DDL_PATH = File.expand_path("../specs/001-ad-click-aggregator/contracts/redshift-schema.sql", __dir__)

# Pull only the executable CREATE TABLE statements (skip the commented query examples).
def create_statements(sql)
  sql.scan(/CREATE TABLE IF NOT EXISTS.*?;/m)
end

def main
  client = Aws::RedshiftDataAPIService::Client.new
  sql = File.read(DDL_PATH)
  statements = create_statements(sql)
  abort "No CREATE TABLE statements found in #{DDL_PATH}" if statements.empty?

  statements.each_with_index do |stmt, i|
    puts "[schema] executing statement #{i + 1}/#{statements.size}"
    resp = client.execute_statement(
      workgroup_name: WORKGROUP,
      secret_arn: SECRET_ARN,
      database: DATABASE,
      sql: stmt
    )
    wait_for(client, resp.id)
  end
  puts "[schema] done: click_aggregates + click_aggregates_stage ready"
end

def wait_for(client, statement_id)
  loop do
    desc = client.describe_statement(id: statement_id)
    case desc.status
    when "FINISHED" then return
    when "FAILED", "ABORTED" then abort "[schema] statement failed: #{desc.error}"
    else sleep 1
    end
  end
end

main if $PROGRAM_NAME == __FILE__
