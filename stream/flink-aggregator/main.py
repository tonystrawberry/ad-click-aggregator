"""PyFlink stream aggregator (research D4).

Reads click events from Kinesis, maintains 1-minute event-time tumbling windows
keyed by campaign_id, and MERGE-upserts each window's count into Redshift
click_aggregates with source='stream'.

Why PyFlink: the windowing is pure Table API / SQL, which is planned and executed
in the JVM regardless of API language — so there is no Python-process penalty (we
have no Python UDFs). This keeps the codebase Ruby + Python and drops the Java/Maven
toolchain. (Constitution Principle III: no Ruby Flink runtime exists; PyFlink is the
chosen non-Ruby exception, alongside PySpark for reconciliation.)

Sink semantics: each closed 1-minute window emits its final count once, so we use a
single-statement Redshift MERGE that REPLACES the (campaign, minute) count. That is
idempotent under checkpoint replay (re-emitting a window re-sets the same value).
Reconciliation later overwrites the period authoritatively with source='batch'.

Runtime properties (Managed Service for Apache Flink "FlinkAppProperties" group):
  stream.name, aws.region, scan.initpos, redshift.jdbc.url, redshift.user,
  redshift.password, connector.jar.dir
"""

import json
import os

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.table import StreamTableEnvironment

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

# Single-statement Redshift MERGE (Redshift supports MERGE; it has no ON CONFLICT).
# Replace semantics on (campaign_id, minute_bucket) — idempotent under replay.
MERGE_SQL = """
MERGE INTO click_aggregates t
USING (
  SELECT CAST(? AS VARCHAR(64)) AS campaign_id,
         CAST(? AS TIMESTAMP)   AS minute_bucket,
         CAST(? AS BIGINT)      AS click_count
) s
ON t.campaign_id = s.campaign_id AND t.minute_bucket = s.minute_bucket
WHEN MATCHED THEN UPDATE
  SET click_count = s.click_count, source = 'stream', updated_at = GETDATE()
WHEN NOT MATCHED THEN INSERT (campaign_id, minute_bucket, click_count, source, updated_at)
  VALUES (s.campaign_id, s.minute_bucket, s.click_count, 'stream', GETDATE())
"""


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH) as f:
            return json.load(f)
    return []


def property_map(props, group_id):
    for group in props:
        if group.get("PropertyGroupId") == group_id:
            return group.get("PropertyMap", {})
    return {}


def source_ddl(cfg):
    """Kinesis source. Event time = click_ts with a 30s bounded-out-of-orderness
    watermark; reconciliation corrects anything later (FR-016)."""
    return f"""
        CREATE TABLE click_events (
          impression_id STRING,
          ad_id STRING,
          campaign_id STRING,
          advertiser_id STRING,
          click_ts STRING,
          click_time AS TO_TIMESTAMP(REPLACE(REPLACE(click_ts, 'T', ' '), 'Z', '')),
          WATERMARK FOR click_time AS click_time - INTERVAL '30' SECOND
        ) WITH (
          'connector' = 'kinesis',
          'stream' = '{cfg.get("stream.name")}',
          'aws.region' = '{cfg.get("aws.region", "us-east-1")}',
          'scan.stream.initpos' = '{cfg.get("scan.initpos", "LATEST")}',
          'format' = 'json',
          'json.ignore-parse-errors' = 'true'
        )
    """


def add_dependency_jars(env, cfg):
    """Connector + Redshift driver jars bundled under the app's lib/ dir."""
    jar_dir = cfg.get("connector.jar.dir") or os.path.join(os.path.dirname(__file__), "lib")
    if os.path.isdir(jar_dir):
        jars = [f"file://{os.path.join(jar_dir, j)}" for j in os.listdir(jar_dir) if j.endswith(".jar")]
        if jars:
            env.add_jars(*jars)


def main():
    props = get_application_properties()
    cfg = property_map(props, "FlinkAppProperties")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(60_000)  # resume from checkpoint without loss (Principle IV)
    add_dependency_jars(env, cfg)

    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(source_ddl(cfg))

    windowed = t_env.sql_query(
        """
        SELECT campaign_id,
               window_start AS minute_bucket,
               COUNT(*)     AS click_count
        FROM TABLE(TUMBLE(TABLE click_events, DESCRIPTOR(click_time), INTERVAL '1' MINUTE))
        GROUP BY campaign_id, window_start, window_end
        """
    )

    row_type = Types.ROW_NAMED(
        ["campaign_id", "minute_bucket", "click_count"],
        [Types.STRING(), Types.SQL_TIMESTAMP(), Types.LONG()],
    )
    ds = t_env.to_data_stream(windowed)

    jdbc_sink = JdbcSink.sink(
        MERGE_SQL,
        row_type,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(cfg.get("redshift.jdbc.url"))
        .with_driver_name("com.amazon.redshift.jdbc42.Driver")
        .with_user_name(cfg.get("redshift.user"))
        .with_password(cfg.get("redshift.password"))
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_size(int(cfg.get("sink.batch.size", "100")))
        .with_batch_interval_ms(5_000)
        .with_max_retries(3)
        .build(),
    )
    ds.add_sink(jdbc_sink).name("redshift-merge")

    env.execute("ad-click-minute-aggregator")


if __name__ == "__main__":
    main()
