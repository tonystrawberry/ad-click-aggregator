"""Spark reconciliation job (research D8).

Re-derives exact per-(campaign, minute) click counts from the raw S3 archive and
overwrites the Redshift aggregates for a closed period as authoritative
(source='batch'), correcting any drift from the real-time Flink path
(FR-012/FR-013/FR-014, SC-004).

The core transform `recompute` is a pure function so it can be unit-tested
without AWS (tests/test_recompute.py).

Glue job arguments:
  --raw_path        s3://<bucket>/raw/      (root of the Parquet archive)
  --period_start    inclusive UTC, e.g. 2026-06-13T14:00:00Z
  --period_end      exclusive UTC
  --redshift_jdbc   jdbc:redshift://host:5439/adclick
  --secret_arn      Secrets Manager ARN holding {username,password,dbname}
  --region          AWS region
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def recompute(raw: DataFrame) -> DataFrame:
    """Exact counts from raw click rows.

    1. De-duplicate by impression_id (at most one click per impression, SC-005) —
       keep the earliest ingest for determinism.
    2. Count per (campaign_id, minute_bucket).

    Input columns: impression_id, campaign_id, minute_bucket, ingest_ts (string).
    Output columns: campaign_id, minute_bucket, click_count (long).
    """
    dedup_order = Window.partitionBy("impression_id").orderBy(F.col("ingest_ts").asc())
    deduped = (
        raw.withColumn("_rn", F.row_number().over(dedup_order))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return (
        deduped.groupBy("campaign_id", "minute_bucket")
        .agg(F.count(F.lit(1)).alias("click_count"))
        .select("campaign_id", "minute_bucket", "click_count")
    )


def _read_period(spark: SparkSession, raw_path: str, start: str, end: str) -> DataFrame:
    """Read raw clicks whose minute_bucket falls in [start, end)."""
    df = spark.read.parquet(raw_path)
    # minute_bucket is an ISO string; lexical comparison is valid for this format.
    return df.filter((F.col("minute_bucket") >= start) & (F.col("minute_bucket") < end))


def _swap_into_redshift(counts: DataFrame, args: dict, creds: dict) -> None:
    """Load exact counts to the stage table, then transactionally overwrite the
    period in click_aggregates with source='batch' (contracts/redshift-schema.sql)."""
    jdbc = args["redshift_jdbc"]
    props = {
        "user": creds["username"],
        "password": creds["password"],
        "driver": "com.amazon.redshift.jdbc42.Driver",
    }

    # Stage the recomputed rows.
    (
        counts.write.format("jdbc")
        .option("url", jdbc)
        .option("dbtable", "click_aggregates_stage")
        .option("user", props["user"])
        .option("password", props["password"])
        .option("driver", props["driver"])
        .mode("overwrite")
        .option("truncate", "true")
        .save()
    )

    # Atomic swap for the period via the Redshift Data API.
    import boto3

    data = boto3.client("redshift-data", region_name=args["region"])
    swap_sql = f"""
        BEGIN;
          DELETE FROM click_aggregates
           WHERE minute_bucket >= '{args["period_start"]}'
             AND minute_bucket <  '{args["period_end"]}';
          INSERT INTO click_aggregates (campaign_id, minute_bucket, click_count, source, updated_at)
          SELECT campaign_id, minute_bucket, click_count, 'batch', GETDATE()
            FROM click_aggregates_stage;
          TRUNCATE click_aggregates_stage;
        COMMIT;
    """
    data.execute_statement(
        workgroup_name=args["workgroup"],
        database=creds["dbname"],
        secret_arn=args["secret_arn"],
        sql=swap_sql,
    )


def main(argv):
    from awsglue.utils import getResolvedOptions  # available in the Glue runtime
    import boto3

    args = getResolvedOptions(
        argv,
        [
            "raw_path", "period_start", "period_end",
            "redshift_jdbc", "secret_arn", "region", "workgroup",
        ],
    )

    spark = SparkSession.builder.appName("ad-click-reconciliation").getOrCreate()
    creds = _load_secret(boto3, args["secret_arn"], args["region"])

    raw = _read_period(spark, args["raw_path"], args["period_start"], args["period_end"])
    counts = recompute(raw)
    _swap_into_redshift(counts, args, creds)
    spark.stop()


def _load_secret(boto3, secret_arn, region):
    import json

    sm = boto3.client("secretsmanager", region_name=region)
    return json.loads(sm.get_secret_value(SecretId=secret_arn)["SecretString"])


if __name__ == "__main__":
    main(sys.argv)
