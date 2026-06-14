#!/usr/bin/env bash
# Package the PyFlink app (main.py + connector/driver jars) into dist/flink-app.zip
# and upload it to the artifacts bucket. Managed Service for Apache Flink runs the
# zip as a Python application.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
APP="$ROOT/stream/flink-aggregator"
DIST="$ROOT/dist"
LIB="$APP/lib"
ARTIFACTS_BUCKET="${ARTIFACTS_BUCKET:?set ARTIFACTS_BUCKET}"
FLINK_VERSION="1.20"

# Connector + Redshift JDBC jars the job needs at runtime.
KINESIS_JAR="flink-sql-connector-kinesis-5.1.0-1.20.jar"
KINESIS_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kinesis/5.1.0-1.20/${KINESIS_JAR}"
REDSHIFT_JAR="redshift-jdbc42-2.1.0.30.jar"
REDSHIFT_URL="https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/2.1.0.30/${REDSHIFT_JAR}"

echo "[build-flink] fetching connector jars into lib/"
mkdir -p "$LIB"
[ -f "$LIB/$KINESIS_JAR" ]  || curl -fsSL "$KINESIS_URL"  -o "$LIB/$KINESIS_JAR"
[ -f "$LIB/$REDSHIFT_JAR" ] || curl -fsSL "$REDSHIFT_URL" -o "$LIB/$REDSHIFT_JAR"

echo "[build-flink] zipping app"
mkdir -p "$DIST"
rm -f "$DIST/flink-app.zip"
( cd "$APP" && zip -qr "$DIST/flink-app.zip" main.py lib )

echo "[build-flink] uploading to s3://$ARTIFACTS_BUCKET/flink/flink-app.zip"
aws s3 cp "$DIST/flink-app.zip" "s3://$ARTIFACTS_BUCKET/flink/flink-app.zip"
echo "[build-flink] done"
