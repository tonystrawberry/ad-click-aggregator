#!/usr/bin/env bash
# Package a Ruby Lambda with the shared gem vendored in, into dist/<service>.zip.
# Usage: scripts/build_lambda.sh <service_dir_under_services>
set -euo pipefail

SERVICE="${1:?usage: build_lambda.sh <service>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/services/$SERVICE"
DIST="$ROOT/dist"
STAGE="$(mktemp -d)"

echo "[build] packaging $SERVICE"
cp -R "$SRC/lib/." "$STAGE/"
mkdir -p "$STAGE/shared"
cp -R "$ROOT/services/shared/lib/." "$STAGE/shared/"

# Vendor gem dependencies for the Lambda ruby runtime.
( cd "$SRC" && bundle config set --local path "$STAGE/vendor/bundle" \
  && bundle config set --local without 'development test' \
  && bundle install >/dev/null )

mkdir -p "$DIST"
( cd "$STAGE" && zip -qr "$DIST/$SERVICE.zip" . )
echo "[build] wrote $DIST/$SERVICE.zip"
rm -rf "$STAGE"
