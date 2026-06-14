#!/usr/bin/env bash
# Package a Ruby Lambda into dist/<service>.zip with LINUX-native gems, built
# inside a container that matches the Lambda runtime (ruby3.3, arm64 by default).
#
# Layout produced (everything at the zip root, which is /var/task on Lambda):
#   handler.rb, ad_repository.rb, ...   <- the service's lib/ at the root
#   shared/                             <- the shared gem (gemspec + lib), as a path gem
#   Gemfile, Gemfile.lock, .bundle/     <- so `require "bundler/setup"` works at runtime
#   vendor/bundle/...                   <- linux-native gem install (pg, aws-sdk, redis)
#
# Usage: scripts/build_lambda.sh <service>
# Env:   LAMBDA_BUILD_PLATFORM (default linux/arm64), LAMBDA_BUILD_IMAGE (default ruby:3.3)
set -euo pipefail

SERVICE="${1:?usage: build_lambda.sh <service>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/dist"
STAGE="$DIST/stage-$SERVICE"
IMAGE="${LAMBDA_BUILD_IMAGE:-ruby:3.3}"
PLATFORM="${LAMBDA_BUILD_PLATFORM:-linux/arm64}"

echo "[build] packaging $SERVICE for Lambda ($PLATFORM) via Docker"
rm -rf "$STAGE"
mkdir -p "$STAGE/shared"

# 1. Handler code at the zip root.
cp -R "$ROOT/services/$SERVICE/lib/." "$STAGE/"

# 2. The shared gem, vendored *inside* the package as a path gem.
cp "$ROOT/services/shared/shared.gemspec" "$STAGE/shared/"
cp -R "$ROOT/services/shared/lib" "$STAGE/shared/lib"

# 3. A Gemfile that points at the bundled shared gem (rewrite ../shared -> shared).
sed 's#path: "../shared"#path: "shared"#' "$ROOT/services/$SERVICE/Gemfile" > "$STAGE/Gemfile"

# 4. Install Linux-native gems inside a runtime-matching container. libpq-dev is a
#    fallback in case pg has no precompiled gem for the target platform.
docker run --rm --platform "$PLATFORM" \
  -v "$STAGE":/build -w /build "$IMAGE" bash -lc '
    set -euo pipefail
    apt-get update -qq && apt-get install -y -qq libpq-dev >/dev/null
    bundle config set --local path vendor/bundle
    bundle config set --local without "development test"
    bundle lock --add-platform aarch64-linux x86_64-linux >/dev/null 2>&1 || true
    bundle install
  '

# 5. Zip (host has zip; -r includes the hidden .bundle/ dir).
#    Remove any stale archive first — `zip` appends to an existing file.
rm -f "$DIST/$SERVICE.zip"
( cd "$STAGE" && zip -qr "$DIST/$SERVICE.zip" . )
rm -rf "$STAGE"
echo "[build] wrote $DIST/$SERVICE.zip"
