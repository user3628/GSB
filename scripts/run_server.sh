#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
PORT=${PORT:-18080}
ADDR=${SHUFFLE_ADDR:-"127.0.0.1:$PORT"}
DB=${SHUFFLE_DB:-"$ROOT/data/engine.db"}

cd "$ROOT"
exec go run ./cmd/server -addr "$ADDR" -db "$DB" "$@"
