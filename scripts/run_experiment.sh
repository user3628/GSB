#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

cd "$ROOT"
if [ "$#" -eq 0 ]; then
  exec go run ./cmd/experiment -workdir "$ROOT/artifacts/experiment" -clean
fi
exec go run ./cmd/experiment "$@"
