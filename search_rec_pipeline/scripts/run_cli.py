#!/usr/bin/env python3
"""Thin launcher for the shared command-line entry point.

Typical order
-------------
1. python scripts/generate_samples.py          # deterministic samples + declarations
2. python scripts/run_cli.py import            # validate config and import manifest
3. python scripts/run_cli.py run --scale all   # normalize -> decide -> encode -> assemble
4. python scripts/run_cli.py status --scale small
5. python scripts/run_cli.py decision --scale small
6. python scripts/run_cli.py budget --scale small
7. python scripts/run_cli.py export --scale small --kind samples --out /tmp/samples.jsonl

Graceful stop and resume
------------------------
  python scripts/run_cli.py run --scale small --stop-after-stage normalize
  python scripts/run_cli.py resume --scale small

Stable error codes (exit status 2; unexpected local errors exit 3)
-----------------------------------------------------------------
E_INVALID_FEATURE        feature region/value is empty or buckets are non-monotonic
E_HASH_DIM_ZERO          hash dimension is zero
E_CROSS_LIMIT            cross cap is exceeded
E_VECTOR_LENGTH_MISMATCH vector length differs from the declared length
E_BUDGET_EXCEEDED        every feasible/selected plan exceeds the budget
E_INVALID_STAGE          stage jump, missing prerequisite, or declaration mismatch
E_STALE_CHECKPOINT       stale/duplicate/non-contiguous/cross-scale checkpoint publish
E_NOT_FOUND              requested stage/artifact is not complete
E_INVALID_REQUEST        malformed request or scale
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recsys_pipeline.cli import main  # noqa: E402


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--project" not in args:
        args = ["--project", str(ROOT), *args]
    raise SystemExit(main(args))
