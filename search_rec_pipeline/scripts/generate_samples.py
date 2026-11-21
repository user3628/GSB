#!/usr/bin/env python3
"""Generate the three deterministic local JSONL scales and declarations.

Run order
---------
1. python scripts/generate_samples.py [--force]
2. python scripts/run_cli.py --project . import
3. python scripts/run_cli.py --project . run --scale all
4. Query status/decision/budget, or run scripts/demo.py for all three paths.

Business error codes returned by the shared pipeline (also listed in README)
--------------------------------------------------------------------------
E_INVALID_FEATURE        empty region/value or non-monotonic bucket boundaries
E_HASH_DIM_ZERO          category/cross hash dimension is zero
E_CROSS_LIMIT            requested crosses exceed declared/global limit
E_VECTOR_LENGTH_MISMATCH fixed vector length differs from declaration
E_BUDGET_EXCEEDED        selected plan exceeds the declared compute budget
E_INVALID_STAGE          illegal transition/missing prerequisite/count mismatch
E_STALE_CHECKPOINT       duplicate, behind, non-contiguous, or cross-scale publish
E_NOT_FOUND              requested completed artifact does not exist
E_INVALID_REQUEST        malformed CLI/HTTP request
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recsys_pipeline.config import Config  # noqa: E402
from recsys_pipeline.generator import generate_all  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=str(ROOT))
    parser.add_argument("--config", default=str(ROOT / "config" / "pipeline.yaml"))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    project = Path(args.project).resolve()
    config = Config.load(project, Path(args.config))
    result = generate_all(project, config, force=args.force)
    print(json.dumps({"ok": True, "data": result}, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
