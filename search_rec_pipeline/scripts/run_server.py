#!/usr/bin/env python3
"""Start the local HTTP test service.

Run order
---------
1. python scripts/generate_samples.py
2. python scripts/run_server.py --port 8765
3. curl http://127.0.0.1:8765/health
4. POST /run with {"scale":"all"} to execute the same manager used by the CLI.

The service shares config/ and workdir/ with the CLI. It binds to 127.0.0.1 by
default and uses only Python's standard-library HTTP server.

Error-code semantics are the shared values documented in run_cli.py:
E_INVALID_FEATURE, E_HASH_DIM_ZERO, E_CROSS_LIMIT, E_VECTOR_LENGTH_MISMATCH,
E_BUDGET_EXCEEDED, E_INVALID_STAGE, E_STALE_CHECKPOINT, E_NOT_FOUND,
E_INVALID_REQUEST. HTTP uses 400/404/409 as appropriate; JSON always carries
the stable code and contextual details.
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from recsys_pipeline.server import main  # noqa: E402


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--project" not in args:
        args = ["--project", str(ROOT), *args]
    raise SystemExit(main(args))
