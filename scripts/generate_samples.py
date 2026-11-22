#!/usr/bin/env python3
"""Generate the deterministic local demo corpus."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from audiofp.samples import generate_samples  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples-dir", default=str(ROOT / "samples"))
    parser.add_argument("--seed", type=int, default=20261123)
    args = parser.parse_args()
    result = generate_samples(args.samples_dir, args.seed)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

