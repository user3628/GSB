#!/usr/bin/env python3
"""Generate the fixed-seed WAV fragments and manifest used by the demo."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sonic_sep.generate import generate_dataset


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples-dir", default="samples")
    parser.add_argument("--seed", type=int, default=20261123)
    args = parser.parse_args()
    path = generate_dataset(Path(args.samples_dir), seed=args.seed)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    print(
        json.dumps(
            {
                "ok": True,
                "manifest": str(path),
                "seed": args.seed,
                "slice_count": manifest["slice_count"],
                "total_samples": manifest["total_samples"],
            },
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

