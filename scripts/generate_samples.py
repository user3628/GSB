#!/usr/bin/env python3
"""Generate all deterministic samples and high-precision references."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from spmm_ops.samples import generate_samples  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=str(ROOT / "samples"))
    args = parser.parse_args()
    manifest = generate_samples(args.out)
    print(
        json.dumps(
            {
                "manifest_digest": manifest["manifest_digest"],
                "sample_count": len(manifest["samples"]),
                "samples": [
                    {
                        "name": sample["name"],
                        "shape": sample["shape"],
                        "density": sample["actual_density"],
                    }
                    for sample in manifest["samples"]
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

