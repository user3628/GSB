from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mpc.samples import DEFAULT_SEED, generate_sample_workspace


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic local shard files")
    parser.add_argument("--output", default="data/seed_20261122")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--loss-scale", type=float, default=64.0)
    args = parser.parse_args()
    manifest = generate_sample_workspace(args.output, seed=args.seed, loss_scale=args.loss_scale)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
