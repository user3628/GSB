from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from embedding_gather.samplegen import generate_all


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic local embedding shards and manifests")
    parser.add_argument("--out", default="samples", help="output directory")
    args = parser.parse_args()
    result = generate_all(args.out)
    for name, metadata in result.items():
        print(f"{name}: seed={metadata['seed']} rows={metadata['total_rows']} dim={metadata['embedding_dim']}")


if __name__ == "__main__":
    main()
