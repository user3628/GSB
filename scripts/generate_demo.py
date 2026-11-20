#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from panopipe.demo import generate_demo


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("demo"))
    parser.add_argument("--seed", type=int, default=20261120)
    args = parser.parse_args()
    print(generate_demo(args.out, seed=args.seed))


if __name__ == "__main__":
    main()
