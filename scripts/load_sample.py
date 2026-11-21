from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description="Load a deterministic JSON task batch")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--queue", required=True)
    parser.add_argument("--file", required=True, type=Path)
    args = parser.parse_args()
    body = json.loads(args.file.read_text(encoding="utf-8"))
    response = httpx.post(
        f"{args.base_url.rstrip('/')}/api/v1/queues/{args.queue}/tasks",
        json=body,
        timeout=10,
        trust_env=False,
    )
    print(json.dumps(response.json(), ensure_ascii=False, indent=2, sort_keys=True))
    if response.status_code >= 400:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

