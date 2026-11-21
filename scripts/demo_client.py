from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import httpx


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def _read(name: str) -> dict[str, Any]:
    return json.loads((SAMPLES / name).read_text(encoding="utf-8"))


def _call(base: str, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
    response = httpx.request(method, base + path, json=payload, timeout=20)
    data = response.json()
    if response.status_code >= 400:
        raise SystemExit(json.dumps(data, ensure_ascii=False, indent=2))
    return data


def main() -> int:
    parser = argparse.ArgumentParser(description="Small CLI client for a running local arbiter")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    sub.add_parser("state")
    sub.add_parser("policy")
    derive = sub.add_parser("derive")
    derive.add_argument("--observation", default="observations_01.json")
    derive.add_argument("--policy-version", type=int, default=1)
    round_cmd = sub.add_parser("round")
    round_cmd.add_argument("--round-id", required=True)
    round_cmd.add_argument("--observation", default="observations_01.json")
    round_cmd.add_argument("--policy-version", type=int, default=1)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    if args.command == "health":
        result = _call(base, "GET", "/api/v1/health")
    elif args.command == "state":
        result = _call(base, "GET", "/api/v1/state")
    elif args.command == "policy":
        result = _call(base, "GET", "/api/v1/policy/current")
    elif args.command == "derive":
        result = _call(
            base,
            "POST",
            "/api/v1/derive",
            {
                "policy_version": args.policy_version,
                "observations": _read(args.observation),
            },
        )
    else:
        result = _call(
            base,
            "POST",
            "/api/v1/rounds",
            {
                "round_id": args.round_id,
                "policy_version": args.policy_version,
                "observations": _read(args.observation),
            },
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
