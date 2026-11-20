#!/usr/bin/env python3
"""Send the deterministic mid-merge crash request to a local demo server."""

from __future__ import annotations

import argparse
import json
import uuid
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def http_json(base_url: str, method: str, path: str, payload=None):
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(base_url + path, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Force a real server termination mid-merge")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--logical-table", default="orders")
    parser.add_argument("--shard-key-value", type=int, default=7)
    parser.add_argument("--request-id", default=f"manual-crash-{uuid.uuid4().hex[:12]}")
    args = parser.parse_args(argv)

    status, state = http_json(args.base_url, "GET", "/state")
    if status != 200 or not state.get("active_version"):
        raise SystemExit(f"no active snapshot: status={status} body={state}")
    request = {
        "request_id": args.request_id,
        "logical_table": args.logical_table,
        "query_type": "scan",
        "routing_version": state["active_version"],
        "limit": 100,
        "inject": "crash_mid_merge",
    }
    print(json.dumps({"sending": request}, ensure_ascii=False, indent=2))
    try:
        status, body = http_json(args.base_url, "POST", "/query", request)
    except (URLError, ConnectionError, TimeoutError, OSError) as exc:
        print(
            json.dumps(
                {
                    "forced_termination_observed": True,
                    "connection_error": repr(exc),
                    "retry_request_id": args.request_id,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    print(json.dumps({"unexpected_response": {"status": status, "body": body}}, ensure_ascii=False, indent=2))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

