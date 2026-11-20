from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gateway_middleware.config import Settings  # noqa: E402
from gateway_middleware.sample_data import generate_small  # noqa: E402


def request_json(method: str, url: str, payload: dict | None = None) -> dict:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a fixed local sample to the running API")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--batch-id", default="api-demo-batch")
    args = parser.parse_args()

    settings = Settings.from_env()
    dataset = generate_small(settings)
    payload = dataset.batch(args.batch_id).model_dump()
    submitted = request_json("POST", args.base_url.rstrip("/") + "/v1/batches", payload)
    print(
        json.dumps(
            {
                "commit_version": submitted["commit_version"],
                "decision_distribution": _distribution(submitted["decisions"]),
                "auth_denied": sum(d["decision_code"] == "E_AUTH_DENIED" for d in submitted["decisions"]),
                "token_expired": sum(d["decision_code"] == "E_TOKEN_EXPIRED" for d in submitted["decisions"]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    decision = next(d for d in submitted["decisions"] if d["decision_code"] == "E_RATE_LIMITED")
    query = urllib.parse.urlencode(
        {"route_key": decision["route_key"], "client_id": decision["client_id"]}
    )
    windows = request_json("GET", args.base_url.rstrip("/") + "/v1/windows?" + query)
    sampling = request_json("GET", args.base_url.rstrip("/") + "/v1/sampling/errors?" + query)
    recovery = request_json("GET", args.base_url.rstrip("/") + "/v1/recovery-report")
    print(
        json.dumps(
            {
                "windows_ok": windows["ok"],
                "sampling_ok": sampling["ok"],
                "recovery_report_id": recovery["report"]["report_id"],
                "rate_limit_example": {
                    "request_id": decision["request_id"],
                    "trigger_window": decision["trigger_window"],
                    "decision_code": decision["decision_code"],
                    "error_code": decision["error"]["code"],
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def _distribution(decisions: list[dict]) -> dict[str, int]:
    output: dict[str, int] = {}
    for decision in decisions:
        code = decision["decision_code"]
        output[code] = output.get(code, 0) + 1
    return dict(sorted(output.items()))


if __name__ == "__main__":
    main()
