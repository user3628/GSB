from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def request(base_url: str, path: str, *, method: str = "GET", body: bytes | None = None, content_type: str | None = None):
    headers = {}
    if content_type:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(base_url.rstrip("/") + path, data=body, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status, json.loads(response.read())


def main() -> None:
    parser = argparse.ArgumentParser(description="Load deterministic samples into a running local service")
    parser.add_argument("--base-url", default="http://127.0.0.1:8765")
    parser.add_argument("--policy-batch", default="sample-policies-001")
    parser.add_argument("--observe", action="store_true", help="also ingest observations and recalculate")
    args = parser.parse_args()

    status, imported = request(
        args.base_url,
        f"/api/policies/import?format=jsonl&batch_id={args.policy_batch}",
        method="POST",
        body=(ROOT / "samples" / "policies.jsonl").read_bytes(),
        content_type="application/x-ndjson",
    )
    print(json.dumps({"status": status, "import": imported}, ensure_ascii=False, indent=2))

    if args.observe:
        payload = json.loads((ROOT / "samples" / "observations.json").read_text("utf-8"))
        status, ingested = request(
            args.base_url,
            "/api/observations",
            method="POST",
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
        )
        print(json.dumps({"status": status, "observations": ingested}, ensure_ascii=False, indent=2))
        status, recalculated = request(
            args.base_url,
            "/api/recalculate",
            method="POST",
            body=json.dumps(
                {
                    "batchId": "sample-recalc-001",
                    "observationBatchId": payload["batchId"],
                    "windowSize": 2,
                }
            ).encode("utf-8"),
            content_type="application/json",
        )
        print(json.dumps({"status": status, "recalculate": recalculated}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

