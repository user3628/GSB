from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from replication_guard.formats import encode_binary, encode_jsonl  # noqa: E402
from replication_guard.models import normalize_policy  # noqa: E402


def main() -> None:
    samples = ROOT / "samples"
    policies = [normalize_policy(raw) for raw in json.loads((samples / "policies.json").read_text("utf-8"))]
    (samples / "policies.jsonl").write_text(encode_jsonl(policies), encoding="utf-8")

    binary, _ = encode_binary(policies, version=2)
    (samples / "policies_v2.bin").write_bytes(binary)

    legacy_raw = {
        "slave_id": "replica-legacy-01",
        "replication_group": "legacy",
        "delay_threshold_ms": 400,
        "tolerable_delay_ms": 1500,
        "expected_version": "5.7.44",
        "degradation_action": "READ_ONLY",
        "priority": 20,
        "forward_compatible": True,
        "policy_version": 9,
        "updated_at_ms": 1762000000123,
    }
    legacy = normalize_policy(legacy_raw)
    legacy_binary, truncations = encode_binary([legacy], version=1)
    (samples / "policies_legacy_v1.bin").write_bytes(legacy_binary)
    (samples / "policies_legacy_export_meta.json").write_text(
        json.dumps(
            {
                "declaredTruncation": "Binary v1 stores whole seconds; milliseconds floor to the second boundary.",
                "truncations": truncations,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print("Generated:")
    for name in ("policies.jsonl", "policies_v2.bin", "policies_legacy_v1.bin", "policies_legacy_export_meta.json"):
        path = samples / name
        print(f"  {path.relative_to(ROOT)} ({path.stat().st_size} bytes)")


if __name__ == "__main__":
    main()

