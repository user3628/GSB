from __future__ import annotations

import copy
import random
import sys
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.snapshot import attach_checksum, pretty_json  # noqa: E402


SEED = 20261120
DATA_DIR = ROOT / "data"


def dec(value: str | int | float) -> Decimal:
    return Decimal(str(value))


def base_components(rng: random.Random) -> dict[str, dict[str, object]]:
    specifications = {
        "api": {
            "quota": 400,
            "ratio": "0.400000",
            "ratio_tolerance": "0.050000",
            "threshold": "120.000000",
            "usage_base": "42.000000",
            "min_headroom_ratio": "0.100000",
            "limit": 1000,
            "weight": "0.400000",
        },
        "worker": {
            "quota": 350,
            "ratio": "0.350000",
            "ratio_tolerance": "0.050000",
            "threshold": "200.000000",
            "usage_base": "90.000000",
            "min_headroom_ratio": "0.100000",
            "limit": 500,
            "weight": "0.350000",
        },
        "cache": {
            "quota": 250,
            "ratio": "0.250000",
            "ratio_tolerance": "0.050000",
            "threshold": "80.000000",
            "usage_base": "30.000000",
            "min_headroom_ratio": "0.150000",
            "limit": 300,
            "weight": "0.250000",
        },
    }
    components: dict[str, dict[str, object]] = {}
    for component_id in sorted(specifications):
        spec = specifications[component_id]
        jitter = dec(f"{rng.uniform(-1.5, 1.5):.6f}")
        usage = dec(str(spec["usage_base"])) + jitter
        components[component_id] = {
            "quota": spec["quota"],
            "ratio": dec(str(spec["ratio"])),
            "ratio_tolerance": dec(str(spec["ratio_tolerance"])),
            "threshold": dec(str(spec["threshold"])),
            "usage": usage,
            "min_headroom_ratio": dec(str(spec["min_headroom_ratio"])),
            "limit": spec["limit"],
            "weight": dec(str(spec["weight"])),
        }
    return components


def snapshot_payload(version: int, generated_at: str, components: dict[str, dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "snapshot_version": version,
        "generated_at": generated_at,
        "policy": {
            "relative_tolerance": dec("0.000010"),
            "min_weighted_consistency": dec("0.900000"),
            "default_min_headroom_ratio": dec("0.100000"),
        },
        "components": components,
    }


def write_snapshot(path: Path, payload: dict[str, object]) -> None:
    path.write_text(pretty_json(attach_checksum(payload)) + "\n", encoding="utf-8")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    rng = random.Random(SEED)
    baseline_components = base_components(rng)
    baseline = snapshot_payload(1, "2026-11-20T00:00:00Z", baseline_components)

    adjusted_components = copy.deepcopy(baseline_components)
    adjusted_components["api"]["quota"] = 390
    adjusted_components["api"]["usage"] = dec(str(adjusted_components["api"]["usage"])) + dec("0.123456")
    adjusted = snapshot_payload(2, "2026-11-20T00:00:01Z", adjusted_components)

    crash_components = copy.deepcopy(adjusted_components)
    crash_components["cache"]["threshold"] = dec("79.500000")
    crash_components["cache"]["usage"] = dec("31.250000")
    crash_candidate = snapshot_payload(3, "2026-11-20T00:00:02Z", crash_components)

    write_snapshot(DATA_DIR / "baseline.json", baseline)
    write_snapshot(DATA_DIR / "adjusted.json", adjusted)
    write_snapshot(DATA_DIR / "crash_candidate.json", crash_candidate)

    mixed_values = (
        [dec("1000000000000.000000")]
        + [dec("0.100000") for _ in range(20)]
        + [dec("-1000000000000.000000")]
    )
    mixed_payload = {
        "seed": SEED,
        "purpose": "binary64 naive-vs-Kahan comparison; production decision path remains fixed integer micros",
        "values": mixed_values,
    }
    (DATA_DIR / "mixed_sum.json").write_text(pretty_json(mixed_payload) + "\n", encoding="utf-8")

    print(f"generated deterministic snapshots in {DATA_DIR} with seed {SEED}")


if __name__ == "__main__":
    main()
