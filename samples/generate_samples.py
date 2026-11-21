"""Generate deterministic local JSON fixtures used by the workbench and demo."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def digest(*parts: str) -> str:
    value = ":".join(parts).encode("utf-8")
    return "sha256:" + hashlib.sha256(value).hexdigest()


def unit(
    env: str,
    version_tag: str,
    name: str,
    expected_version: str,
    dependencies: list[str],
    config: dict[str, Any],
    *,
    image_tag: str | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "image_digest": digest(name, image_tag or version_tag),
        "config": config,
        "dependencies": dependencies,
        "expected_version": expected_version,
    }


def environment(
    name: str,
    version: int,
    *,
    baseline: bool = False,
    tolerance: int = 0,
    release_tag: str = "v1",
) -> dict[str, Any]:
    staging = name == "staging"
    return {
        "name": name,
        "version": version,
        "version_lag_tolerance": tolerance,
        "is_baseline": baseline,
        "units": [
            unit(
                name,
                release_tag,
                "auth",
                "3.1.0",
                ["identity-db"],
                {"JWT_TTL_MINUTES": 30, "LOG_LEVEL": "info"},
            ),
            unit(
                name,
                release_tag,
                "gateway",
                "1.8.0",
                ["auth", "orders"],
                {
                    "LOG_LEVEL": "info",
                    "UPSTREAM_TIMEOUT_MS": 3000 if staging else 2500,
                },
            ),
            unit(
                name,
                release_tag,
                "identity-db",
                "2.0.1",
                [],
                {"LOG_LEVEL": "warn", "POOL_SIZE": 20},
            ),
            unit(
                name,
                release_tag,
                "inventory",
                "2.2.0",
                ["ledger"],
                {
                    "CACHE_TTL_SECONDS": 120 if staging else 60,
                    "LOG_LEVEL": "info",
                },
            ),
            unit(
                name,
                release_tag,
                "ledger",
                "4.0.0",
                [],
                {"LOG_LEVEL": "info", "RETENTION_DAYS": 365},
                image_tag="ledger-dev-preview" if name == "dev" else release_tag,
            ),
            unit(
                name,
                release_tag,
                "orders",
                "2.2.0" if staging and release_tag == "v1" else "2.3.0",
                ["inventory", "payments"],
                {"BATCH_SIZE": 100, "LOG_LEVEL": "info"},
            ),
            unit(
                name,
                release_tag,
                "payments",
                "3.0.0",
                ["ledger"],
                {"LOG_LEVEL": "info", "REFUND_WINDOW_HOURS": 72},
                image_tag="payments-staging-canary" if staging else release_tag,
            ),
        ],
    }


def snapshot_v1() -> dict[str, Any]:
    return {
        "commit_id": "demo-full-v1",
        "snapshot_version": 1,
        "environments": [
            environment("dev", 1, tolerance=1, release_tag="v1"),
            environment("staging", 1, tolerance=0, release_tag="v1"),
            environment("prod", 1, baseline=True, tolerance=0, release_tag="v1"),
        ],
    }


def snapshot_v2() -> dict[str, Any]:
    return {
        "commit_id": "demo-full-v2",
        "snapshot_version": 2,
        "environments": [
            environment("dev", 2, tolerance=1, release_tag="v2"),
            environment("staging", 2, tolerance=0, release_tag="v2"),
            environment("prod", 2, baseline=True, tolerance=0, release_tag="v2"),
        ],
    }


def increment_image() -> dict[str, Any]:
    return {
        "commit_id": "demo-inc-prod-inventory-image",
        "environment": "prod",
        "base_version": 2,
        "units": [
            {
                "name": "inventory",
                "image_digest": digest("prod", "inventory", "hotfix-2026-11-22"),
                "config": {
                    "CACHE_TTL_SECONDS": 60,
                    "LOG_LEVEL": "info",
                },
                "dependencies": ["ledger"],
                "expected_version": "2.2.0",
            }
        ],
    }


def cycle_increment() -> dict[str, Any]:
    return {
        "commit_id": "demo-inc-dev-cycle",
        "environment": "dev",
        "base_version": 3,
        "units": [
            {
                "name": "identity-db",
                "image_digest": digest("dev", "identity-db", "v2"),
                "config": {"LOG_LEVEL": "warn", "POOL_SIZE": 20},
                "dependencies": ["auth"],
                "expected_version": "2.0.1",
            }
        ],
    }


def write(name: str, payload: dict[str, Any]) -> None:
    path = ROOT / name
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"wrote {path}")


def main() -> None:
    write("snapshot_v1.json", snapshot_v1())
    write("snapshot_v2.json", snapshot_v2())
    write("increment_image.json", increment_image())
    write("cycle_increment.json", cycle_increment())


if __name__ == "__main__":
    main()
