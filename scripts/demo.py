"""End-to-end deterministic demonstration.

Run: python scripts/demo.py
Optional: python scripts/demo.py --output artifacts/demo_report.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.service import GovernanceService  # noqa: E402

T0 = 1_767_225_600_000


def load(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "samples" / name).read_text(encoding="utf-8"))


def summarize(execution) -> dict[str, Any]:
    decision = execution.decision
    details = decision.get("details") or {}
    resources = details.get("resources") or {}
    cpu = resources.get("cpu_millicores") or {}
    return {
        "request_id": decision["request_id"],
        "decision": decision["decision"],
        "code": decision["code"],
        "state_after": decision["state_after"],
        "blocking_tenant": details.get("blocking_tenant_id") or details.get("tenant_id"),
        "cpu_shortfall": cpu.get("shortfall"),
        "token_shortfall_units": (details.get("tokens") or {}).get("shortfall_units"),
    }


def run_batch(service: GovernanceService, batch: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    result = service.submit_batch(batch)
    summaries = [summarize(type("E", (), {"decision": item["decision"]})()) for item in result["items"]]
    full = service.list_decisions(page=1, page_size=1000)
    pinned = full["revision"]
    chunks: list[dict[str, Any]] = []
    page = 1
    while True:
        part = service.list_decisions(page=page, page_size=3, revision=pinned)
        chunks.extend(part["items"])
        if page >= part["pages"]:
            break
        page += 1
    pagination = {
        "revision": pinned,
        "total": full["total"],
        "full_matches_concatenated_pages": chunks == full["items"],
        "unique_request_ids": len({item["request_id"] for item in chunks}),
    }
    return summaries, pagination


def differences(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {item["request_id"]: item for item in after}
    changes: list[dict[str, Any]] = []
    for old in before:
        new = by_id[old["request_id"]]
        fields = [
            key
            for key in ("decision", "code", "state_after", "blocking_tenant", "cpu_shortfall", "token_shortfall_units")
            if old.get(key) != new.get(key)
        ]
        if fields:
            changes.append(
                {
                    "request_id": old["request_id"],
                    "changed_fields": fields,
                    "before": {key: old.get(key) for key in fields},
                    "after": {key: new.get(key) for key in fields},
                }
            )
    return changes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    batch = load("requests_batch.json")
    flat = load("policies_flat_v1.json")
    hierarchical = load("policies_hierarchical_v2.json")

    report: dict[str, Any] = {
        "samples": {
            "legacy_schema": flat["schema_version"],
            "legacy_checksum": flat["checksum"],
            "hierarchical_schema": hierarchical["schema_version"],
            "hierarchical_checksum": hierarchical["checksum"],
            "batch_id": batch["batch_id"],
            "request_count": len(batch["applications"]),
        }
    }

    with tempfile.TemporaryDirectory(prefix="governance-demo-") as temp_dir:
        # 1) Legacy flat policy decisions.
        legacy_db = str(Path(temp_dir) / "legacy.sqlite")
        legacy_svc = GovernanceService(legacy_db)
        legacy_svc.startup()
        legacy_load = legacy_svc.load_snapshot(flat)
        legacy_decisions, legacy_pagination = run_batch(legacy_svc, batch)
        legacy_snapshot = legacy_svc.current_snapshot()

        # 2) New hierarchical policy decisions for the identical batch.
        new_db = str(Path(temp_dir) / "new.sqlite")
        new_svc = GovernanceService(new_db)
        new_svc.startup()
        new_load = new_svc.load_snapshot(hierarchical)
        new_decisions, new_pagination = run_batch(new_svc, batch)

        # 3) v1-flat -> v2-hierarchical migration, then the same batch again.
        migration_db = str(Path(temp_dir) / "migrated.sqlite")
        migration_svc = GovernanceService(migration_db)
        migration_svc.startup()
        migration = migration_svc.migrate_legacy({"legacy": flat})
        migrated_decisions, migrated_pagination = run_batch(migration_svc, batch)
        migrated_snapshot = migration_svc.current_snapshot()

        migration_diff = differences(legacy_decisions, migrated_decisions)
        policy_diff = differences(legacy_decisions, new_decisions)

        # 4) Controlled hard-kill after intent persistence, followed by recovery.
        crash_db = str(Path(temp_dir) / "crash.sqlite")
        crash_svc = GovernanceService(crash_db)
        crash_svc.startup()
        crash_svc.load_snapshot(hierarchical)
        crash_svc.submit_decision(
            {
                "request_id": "crash-demo-baseline",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 500,
                "cpu_millicores": 100,
                "memory_bytes": 1024,
            }
        )
        baseline_revision = crash_svc.meta()["revision"]
        baseline_tenants = {t["tenant_id"]: t for t in crash_svc.list_tenants()["tenants"]}

        process = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "crash_probe.py"),
                "--db",
                crash_db,
                "--request-id",
                "crash-demo-intent",
                "--arrival-ms",
                str(T0 + 1000),
                "--cpu",
                "100",
                "--memory",
                "1024",
            ],
            cwd=str(ROOT),
            text=True,
            capture_output=True,
            timeout=30,
        )
        assert process.returncode == 91, process.stderr
        recovered_svc = GovernanceService(crash_db)
        recovery = recovered_svc.startup()
        recovered_tenants = {t["tenant_id"]: t for t in recovered_svc.list_tenants()["tenants"]}
        recovery_verified = {
            "probe_exit_code": process.returncode,
            "pending_found": recovery["pending_found"],
            "aborted_count": recovery["aborted_count"],
            "last_complete_revision": recovery["last_complete_revision"],
            "expected_revision": baseline_revision,
            "revision_preserved": recovery["last_complete_revision"] == baseline_revision,
            "starter_usage_preserved": (
                recovered_tenants["starter"]["usage"] == baseline_tenants["starter"]["usage"]
            ),
            "starter_tokens_preserved": (
                recovered_tenants["starter"]["token_bucket"]
                == baseline_tenants["starter"]["token_bucket"]
            ),
            "crash_request_present": any(
                item["request_id"] == "crash-demo-intent"
                for item in recovered_svc.list_decisions(page_size=1000)["items"]
            ),
            "conclusion": recovery["conclusion"],
            "report_checksum": recovery["report_checksum"],
        }

        report.update(
            {
                "decisions": {
                    "legacy_flat": legacy_decisions,
                    "hierarchical_v2": new_decisions,
                    "after_migration": migrated_decisions,
                },
                "migration": {
                    "status": migration["status"],
                    "code": migration["code"],
                    "summary": migration["summary"],
                    "output_version": migration["output_version"],
                    "report_checksum": migration["report_checksum"],
                    "before_after_decision_differences": migration_diff,
                    "mapping_preserves_decisions": not migration_diff,
                },
                "legacy_vs_hierarchical_policy_differences": policy_diff,
                "snapshots": {
                    "legacy": {
                        "version": legacy_load["version"],
                        "source_schema": legacy_snapshot["source_schema"],
                    },
                    "hierarchical": {
                        "version": new_load["version"],
                        "source_schema": "governance.policy/v2",
                    },
                    "after_migration": {
                        "version": migrated_snapshot["version"],
                        "source_schema": migrated_snapshot["source_schema"],
                    },
                },
                "pagination": {
                    "legacy": legacy_pagination,
                    "hierarchical": new_pagination,
                    "migrated": migrated_pagination,
                },
                "recovery": recovery_verified,
            }
        )

    report["result"] = "PASS"
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    print("DEMO RESULT:", report["result"])
    print(
        "legacy decisions:",
        {item["code"]: sum(1 for x in legacy_decisions if x["code"] == item["code"]) for item in legacy_decisions},
    )
    print(
        "hierarchical decisions:",
        {item["code"]: sum(1 for x in new_decisions if x["code"] == item["code"]) for item in new_decisions},
    )
    print(
        "migration:",
        migration["status"],
        migration["code"],
        migration["summary"],
        "decision_differences=",
        len(migration_diff),
    )
    print(
        "legacy-vs-v2 decision differences:",
        [(item["request_id"], item["changed_fields"]) for item in policy_diff],
    )
    print("pagination exact:", all(p["full_matches_concatenated_pages"] for p in report["pagination"].values()))
    print("recovery:", recovery_verified)
    if args.output:
        print("report:", args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
