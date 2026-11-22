from __future__ import annotations

import copy

import pytest

from app.errors import AppError, ErrorCode
from app.policy import compute_checksum
from tests.conftest import GI, T0, sample


def test_flat_to_hierarchical_full_mapping_is_applied(service):
    report = service.migrate_legacy({"legacy": sample("policies_flat_v1.json")})
    assert report["status"] == "APPLIED"
    assert report["code"] == "OK"
    assert report["summary"] == {
        "total": 8,
        "accepted": 8,
        "pending_review": 0,
        "rejected": 0,
    }
    assert report["output_version"] == 1
    assert all(item["mapping_basis"] for item in report["items"])
    snapshot = service.current_snapshot()
    assert snapshot["version"] == 1
    assert snapshot["source_schema"] == "governance.policy/v1-flat"
    replay = service.get_migration(report["migration_id"])
    assert replay["replayable"] is True
    assert replay["report"]["report_checksum"] == report["report_checksum"]
    audit_replay = service.replay_migration(report["migration_id"])
    assert audit_replay["replayed"] is True
    assert audit_replay["input_checksum_valid"] is True
    assert audit_replay["report_checksum_valid"] is True
    assert audit_replay["replay_mode"] == "AUDIT_ONLY_NO_STATE_CHANGE"


def test_conflicting_deprecated_fields_are_pending_and_atomic(loaded_service):
    service = loaded_service
    before_snapshot = service.current_snapshot()
    before_revision = before_snapshot["revision"]
    # At this revision the active snapshot is v2; the migration report is audited
    # at a new revision but policy/runtime state stays at v2.
    report = service.migrate_legacy({"legacy": sample("migration_conflict.json")})
    assert report["status"] == "PENDING_REVIEW"
    assert report["code"] == ErrorCode.E_MIGRATION_REVIEW
    assert report["output_version"] is None
    assert report["summary"]["accepted"] == 1
    assert report["summary"]["pending_review"] == 1
    pending = next(item for item in report["items"] if item["tenant_id"] == "mig-conflict")
    assert pending["status"] == "PENDING_REVIEW"
    assert "DEPRECATED_ALIAS_CONFLICT" in pending["reasons"]
    assert "DEPRECATED_FIELD_UNMAPPED" in pending["reasons"]

    after_snapshot = service.current_snapshot()
    assert after_snapshot["version"] == before_snapshot["version"] == 2
    assert after_snapshot["revision"] > before_revision
    # Pinned read after the rollback revision still uses snapshot v2.
    pinned = service.list_tenants(revision=after_snapshot["revision"])
    assert pinned["snapshot_version"] == 2
    replay = service.get_migration(report["migration_id"])
    assert replay["replayable"] is True


def test_missing_parent_is_rejected_and_rolls_back(loaded_service):
    service = loaded_service
    document = copy.deepcopy(sample("migration_conflict.json"))
    document["tenants"][0]["deprecated_parent_tenant_id"] = "missing-root"
    document["checksum"] = compute_checksum(document)
    before = service.current_snapshot()
    report = service.migrate_legacy({"legacy": document})
    assert report["status"] == "ROLLED_BACK"
    assert report["code"] == ErrorCode.E_MIGRATION_REJECTED
    assert report["summary"]["rejected"] >= 1
    assert report["output_version"] is None
    after = service.current_snapshot()
    assert after["version"] == before["version"]
    assert after["checksum"] == before["checksum"]


def test_lifecycle_overlap_requires_confirmation(loaded_service):
    service = loaded_service
    legacy = sample("policies_flat_v1.json")
    pending = service.migrate_legacy({"legacy": legacy})
    assert pending["status"] == "PENDING_REVIEW"
    assert pending["summary"]["pending_review"] == 8
    assert pending["output_version"] is None

    applied = service.migrate_legacy(
        {
            "legacy": legacy,
            "confirm_lifecycle_overlap": True,
            "base_version": 2,
            "generated_at_ms": T0 + 10_000,
        }
    )
    assert applied["status"] == "APPLIED"
    assert applied["output_version"] == 3
    assert applied["summary"]["accepted"] == 8
    assert any(
        basis["source"] == "lifecycle_overlap_confirmation"
        for item in applied["items"]
        for basis in item["mapping_basis"]
    )


def test_migration_stale_base_has_no_effect(loaded_service):
    service = loaded_service
    before = service.meta()
    with pytest.raises(AppError) as exc:
        service.migrate_legacy(
            {"legacy": sample("policies_flat_v1.json"), "base_version": 1}
        )
    assert exc.value.code == ErrorCode.E_STALE_BASE
    assert service.meta() == before
