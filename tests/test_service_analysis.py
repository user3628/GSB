from __future__ import annotations

import json
from pathlib import Path

import pytest

from slowquery.config import WorkbenchConfig
from slowquery.errors import BusinessError
from slowquery.service import SlowQueryService
from slowquery.verification import VerificationExecutionError


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def _diagnose(service: SlowQueryService, session_id: str, sample_id: str):
    service.create_session(session_id=session_id, sample_id=sample_id)
    return service.diagnose(session_id)


def test_all_slow_causes_and_rewrite_kinds(seeded_service: SlowQueryService) -> None:
    expected = {
        "sq-missing-index": ("MISSING_INDEX", {"ADD_INDEX"}),
        "sq-selectivity": ("SELECTIVITY_ESTIMATE_BIAS", {"REORDER_PREDICATES"}),
        "sq-lock-wait": ("LOCK_WAIT", {"ADD_LIMIT"}),
        "sq-or-split": ("OR_PREDICATE", {"SPLIT_OR", "ADD_LIMIT"}),
        "sq-many-rows": ("EXCESSIVE_RETURNED_ROWS", {"ADD_LIMIT"}),
        "sq-full-scan": ("EXCESSIVE_RETURNED_ROWS", {"ADD_LIMIT"}),
    }
    for sample_id, (reason_code, kinds) in expected.items():
        result = _diagnose(seeded_service, f"diag-{sample_id}", sample_id)
        session = result["session"]
        assert session["status"] == "SUGGESTED"
        assert session["is_slow"] is True
        assert reason_code in {reason["code"] for reason in session["analysis"]["slow_reasons"]}
        assert {suggestion["kind"] for suggestion in session["suggestions"]} == kinds
        for suggestion in session["suggestions"]:
            assert suggestion["rule_id"]
            assert suggestion["expected_benefit"]
            assert suggestion["snapshot_version"] == 1
            assert suggestion["session_id"] == session["session_id"]
            for reason in session["analysis"]["slow_reasons"]:
                assert reason["suggestion_ids"]


def test_fast_query_is_not_slow(seeded_service: SlowQueryService) -> None:
    result = _diagnose(seeded_service, "fast-session", "sq-fast")
    session = result["session"]
    assert session["status"] == "SUGGESTED"
    assert session["is_slow"] is False
    assert session["analysis"]["slow_reasons"] == []
    assert session["suggestions"] == []


def test_verification_success_compares_cost_and_closes_resources(
    seeded_service: SlowQueryService,
) -> None:
    _diagnose(seeded_service, "verify-ok", "sq-missing-index")
    result = seeded_service.verify("verify-ok")
    record = result["verification"]
    assert result["session"]["status"] == "VERIFIED"
    assert record["outcome"] == "VERIFIED"
    assert record["original_result_digest"] == record["rewritten_result_digest"]
    assert record["original_row_count"] == record["rewritten_row_count"] == 13
    assert record["original_cost"]["score"] == 250
    assert record["rewritten_cost"]["score"] < record["original_cost"]["score"]
    assert record["detail"]["cost_improved"] is True
    resources = record["detail"]["resources"]
    assert all(resources.values())
    assert set(resources) == {
        "source_connection_closed",
        "copy_destination_connection_closed",
        "verification_connection_closed",
        "temp_database_removed",
    }
    suggestions = seeded_service.get_suggestions("verify-ok")["suggestions"]
    assert suggestions[0]["verification_status"] == "VERIFIED"


def test_verification_mismatch_moves_session_to_failed(
    seeded_service: SlowQueryService,
) -> None:
    _diagnose(seeded_service, "verify-bad", "sq-full-scan")
    with pytest.raises(BusinessError) as caught:
        seeded_service.verify("verify-bad")
    assert caught.value.code == "E_VERIFY_MISMATCH"
    assert caught.value.http_status == 422
    assert caught.value.details["original_row_count"] == 1000
    assert caught.value.details["rewritten_row_count"] == 500
    session = seeded_service.get_session("verify-bad")
    assert session["status"] == "FAILED"
    assert len(session["verifications"]) == 1
    assert session["verifications"][0]["outcome"] == "FAILED"
    assert session["suggestions"][0]["verification_status"] == "FAILED"
    assert session["last_error_code"] == "E_VERIFY_MISMATCH"


def test_verification_before_suggestion_is_rejected(seeded_service: SlowQueryService) -> None:
    seeded_service.create_session(session_id="too-early", sample_id="sq-missing-index")
    with pytest.raises(BusinessError) as caught:
        seeded_service.verify("too-early")
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert seeded_service.get_session("too-early")["status"] == "CREATED"


def test_no_snapshot_means_no_suggestions(service: SlowQueryService) -> None:
    service.submit_samples(json.loads((DATA_DIR / "samples.json").read_text()))
    service.create_session(session_id="no-snapshot", sample_id="sq-missing-index")
    with pytest.raises(BusinessError) as caught:
        service.diagnose("no-snapshot")
    assert caught.value.code == "E_SNAPSHOT_INVALID"
    session = service.get_session("no-snapshot")
    assert session["status"] == "CREATED"
    assert session["suggestions"] == []


def test_retired_snapshot_cannot_interpret_new_session(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.load_snapshot(json.loads((DATA_DIR / "snapshot_v2.json").read_text()))
    seeded_service.create_session(session_id="retired", sample_id="sq-many-rows")
    with pytest.raises(BusinessError) as caught:
        seeded_service.diagnose("retired", requested_snapshot_version=1)
    assert caught.value.code == "E_SNAPSHOT_RETIRED"
    session = seeded_service.get_session("retired")
    assert session["status"] == "CREATED"
    assert session["snapshot_version"] is None


def test_existing_session_keeps_bound_snapshot_after_replacement(
    seeded_service: SlowQueryService,
) -> None:
    _diagnose(seeded_service, "bound-v1", "sq-missing-index")
    before = seeded_service.get_session("bound-v1")
    seeded_service.load_snapshot(json.loads((DATA_DIR / "snapshot_v2.json").read_text()))
    after = seeded_service.get_session("bound-v1")
    assert after["snapshot_version"] == 1
    assert after["suggestions"][0]["snapshot_version"] == 1
    assert after["analysis"] == before["analysis"]
    assert seeded_service.repo.active_snapshot_version() == 2
    verified = seeded_service.verify("bound-v1")
    assert verified["verification"]["outcome"] == "VERIFIED"


def test_verification_execution_error_marks_session_failed(
    config: WorkbenchConfig,
) -> None:
    def failing_verifier(**kwargs):
        raise VerificationExecutionError(
            "E_VERIFY_ERROR", "synthetic SQLite execution failure", {"sqlite_error": "boom"}
        )

    service = SlowQueryService(config, verifier=failing_verifier)
    service.submit_samples(json.loads((DATA_DIR / "samples.json").read_text()))
    service.load_snapshot(json.loads((DATA_DIR / "snapshot_v1.json").read_text()))
    service.create_session(session_id="exec-error", sample_id="sq-missing-index")
    service.diagnose("exec-error")
    with pytest.raises(BusinessError) as caught:
        service.verify("exec-error")
    assert caught.value.code == "E_VERIFY_ERROR"
    session = service.get_session("exec-error")
    assert session["status"] == "FAILED"
    assert session["verifications"][0]["outcome"] == "FAILED"
    assert session["verifications"][0]["error_code"] == "E_VERIFY_ERROR"
    assert session["last_error_code"] == "E_VERIFY_ERROR"
