from __future__ import annotations

import sqlite3

import pytest

from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService
from prep_pipeline.verifier import ResultVerifier

from tests.helpers import batch_request


def test_verifier_matches_all_stages(bootstrapped_service: PipelineService) -> None:
    bootstrapped_service.submit_batch(batch_request("verify-good"))
    report = ResultVerifier(
        bootstrapped_service.settings, bootstrapped_service.db
    ).verify("verify-good")
    assert report["status"] == "match"
    assert report["first_divergence_stage"] is None
    assert [stage["stage"] for stage in report["stage_results"]] == [
        "parse_clean",
        "filter",
        "shard",
        "sample",
        "tensorize",
    ]
    assert all(stage["matches"] for stage in report["stage_results"])


def test_first_divergence_stage_and_two_sided_digests(
    bootstrapped_service: PipelineService,
) -> None:
    bootstrapped_service.submit_batch(batch_request("verify-corrupt"))
    with sqlite3.connect(bootstrapped_service.settings.db_path) as conn:
        conn.execute(
            "UPDATE stage_facts SET output_digest = ? "
            "WHERE batch_id = 'verify-corrupt' AND stage_name = 'sample'",
            ("sha256:" + "f" * 64,),
        )
        conn.commit()

    with pytest.raises(PipelineError) as exc:
        ResultVerifier(bootstrapped_service.settings, bootstrapped_service.db).verify(
            "verify-corrupt"
        )
    assert exc.value.code == "E_REPRO_MISMATCH"
    assert exc.value.context["first_divergence_stage"] == "sample"
    assert exc.value.context["stored_digest"] == "sha256:" + "f" * 64
    assert exc.value.context["recomputed_digest"].startswith("sha256:")
    suggestions = exc.value.context["verification"]["suggestion_scope"]
    assert suggestions["first_stage"] == "sample"
    assert any("seed" in item for item in suggestions["checks"])


def test_historical_policy_replay_uses_stored_snapshot(
    bootstrapped_service: PipelineService,
) -> None:
    bootstrapped_service.submit_batch(batch_request("history-v1", policy_version=1))
    bootstrapped_service.submit_batch(batch_request("history-v2", policy_version=2))
    verifier = ResultVerifier(bootstrapped_service.settings, bootstrapped_service.db)

    current_history = verifier.verify("history-v1", policy_version=1)
    assert current_history["status"] == "match"

    cross_version = verifier.verify(
        "history-v1", policy_version=2, baseline_batch_id="history-v2"
    )
    assert cross_version["status"] == "match"
    assert cross_version["comparison_mode"] == "explicit_baseline"
    assert cross_version["selected_policy_version"] == 2


def test_policy_replay_without_baseline_is_explicit(
    bootstrapped_service: PipelineService,
) -> None:
    bootstrapped_service.submit_batch(batch_request("history-only-v1", policy_version=1))
    report = ResultVerifier(
        bootstrapped_service.settings, bootstrapped_service.db
    ).verify("history-only-v1", policy_version=2)
    assert report["status"] == "replayed_unpaired"
    assert report["comparable"] is False
    assert report["first_divergence_stage"] is None
