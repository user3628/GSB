from __future__ import annotations

import pytest

from prep_pipeline.errors import PipelineError
from prep_pipeline.models import InjectionOptions
from prep_pipeline.service import PipelineService

from tests.helpers import batch_request


def test_five_injection_codes_are_distinct_and_uncommitted(
    bootstrapped_service: PipelineService,
) -> None:
    stable = bootstrapped_service.submit_batch(batch_request("stable-before-injections"))
    stable_digests = {stage["stage"]: stage["output_digest"] for stage in stable["stages"]}
    cases = {
        "inject-label": InjectionOptions(invalid_label=True),
        "inject-shard": InjectionOptions(missing_shard_key=True),
        "inject-ratio": InjectionOptions(sample_ratio=1.25),
        "inject-conflict": InjectionOptions(duplicate_conflict=True),
        "inject-interrupt": InjectionOptions(
            interrupt_at_stage="shard", interrupt_after_records=2
        ),
    }
    observed: dict[str, str] = {}
    for batch_id, injection in cases.items():
        with pytest.raises(PipelineError) as exc:
            bootstrapped_service.submit_batch(
                batch_request(batch_id, injections=injection)
            )
        observed[batch_id] = exc.value.code
        assert bootstrapped_service.db.get_batch(batch_id)["status"] == "failed"
        with pytest.raises(PipelineError) as hidden:
            bootstrapped_service.get_batch(batch_id)
        assert hidden.value.code == "E_BATCH_NOT_COMMITTED"

    assert observed == {
        "inject-label": "E_LABEL_INVALID",
        "inject-shard": "E_SHARD_KEY_MISSING",
        "inject-ratio": "E_SAMPLE_RATIO_INVALID",
        "inject-conflict": "E_CONFLICTING_RECORD",
        "inject-interrupt": "E_INJECTED_INTERRUPT",
    }
    assert len(set(observed.values())) == 5
    after = bootstrapped_service.get_batch("stable-before-injections")
    assert {stage["stage"]: stage["output_digest"] for stage in after["stages"]} == stable_digests


def test_interrupt_marker_reports_only_completed_stage_facts(
    bootstrapped_service: PipelineService,
) -> None:
    with pytest.raises(PipelineError) as exc:
        bootstrapped_service.submit_batch(
            batch_request(
                "interrupt-context",
                injections=InjectionOptions(
                    interrupt_at_stage="shard", interrupt_after_records=3
                ),
            )
        )
    assert exc.value.code == "E_INJECTED_INTERRUPT"
    assert exc.value.context["completed_stages"] == ["parse_clean", "filter"]
    facts = bootstrapped_service.db.get_stage_facts("interrupt-context")
    assert [fact["stage_name"] for fact in facts] == ["parse_clean", "filter", "shard"]
    assert facts[-1]["status"] == "failed"
