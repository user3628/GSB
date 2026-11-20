import json

from app.compare import compute_batch, compute_run_quantiles
from app.config import SAMPLES_DIR
from app.scheduler import schedule_trace
from app.spec import parse_trace_payload


def _payload(name):
    catalog = json.loads((SAMPLES_DIR / "models.json").read_text(encoding="utf-8"))
    trace = json.loads((SAMPLES_DIR / name).read_text(encoding="utf-8"))
    return {
        "models": catalog["models"],
        "node": trace.get("node", catalog["node"]),
        "trace": trace,
    }


def test_normal_schedule_is_deterministic_and_priority_isolated():
    trace = parse_trace_payload(_payload("trace_normal.json"))
    first = schedule_trace(trace)
    second = schedule_trace(trace)
    assert first.deterministic_digest == second.deterministic_digest
    assert [batch.canonical_payload() for batch in first.batches] == [
        batch.canonical_payload() for batch in second.batches
    ]
    assert len(first.batches) == 5
    llm_priority_1 = [b for b in first.batches if b.model_id == "llm-7b" and b.priority == 1]
    llm_priority_2 = [b for b in first.batches if b.model_id == "llm-7b" and b.priority == 2]
    assert len(llm_priority_1) == 1
    assert len(llm_priority_2) == 1
    assert not set(member.request.request_id for member in llm_priority_1[0].members) & set(
        member.request.request_id for member in llm_priority_2[0].members
    )
    assert any("LATENCY_CAP" in batch.split_reasons for batch in first.batches)


def test_batch_constraints_and_quantiles_are_declared():
    trace = parse_trace_payload(_payload("trace_normal.json"))
    schedule = schedule_trace(trace)
    for batch in schedule.batches:
        model = trace.models[batch.model_id]
        assert batch.sample_count <= model.max_batch_samples
        assert batch.memory_bytes <= schedule.node_initial_free_bytes
        computation = compute_batch(batch)
        for quantile in computation.quantiles:
            assert quantile.sample_count == batch.member_count
            assert quantile.rank >= 1
            assert quantile.rank <= quantile.sample_count
    run_quantiles = compute_run_quantiles(schedule.batches)
    assert [item.label for item in run_quantiles] == ["P50", "P95", "P99"]


def test_fragment_sample_downbatches_and_records_bitmaps_and_mixed_rejection():
    trace = parse_trace_payload(_payload("trace_fragment.json"))
    schedule = schedule_trace(trace)
    assert len(schedule.fragmentation_events) >= 1
    assert len(schedule.mixed_rejections) >= 1
    rejection = schedule.mixed_rejections[0]
    assert rejection["code"] == "MIXED_BATCH_REJECTED_LATENCY_CAP"
    assert rejection["combined_memory_bytes"] <= rejection["region_size_bytes"]
    assert rejection["combined_latency_us"] > rejection["latency_cap_us"]
    event = schedule.fragmentation_events[0]
    assert event.before_bitmap["text"] != event.after_bitmap["text"]
    assert event.constraint_assertions["within_model_max"] is True
    for item in event.constraint_assertions["per_member_samples"]:
        assert item["within_model_max"] is True
    for batch in schedule.batches:
        assert batch.sample_count <= trace.models[batch.model_id].max_batch_samples
    fragments = [
        member
        for batch in schedule.batches
        for member in batch.members
        if member.request.request_id == "frag-large-40"
    ]
    assert sum(member.samples for member in fragments) == 40
    assert len(fragments) >= 3
    assert all(member.part_count == len(fragments) for member in fragments)


def test_precision_drift_sample_is_marked_failed():
    trace = parse_trace_payload(_payload("trace_precision_drift.json"))
    schedule = schedule_trace(trace)
    computation = compute_batch(schedule.batches[0])
    assert computation.validation_state == "VALIDATION_FAILED"
    assert any("latency_us" in item for item in computation.failed_fields)
    assert computation.quantiles[0].raw_us == 1
    assert computation.quantiles[0].rounded_us == 0
