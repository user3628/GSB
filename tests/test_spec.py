import copy
import json
from pathlib import Path

import pytest

from app.config import SAMPLES_DIR
from app.errors import SpecInvalidError, TraceInvalidError
from app.spec import parse_trace_payload


def _catalog():
    return json.loads((SAMPLES_DIR / "models.json").read_text(encoding="utf-8"))


def _trace(name):
    return json.loads((SAMPLES_DIR / name).read_text(encoding="utf-8"))


def _payload(name="trace_normal.json"):
    catalog = _catalog()
    trace = _trace(name)
    return {
        "models": catalog["models"],
        "node": trace.get("node", catalog["node"]),
        "trace": trace,
    }


def test_normal_trace_normalizes_to_integers():
    trace = parse_trace_payload(_payload())
    assert trace.trace_id == "trace-normal-v1"
    assert trace.models["llm-7b"].memory_bytes == 512 * 1024**2
    assert trace.models["llm-7b"].cost_us == 1_500
    assert trace.requests[0].arrival_us == 0
    assert trace.requests[1].arrival_us == 1_000
    assert trace.payload_digest


def test_unknown_model_rejects_entire_record_set():
    payload = _payload()
    payload["trace"]["requests"][0]["model_id"] = "unknown-model"
    with pytest.raises(SpecInvalidError) as raised:
        parse_trace_payload(payload)
    assert raised.value.code == "E_SPEC_INVALID"
    assert raised.value.details["written"] is False
    assert any("unknown model" in item["message"] for item in raised.value.details["errors"])


def test_invalid_dimension_record_is_rejected():
    document = json.loads((SAMPLES_DIR / "dimensions_invalid.json").read_text(encoding="utf-8"))
    with pytest.raises(SpecInvalidError) as raised:
        parse_trace_payload(document["reject_payload"])
    assert raised.value.code == "E_SPEC_INVALID"
    assert raised.value.details["written"] is False


def test_non_monotonic_arrivals_return_trace_invalid():
    payload = _payload()
    payload["trace"]["requests"][1]["arrival_ms"] = -1
    # Use a valid numeric timestamp that moves backwards instead of tripping the negative parser.
    payload["trace"]["requests"][1]["arrival_ms"] = 0
    payload["trace"]["requests"][0]["arrival_ms"] = 9
    with pytest.raises(TraceInvalidError) as raised:
        parse_trace_payload(payload)
    assert raised.value.code == "E_TRACE_INVALID"
    assert raised.value.details["written"] is False


def test_latency_cap_smaller_than_one_sample_is_spec_invalid():
    payload = _payload()
    payload["trace"]["requests"][0]["expected_batch_latency_ms"] = "1us"
    with pytest.raises(SpecInvalidError) as raised:
        parse_trace_payload(payload)
    assert raised.value.code == "E_SPEC_INVALID"
    assert any("smaller than one sample" in item["message"] for item in raised.value.details["errors"])


def test_payload_is_not_mutated():
    payload = _payload("trace_fragment.json")
    original = copy.deepcopy(payload)
    parse_trace_payload(payload)
    assert payload == original
