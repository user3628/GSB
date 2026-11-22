from __future__ import annotations

import sqlite3

import pytest

from app.errors import BusinessError, E_SPEC_INVALID, E_TRACE_OUT_OF_ORDER
from tests.conftest import spec


@pytest.mark.parametrize(
    "bad_override",
    [
        {"init_us": -1},
        {"memory_bytes": True},
        {"per_instance_concurrency": 0},
        {"function_concurrency_limit": 0},
        {"reserved_quota": 2, "function_concurrency_limit": 1},
        {"init_us": 2**63},
        {"memory_bytes": 1.5},
    ],
)
def test_invalid_spec_rejects_whole_batch(engine_factory, bad_override):
    engine = engine_factory()
    payload = {
        "functions": [
            spec("valid-one"),
            spec("invalid-one", **bad_override),
        ]
    }
    with pytest.raises(BusinessError) as exc_info:
        engine.load_specs(payload)
    assert exc_info.value.code == E_SPEC_INVALID
    assert engine.list_functions() == []
    with sqlite3.connect(engine.settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM function_specs").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM spec_batches").fetchone()[0] == 0


def test_trace_unknown_function_does_not_partially_insert(engine_factory):
    engine = engine_factory()
    engine.load_specs({"functions": [spec("known")]})
    payload = {
        "batch_id": "bad-trace",
        "calls": [
            {
                "call_id": "ok",
                "function_id": "known",
                "arrival_ms": 1,
                "execution_us": 1000,
                "priority": 1,
                "retry": False,
            },
            {
                "call_id": "bad",
                "function_id": "undeclared",
                "arrival_ms": 2,
                "execution_us": 1000,
                "priority": 1,
                "retry": False,
            },
        ],
    }
    with pytest.raises(BusinessError) as exc_info:
        engine.load_trace(payload)
    assert exc_info.value.code == E_SPEC_INVALID
    with sqlite3.connect(engine.settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM trace_batches").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM trace_records").fetchone()[0] == 0


def test_out_of_order_trace_rejected_without_insert(engine_factory):
    engine = engine_factory()
    engine.load_specs({"functions": [spec("known")]})
    with pytest.raises(BusinessError) as exc_info:
        engine.load_trace(
            {
                "batch_id": "reordered",
                "calls": [
                    {
                        "call_id": "late",
                        "function_id": "known",
                        "arrival_ms": 20,
                        "execution_us": 1000,
                        "priority": 1,
                        "retry": False,
                    },
                    {
                        "call_id": "early",
                        "function_id": "known",
                        "arrival_ms": 10,
                        "execution_us": 1000,
                        "priority": 1,
                        "retry": False,
                    },
                ],
            }
        )
    assert exc_info.value.code == E_TRACE_OUT_OF_ORDER
    with sqlite3.connect(engine.settings.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM trace_records").fetchone()[0] == 0


def test_valid_sample_files_load(engine_factory):
    import json
    from app.config import ROOT

    engine = engine_factory()
    specs = json.loads((ROOT / "samples" / "functions.json").read_text(encoding="utf-8"))
    trace = json.loads((ROOT / "samples" / "trajectory.json").read_text(encoding="utf-8"))
    spec_result = engine.load_specs(specs)
    trace_result = engine.load_trace(trace)
    assert spec_result["function_count"] == 4
    assert trace_result["record_count"] == 24

