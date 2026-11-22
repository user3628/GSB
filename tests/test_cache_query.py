import pytest

from xfpipe.errors import E_LATENCY_BUDGET, PipelineError
from xfpipe.query import QueryEngine
from xfpipe.cache import QueryCache


def test_cache_hit_is_byte_identical(ran_pipeline):
    engine = ran_pipeline.query_engine
    first = engine.query("explained_variance")
    second = engine.query("explained_variance")
    assert first["cache_hit"] is False
    assert second["cache_hit"] is True
    assert first["result"] == second["result"]


def test_cache_eviction_does_not_change_results(ran_pipeline):
    ran_pipeline.cache.capacity = 1
    engine = ran_pipeline.query_engine
    a1 = engine.query("explained_variance")
    b1 = engine.query("combinations")
    a2 = engine.query("explained_variance")  # evicted -> recompute
    assert ran_pipeline.cache.evictions >= 1
    assert a1["result"] == a2["result"]
    assert b1["cache_hit"] is False


def test_query_results_stable_across_orders(ran_pipeline):
    engine = ran_pipeline.query_engine
    engine.query("combinations")
    ev1 = engine.query("explained_variance")["result"]
    engine2 = QueryEngine(ran_pipeline.pipeline, QueryCache(16), 500)
    ev2 = engine2.query("explained_variance")["result"]
    assert ev1 == ev2


def test_latency_budget_exceeded(ran_pipeline):
    engine = QueryEngine(ran_pipeline.pipeline, QueryCache(4), latency_budget_ms=1e-9)
    with pytest.raises(PipelineError) as exc:
        engine.query("feature_summary")
    assert exc.value.code == E_LATENCY_BUDGET
    assert exc.value.details["query_id"]
    assert "elapsed_ms" in exc.value.details
    assert exc.value.details["budget_ms"] == 1e-9


def test_latency_traces_recorded(ran_pipeline):
    engine = ran_pipeline.query_engine
    engine.query("combinations")
    traces = ran_pipeline.query_engine.traces
    assert traces and traces[-1]["kind"] == "combinations"
    assert traces[-1]["elapsed_ms"] <= traces[-1]["budget_ms"]
