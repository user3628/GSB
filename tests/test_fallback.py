from __future__ import annotations

from qiu.engine import QueryEngine


def _sample(query_id: str, channel: str, history: list[str] | None = None) -> dict[str, object]:
    return {
        "channel": channel,
        "history_categories": history or [],
        "locale": "zh-CN",
        "query_id": query_id,
        "raw_query": "你好",
    }


def test_confidence_falls_back_to_history_then_channel_then_final(engine: QueryEngine) -> None:
    history = engine.parse(_sample("fb-history", "web", ["餐厅"]))
    assert history["intent"]["used_fallback_level"] == 1
    assert history["intent"]["source"] == "history_prior"
    assert history["intent"]["label"] == "local_service"
    assert history["intent"]["fallback_trace"][0]["reason"] == "confidence_below_threshold"
    assert [item["status"] for item in history["intent"]["fallback_trace"][2:]] == ["skipped", "skipped"]

    channel = engine.parse(_sample("fb-channel", "app"))
    assert channel["intent"]["used_fallback_level"] == 2
    assert channel["intent"]["source"] == "channel_default"
    assert channel["intent"]["label"] == "navigation"
    assert channel["intent"]["fallback_trace"][1]["reason"] == "no_mapped_history"

    final = engine.parse(_sample("fb-final", "unknown-channel"))
    assert final["intent"]["used_fallback_level"] == 3
    assert final["intent"]["source"] == "final_default"
    assert final["intent"]["label"] == "informational"
    assert final["intent"]["fallback_trace"][2]["reason"] == "unmapped_channel"


def test_confident_primary_marks_lower_levels_as_skipped(engine: QueryEngine) -> None:
    result = engine.parse(
        {
            "channel": "app",
            "history_categories": [],
            "locale": "zh-CN",
            "query_id": "fb-primary",
            "raw_query": "华为官网",
        }
    )
    assert result["intent"]["used_fallback_level"] == 0
    trace = result["intent"]["fallback_trace"]
    assert trace[0]["status"] == "selected"
    assert [item["status"] for item in trace[1:]] == ["skipped", "skipped", "skipped"]
