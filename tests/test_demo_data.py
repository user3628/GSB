from __future__ import annotations


def test_demo_dataset_has_required_coverage(data_index) -> None:
    assert data_index["seed"] == 20261122
    assert data_index["sample_rate"] == 16000
    assert data_index["channels"] == 1
    assert data_index["bit_depth"] == 16
    assert data_index["shard_count"] >= 4
    assert data_index["total_samples"] == sum(item["sample_count"] for item in data_index["shard_order"])
    assert "manifest_v1.json" in data_index["manifests"]
    assert "manifest_v2.json" in data_index["manifests"]
    symbols = [symbol for item in data_index["shard_order"] for symbol in item["unit_symbols"]]
    assert any(any("\u4e00" <= char <= "\u9fff" for char in symbol) for symbol in symbols)
    assert any(any("a" <= char.lower() <= "z" for char in symbol) for symbol in symbols)
    assert "anomaly_timeline.json" in data_index["anomalies"]
    assert "anomaly_numeric.json" in data_index["anomalies"]

