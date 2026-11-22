from __future__ import annotations

import pytest

from rca_pipeline.errors import (
    ChecksumMismatch,
    ConflictingBatch,
    SampleInvalid,
    StaleBase,
    TopologyInvalid,
)
from tests.conftest import submit_topology_v1


def test_sample_before_topology_rejected(pipeline, dataset):
    with pytest.raises(TopologyInvalid):
        pipeline.submit_samples(dataset[0])


def test_full_happy_path_batches(pipeline, dataset, topo_v1):
    submit_topology_v1(pipeline, topo_v1)
    result1 = pipeline.submit_samples(dataset[0])
    assert result1["record_count"] == 588
    assert result1["duplicate_count"] == 1
    assert result1["new_version"] == 1

    result2 = pipeline.submit_samples(dataset[1])
    assert result2["new_version"] == 2
    result3 = pipeline.submit_samples(dataset[2])
    assert result3["new_version"] == 3


def test_invalid_sample_whole_batch_rejected(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    bad = {
        "commit_id": "bad-batch",
        "base_version": 0,
        "records": [dict(dataset[0]["records"][0]),
                    {"ts_ms": 1, "service": "gateway", "metric": "m",
                     "is_good": "yes", "latency_us": 10}],
    }
    with pytest.raises(SampleInvalid):
        pipeline.submit_samples(bad)
    # Nothing landed: the next valid base-0 batch must still succeed.
    ok = pipeline.submit_samples(dataset[0])
    assert ok["new_version"] == 1


def test_undeclared_service_rejected(pipeline, topo_v1):
    submit_topology_v1(pipeline, topo_v1)
    payload = {
        "commit_id": "ghost",
        "base_version": 0,
        "records": [{"ts_ms": 1, "service": "ghost-service", "metric": "m",
                     "is_good": True, "latency_us": 10}],
    }
    with pytest.raises(SampleInvalid) as exc:
        pipeline.submit_samples(payload)
    assert exc.value.code == "E_SAMPLE_INVALID"


def test_stale_base_sample(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_samples(dataset[0])
    stale = dict(dataset[1])
    stale["base_version"] = 0
    with pytest.raises(StaleBase) as exc:
        pipeline.submit_samples(stale)
    assert exc.value.code == "E_STALE_BASE"


def test_idempotent_replay_and_conflict(pipeline, topo_v1, dataset):
    submit_topology_v1(pipeline, topo_v1)
    first = pipeline.submit_samples(dataset[0])
    replay = pipeline.submit_samples(dataset[0])
    assert replay["replayed"] is True
    assert replay["new_version"] == first["new_version"]

    conflict = dict(dataset[0])
    conflict["records"] = [dict(record, latency_us=record["latency_us"] + 1)
                           for record in conflict["records"][:5]]
    with pytest.raises(ConflictingBatch) as exc:
        pipeline.submit_samples(conflict)
    assert exc.value.code == "E_CONFLICTING_BATCH"


def test_topology_stale_increment(pipeline, topo_v1, topo_v2, dataset):
    submit_topology_v1(pipeline, topo_v1)
    stale = dict(topo_v2)
    stale["base_version"] = 99
    with pytest.raises(StaleBase):
        pipeline.submit_topology(stale)


def test_topology_checksum_mismatch(pipeline, topo_v1):
    bad = dict(topo_v1)
    bad["checksum"] = "deadbeef"
    with pytest.raises(ChecksumMismatch):
        pipeline.submit_topology(bad)


def test_snapshot_replacement_does_not_mix(pipeline, topo_v1, topo_v3):
    submit_topology_v1(pipeline, topo_v1)
    pipeline.submit_topology(topo_v3)
    report = pipeline.diagnostic_report()
    services = {row["service"] for row in report["service_refs"]}
    # cache was removed in the v3 replacement; no derived artifacts may remain.
    assert "cache" not in services
    with pytest.raises(SampleInvalid):
        pipeline.submit_samples({
            "commit_id": "cache-after-removal",
            "base_version": 0,
            "records": [{"ts_ms": 1, "service": "cache", "metric": "m",
                         "is_good": True, "latency_us": 10}],
        })
