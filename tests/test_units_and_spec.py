import pytest

from gpworkbench.errors import E_SPEC_INVALID, BusinessError, SpecValidationError
from gpworkbench.spec import parse_workload
from gpworkbench.units import MAX_INT64, parse_compute, parse_memory, parse_positive_int


def test_parse_memory_binary_units():
    assert parse_memory("40Gi") == 40 * 1024 ** 3
    assert parse_memory("512Mi") == 512 * 1024 ** 2
    assert parse_memory("1024B") == 1024
    assert parse_memory(2048) == 2048
    assert parse_memory(" 16Gi ") == 16 * 1024 ** 3


def test_parse_compute_milli_units():
    assert parse_compute("250m") == 250
    assert parse_compute("1000") == 1000
    assert parse_compute(0) == 0


def test_parse_rejects_negative_and_overflow():
    with pytest.raises(SpecValidationError) as exc:
        parse_memory("-1Gi")
    assert exc.value.code == E_SPEC_INVALID
    with pytest.raises(SpecValidationError):
        parse_compute("-5m")
    with pytest.raises(SpecValidationError):
        parse_memory(MAX_INT64 + 1)
    with pytest.raises(SpecValidationError):
        parse_memory("40Gb")
    with pytest.raises(SpecValidationError):
        parse_positive_int(0, "samples")


def test_spec_invalid_does_not_touch_database(tmp_path):
    from gpworkbench.database import Database
    from gpworkbench.scheduler import SchedulerService

    db = Database(tmp_path / "empty.sqlite3")
    svc = SchedulerService(db)
    bad = {
        "quota_groups": [{"group_id": "g", "memory_limit": "10Gi", "compute_limit": "100m"}],
        "nodes": [
            {
                "node_id": "n",
                "total_memory": "10Gi",
                "total_compute": "100m",
                "used_memory": "20Gi",
                "group_id": "g",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m", "cost_per_sample": 1, "max_batch_samples": 4}],
        "requests": [],
    }
    with pytest.raises(BusinessError) as exc:
        svc.load_spec(parse_workload(bad))
    assert exc.value.code == E_SPEC_INVALID
    assert svc.repo.list_nodes() == []
    assert svc.repo.list_groups() == []


def test_spec_rejects_dangling_group_reference():
    payload = {
        "quota_groups": [
            {"group_id": "g1", "memory_limit": "10Gi", "compute_limit": "100m"}
        ],
        "nodes": [
            {
                "node_id": "n1",
                "total_memory": "10Gi",
                "total_compute": "100m",
                "used_memory": "1Gi",
                "group_id": "g1",
                "health": "HEALTHY",
            }
        ],
        "models": [{"model_id": "m1", "cost_per_sample": 1, "max_batch_samples": 4}],
        "requests": [
            {
                "request_id": "x1",
                "service_name": "s",
                "model_id": "m1",
                "memory_need": "1Gi",
                "compute_need": "10m",
                "samples_per_batch": 1,
                "latency_limit_ms": 10,
                "priority": 1,
                "group_id": "ghost",
            }
        ],
    }
    with pytest.raises(SpecValidationError):
        parse_workload(payload)
