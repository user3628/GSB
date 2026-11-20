from __future__ import annotations

import pytest

from app.errors import BusinessError
from app.quantity import MAX_INT64, parse_cpu_millicores, parse_memory_bytes


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("500m", 500),
        ("2", 2000),
        ("1.5", 1500),
        ("0.25", 250),
        ("0", 0),
        (3, 3000),
    ],
)
def test_parse_cpu_exact_millicores(value: object, expected: int) -> None:
    assert parse_cpu_millicores(value, path="cpu") == expected


@pytest.mark.parametrize("value", ["10Mi", "1.5m", "-1", "1Gi", "", "nan", True, 1.5])
def test_parse_cpu_rejects_invalid_or_sub_millicore(value: object) -> None:
    with pytest.raises(BusinessError) as caught:
        parse_cpu_millicores(value, path="pods[0].resources.requests.cpu")
    assert caught.value.code == "E_INVALID_QUANTITY"
    assert caught.value.details["path"] == "pods[0].resources.requests.cpu"


def test_parse_cpu_overflow() -> None:
    with pytest.raises(BusinessError) as caught:
        parse_cpu_millicores(MAX_INT64, path="cpu")
    assert caught.value.code == "E_INVALID_QUANTITY"
    assert "64-bit" in caught.value.message


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("512Mi", 512 * 1024**2),
        ("1Gi", 1024**3),
        ("1.5Gi", 3 * 1024**3 // 2),
        ("1000M", 1_000_000_000),
        ("1024", 1024),
        (4096, 4096),
    ],
)
def test_parse_memory_exact_bytes(value: object, expected: int) -> None:
    assert parse_memory_bytes(value, path="memory") == expected


@pytest.mark.parametrize("value", ["1m", "1GiB", "1.1", "-2Gi", "", False, 3.5])
def test_parse_memory_rejects_invalid_units(value: object) -> None:
    with pytest.raises(BusinessError) as caught:
        parse_memory_bytes(value, path="nodes[0].allocatable.memory")
    assert caught.value.code == "E_INVALID_QUANTITY"


def test_parse_memory_overflow_is_rejected() -> None:
    with pytest.raises(BusinessError) as caught:
        parse_memory_bytes(str(MAX_INT64 + 1), path="memory")
    assert caught.value.code == "E_INVALID_QUANTITY"
