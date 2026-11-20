from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BusinessError

from .conftest import digest, write


def test_write_persists_version_digest_and_timestamp(service):
    before = service.get_current_view("vol0")
    content = b"block-zero-revision-2"
    result = write(service, 0, content, "write-1")

    assert result["ok"] is True
    assert result["previous_version"] == 1
    assert result["version"] == 2
    assert result["digest"] == digest(content)
    assert result["consistency_seq"] == before["consistency_seq"] + 1
    assert result["cow_performed"] is False

    after = service.get_current_view("vol0")
    block0 = after["blocks"][0]
    assert block0["version"] == 2
    assert block0["digest"] == digest(content)
    assert block0["last_written_at"] == result["written_at"]


def test_invalid_block_returns_business_code_without_mutation(service):
    before = service.get_current_view("vol0")
    with pytest.raises(BusinessError) as exc:
        write(service, 99, b"invalid", "invalid-block")
    assert exc.value.code == "E_BLOCK_NOT_FOUND"
    assert service.get_current_view("vol0") == before


def test_digest_mismatch_returns_e_block_corrupt_without_mutation(service):
    before = service.get_current_view("vol0")
    with pytest.raises(BusinessError) as exc:
        service.write_block("vol0", 1, b"payload", "0" * 64, "bad-digest")
    assert exc.value.code == "E_BLOCK_CORRUPT"
    assert service.get_current_view("vol0") == before
    versions = service.get_block_versions("vol0", 1)["versions"]
    assert [item["version"] for item in versions] == [1]


def test_same_request_id_reuses_result_and_conflict_is_rejected(service):
    content = b"idempotent"
    first = write(service, 2, content, "same-request")
    second = write(service, 2, content, "same-request")
    assert second == first
    assert [v["version"] for v in service.get_block_versions("vol0", 2)["versions"]] == [1, 2]

    with pytest.raises(BusinessError) as exc:
        write(service, 2, b"different", "same-request")
    assert exc.value.code == "E_CONFLICTING_REQUEST"
    assert service.get_current_view("vol0")["blocks"][2]["version"] == 2


def test_concurrent_duplicate_request_executes_once(service):
    barrier = threading.Barrier(2)

    def invoke() -> dict:
        barrier.wait(timeout=5)
        return write(service, 1, b"one-write", "duplicate-concurrent")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: invoke(), range(2)))

    assert results[0] == results[1]
    versions = service.get_block_versions("vol0", 1)["versions"]
    assert [item["version"] for item in versions] == [1, 2]


def test_concurrent_writes_are_serialized_per_volume(service):
    def invoke(index: int) -> dict:
        return write(service, 3, f"writer-{index}".encode(), f"writer-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(invoke, range(20)))

    assert all(item["ok"] for item in results)
    versions = service.get_block_versions("vol0", 3)["versions"]
    assert [item["version"] for item in versions] == list(range(1, 22))
    assert service.get_current_view("vol0")["blocks"][3]["version"] == 21

