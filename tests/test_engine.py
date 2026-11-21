from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor

import pytest

from chunk_engine.engine import EngineConfig
from chunk_engine.errors import (
    BusinessError,
    E_BATCH_NOT_FOUND,
    E_BLOCK_CORRUPT,
    E_KEY_SET_TOO_LARGE,
    E_MEMORY_BUDGET_EXCEEDED,
    E_PAGE_CONSUMED,
)


def test_query_nulls_float_format_and_stable_order(engine_factory) -> None:
    engine = engine_factory((1,))
    response = engine.create_batch(
        [113, 97, 89, 1],
        ["label", "score", "region", "shard", "id"],
        page_size=10,
    )
    rows = response["page"]["rows"]
    assert [row["key"] for row in rows] == [1, 89, 97, 113]
    assert response["page"]["columns"][0]["name"] == "id"
    by_key = {row["key"]: row for row in rows}
    columns = [column["name"] for column in response["page"]["columns"]]
    score_index = columns.index("score")
    region_index = columns.index("region")
    label_index = columns.index("label")
    assert by_key[89]["null_mask"][score_index] is True
    assert by_key[89]["values"][score_index] is None
    assert by_key[97]["null_mask"][region_index] is True
    assert by_key[113]["null_mask"][label_index] is True
    assert re.fullmatch(r"-?\d+\.\d{6}", by_key[1]["values"][score_index])
    assert response["page"]["rounding"]["decimal_places"] == 6
    assert response["page"]["rounding"]["float_columns"] == ["score"]


def test_empty_key_set_returns_empty_first_page(engine_factory) -> None:
    engine = engine_factory((1,))
    response = engine.create_batch([], ["id", "score"], page_size=5)
    assert response["batch"]["total_rows"] == 0
    assert response["batch"]["total_pages"] == 1
    assert response["page"]["rows"] == []
    assert response["page"]["null_mask"] == {"id": [], "score": []}


def test_key_limit_is_business_error(engine_factory) -> None:
    config = EngineConfig(
        total_memory_budget_bytes=2 * 1024 * 1024,
        single_task_budget_bytes=16 * 1024,
        max_parallel_blocks=4,
        max_keys=2,
        max_page_size=50,
    )
    engine = engine_factory((1,), config=config)
    with pytest.raises(BusinessError) as raised:
        engine.create_batch([1, 2, 3], ["id"], page_size=2)
    assert raised.value.code == E_KEY_SET_TOO_LARGE


def test_pagination_concat_matches_full_and_claims_once(engine_factory) -> None:
    engine = engine_factory((1,))
    keys = list(range(1, 901))
    response = engine.create_batch(keys, ["id", "score", "region"], page_size=17)
    handle = response["result_handle"]
    assert response["batch"]["total_pages"] == 53
    rows = list(response["page"]["rows"])

    def fetch(cid: str):
        try:
            return engine.get_page(handle, 1, cid)
        except BusinessError as error:
            return error

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(fetch, [f"racer-{index}" for index in range(8)]))
    successes = [item for item in outcomes if isinstance(item, dict)]
    failures = [item for item in outcomes if isinstance(item, BusinessError)]
    assert len(successes) == 1
    assert len(failures) == 7
    assert all(error.code == E_PAGE_CONSUMED for error in failures)
    rows.extend(successes[0]["rows"])
    for page_no in range(2, response["batch"]["total_pages"]):
        rows.extend(engine.get_page(handle, page_no, f"reader-{page_no}")["rows"])
    assert [row["key"] for row in rows] == keys
    assert len({row["key"] for row in rows}) == len(keys)
    meta, internal_rows = engine.read_result_rows(handle)
    internal = list(internal_rows)
    assert len(internal) == len(rows) == meta["row_count"]
    assert [row["key"] for row in internal] == keys
    verification = engine.verify_pagination(handle)
    assert verification["equal"] is True
    assert verification["row_count"] == len(keys)
    assert verification["page_count"] == response["batch"]["total_pages"]


def test_stream_and_batch_are_bitwise_equal_and_deterministic(engine_factory) -> None:
    engine = engine_factory((1, 2))
    keys = sorted(set([1, 4999, 5000] + list(range(1000, 20001, 700))))
    comparison = engine.compare_paths(
        keys,
        ["id", "region", "shard", "score", "label"],
        page_size=31,
    )
    assert comparison["bitwise_equal"] is True
    assert comparison["within_tolerance"] is True
    assert comparison["max_absolute_deviation"] == 0.0
    assert comparison["max_relative_deviation"] == 0.0
    assert comparison["differences"] == []
    assert comparison["stream"]["selected_path"] == "stream"
    assert comparison["batch"]["selected_path"] == "batch"
    assert comparison["stream"]["result_digest"] == comparison["batch"]["result_digest"]
    assert comparison["batch"]["split_record_count"] > 0


def test_corrupt_block_fails_batch_with_context(engine_factory, sample_suite: dict) -> None:
    engine = engine_factory((1, 2, 3))
    corrupt_key = sample_suite["queries"]["corrupt_key"]
    with pytest.raises(BusinessError) as raised:
        engine.create_batch(
            [corrupt_key],
            ["id", "label"],
            page_size=10,
            batch_id="b_corrupt_test",
        )
    assert raised.value.code == E_BLOCK_CORRUPT
    assert raised.value.context["block_no"] == 1
    assert engine.db.get_batch("b_corrupt_test")["state"] == "failed"
    assert engine.db.get_batch("b_corrupt_test")["error_code"] == E_BLOCK_CORRUPT


def test_memory_budget_error_happens_in_planning(engine_factory) -> None:
    config = EngineConfig(
        total_memory_budget_bytes=8,
        single_task_budget_bytes=8,
        max_parallel_blocks=1,
        max_keys=100,
        max_page_size=10,
    )
    engine = engine_factory((2,), config=config)
    with pytest.raises(BusinessError) as raised:
        engine.create_batch([1], ["id"], page_size=2)
    assert raised.value.code == E_MEMORY_BUDGET_EXCEEDED
    assert engine.db.list_batches() == []


def test_unknown_batch_and_page_errors(engine_factory) -> None:
    engine = engine_factory((1,))
    with pytest.raises(BusinessError) as missing:
        engine.get_page("rh_does_not_exist", 0, "reader")
    assert missing.value.code == E_BATCH_NOT_FOUND
    response = engine.create_batch([1], ["id"], page_size=2)
    with pytest.raises(BusinessError) as consumed:
        engine.get_page(response["result_handle"], 0, "another-reader")
    assert consumed.value.code == E_PAGE_CONSUMED


def test_page_size_is_bounded_by_client_and_server(engine_factory) -> None:
    engine = engine_factory((1,))
    response = engine.create_batch([1, 2, 3], ["id"], page_size=100, client_max_page_size=7)
    assert response["page_size_effective"] == 7
    assert response["page"]["page_size"] == 7
    assert len(response["page"]["rows"]) == 3
