from __future__ import annotations

import copy

from conftest import finalized


EXPECTED_ORDER = [1, 9001, 2, 3, 7, 4, 5, 6, 10, 1002, 1003, 12, 13, 14, 15]


def replace_conflict_rule(service, snapshot_document, rule: str):
    document = copy.deepcopy(snapshot_document)
    document["snapshot_version"] = 2
    document["generated_at"] = "2026-11-21T05:00:00Z"
    document["tables"]["orders"]["conflict_rule"] = rule
    service.load_snapshot(finalized(document))


def test_merge_is_stable_and_deduplicates_same_primary_key(service):
    result = service.execute_query(
        {
            "request_id": "merge-first",
            "logical_table": "orders",
            "query_type": "scan",
            "limit": 100,
        }
    )
    assert [row["id"] for row in result["rows"]] == EXPECTED_ORDER
    assert result["page"]["total_after_conflict"] == len(EXPECTED_ORDER)
    assert len({row["id"] for row in result["rows"]}) == len(result["rows"])
    assert len(result["conflicts"]) == 1
    conflict = result["conflicts"][0]
    assert conflict["primary_key"] == 9001
    assert conflict["winner"]["physical_table"] == "orders_0000"
    assert conflict["discarded"]["physical_table"] == "orders_0001"


def test_last_and_max_updated_conflict_rules(service, snapshot_document):
    replace_conflict_rule(service, snapshot_document, "physical_order_last")
    last = service.execute_query(
        {
            "request_id": "conflict-last",
            "logical_table": "orders",
            "query_type": "scan",
        }
    )
    selected = next(row for row in last["rows"] if row["id"] == 9001)
    assert selected["source"] == "orders_0001"
    assert last["conflicts"][0]["winner"]["physical_table"] == "orders_0001"


def test_max_updated_at_conflict_rule(service, snapshot_document):
    replace_conflict_rule(service, snapshot_document, "max_updated_at_physical_order")
    result = service.execute_query(
        {
            "request_id": "conflict-max-updated",
            "logical_table": "orders",
            "query_type": "scan",
        }
    )
    selected = next(row for row in result["rows"] if row["id"] == 9001)
    assert selected["source"] == "orders_0001"
    assert selected["updated_at"] == "2026-11-20T01:01:00Z"


def test_limit_offset_pages_concatenate_without_gaps_or_duplicates(service):
    full = service.execute_query(
        {
            "request_id": "pagination-full",
            "logical_table": "orders",
            "query_type": "scan",
            "limit": 1000,
            "offset": 0,
        }
    )
    pages = []
    for index, offset in enumerate(range(0, 18, 3)):
        page = service.execute_query(
            {
                "request_id": f"pagination-page-{index}",
                "logical_table": "orders",
                "query_type": "scan",
                "limit": 3,
                "offset": offset,
            }
        )
        pages.extend(row["id"] for row in page["rows"])
    assert pages == [row["id"] for row in full["rows"]]
    assert len(pages) == len(set(pages))

    empty_page = service.execute_query(
        {
            "request_id": "pagination-zero",
            "logical_table": "orders",
            "query_type": "scan",
            "limit": 0,
            "offset": 0,
        }
    )
    assert empty_page["rows"] == []
    assert empty_page["page"]["total_after_conflict"] == len(EXPECTED_ORDER)

