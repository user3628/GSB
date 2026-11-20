from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest

from inverted_index.errors import BusinessError
from inverted_index.storage import IndexStore, SearchClause


def test_query_started_during_commit_uses_prior_version(tmp_path, index_fields) -> None:
    entered = threading.Event()
    release = threading.Event()
    armed = threading.Event()

    def hook(name: str) -> None:
        if name == "batch:after_changes" and armed.is_set():
            entered.set()
            assert release.wait(10)

    store = IndexStore(tmp_path / "index.db", index_fields, failpoint_hook=hook)
    first = store.write_batch(
        inserts=[{"_id": "doc-a", "body": "old text"}], batch_id="initial"
    )
    armed.set()
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            store.write_batch(
                updates=[{"_id": "doc-a", "body": "newtoken text"}], batch_id="slow-update"
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert entered.wait(5)
    during = store.search(
        clauses=[SearchClause(kind="term", field_path="body", term="newtoken")]
    )
    assert during.version == first.version
    assert during.total == 0
    assert store.get_document("doc-a")["body"] == "old text"
    release.set()
    thread.join(10)
    assert not errors
    after = store.search(
        clauses=[SearchClause(kind="term", field_path="body", term="newtoken")]
    )
    assert after.version == first.version + 1
    assert [hit.doc_id for hit in after.items] == ["doc-a"]


def test_captured_query_version_survives_later_commit(tmp_path, index_fields) -> None:
    entered = threading.Event()
    release = threading.Event()
    armed = False

    def hook(version: int) -> None:
        if armed and version == 1:
            entered.set()
            assert release.wait(10)

    store = IndexStore(tmp_path / "index.db", index_fields, query_hook=hook)
    store.write_batch(inserts=[{"_id": "doc-a", "body": "old text"}], batch_id="initial")
    armed = True

    def reader() -> None:
        # The version is captured before this hook blocks. The writer below
        # completes a new version before the actual SELECT statements run.
        result = store.search(
            clauses=[SearchClause(kind="term", field_path="body", term="old")],
            version=None,
        )
        assert result.version == 1
        assert [hit.doc_id for hit in result.items] == ["doc-a"]
        assert result.items[0].document["body"] == "old text"

    thread = threading.Thread(target=reader)
    thread.start()
    assert entered.wait(5)
    store.write_batch(
        updates=[{"_id": "doc-a", "body": "newtoken text"}], batch_id="later-commit"
    )
    release.set()
    thread.join(10)
    assert not thread.is_alive()
    assert store.current_version() == 2
    assert store.search(
        clauses=[SearchClause(kind="term", field_path="body", term="old")], version=2
    ).total == 0


def test_retirement_never_removes_an_active_query(tmp_path, index_fields) -> None:
    hold_version: list[int | None] = [None]
    entered = threading.Event()
    release = threading.Event()

    def query_hook(version: int) -> None:
        if hold_version[0] == version:
            entered.set()
            assert release.wait(10)

    store = IndexStore(tmp_path / "index.db", index_fields, query_hook=query_hook)
    for number in range(1, 5):
        store.write_batch(
            updates=[],
            inserts=[{"_id": f"doc-{number}", "body": f"token{number} common"}],
            batch_id=f"batch-{number}",
        )
    assert store.current_version() == 4
    hold_version[0] = 1
    result_holder: dict[str, object] = {}

    def old_query() -> None:
        result_holder["response"] = store.search(
            clauses=[SearchClause(kind="term", field_path="body", term="common")],
            version=1,
        )

    thread = threading.Thread(target=old_query)
    thread.start()
    assert entered.wait(5)
    first_retirement = store.retire_versions(retain_versions=1)
    assert 1 not in first_retirement["retired_versions"]
    assert store.query_activity()["active_by_version"].get("1") == 1
    release.set()
    thread.join(10)
    assert len(result_holder["response"].items) == 1
    second_retirement = store.retire_versions(retain_versions=1)
    assert 1 in second_retirement["retired_versions"]
    with pytest.raises(BusinessError) as exc:
        store.search(
            clauses=[SearchClause(kind="term", field_path="body", term="common")],
            version=1,
        )
    assert exc.value.code == "E_VERSION_RETIRED"


def test_retired_version_is_rejected_after_reclaim(store: IndexStore) -> None:
    for number in range(1, 5):
        store.write_batch(
            inserts=[{"_id": f"d{number}", "body": "same"}], batch_id=f"b{number}"
        )
    retired = store.retire_versions(retain_versions=2)
    assert {0, 1, 2}.issubset(set(retired["retired_versions"]))
    with pytest.raises(BusinessError) as exc:
        store.get_postings("body", "same", 1)
    assert exc.value.code == "E_VERSION_RETIRED"
    # Current retained versions keep working.
    assert store.get_postings("body", "same", 4).entries
