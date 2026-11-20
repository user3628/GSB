from __future__ import annotations

import pytest

from inverted_index.errors import BusinessError
from inverted_index.storage import IndexStore, SearchClause


def search_terms(store: IndexStore, field: str, term: str, version: int | None = None):
    return store.search(
        clauses=[SearchClause(kind="term", field_path=field, term=term)],
        version=version,
    )


def test_incremental_versions_are_isolated(loaded_store) -> None:
    store, first, second = loaded_store
    old_quantum = search_terms(store, "title", "quantum", first)
    old_local = search_terms(store, "tags", "local", first)
    old_phrase = store.search(
        clauses=[
            SearchClause(
                kind="phrase",
                field_path="body",
                terms=("local", "search", "index"),
            )
        ],
        version=first,
    )
    assert old_quantum.total == 0
    assert old_phrase.total > 0

    # Version 2 must not rewrite the answer for an explicit version 1 query.
    assert search_terms(store, "title", "quantum", first).as_dict() == old_quantum.as_dict()
    assert search_terms(store, "tags", "local", first).as_dict() == old_local.as_dict()
    new_quantum = search_terms(store, "title", "quantum", second)
    assert [hit.doc_id for hit in new_quantum.items] == ["doc-002", "doc-013"]
    assert search_terms(store, "title", "quantum", None).version == second

    # Delete is visible only at the new version.
    assert store.get_document("doc-004", first) is not None
    assert store.get_document("doc-004", second) is None


def test_update_replaces_term_posting_instead_of_accumulating(loaded_store) -> None:
    store, first, second = loaded_store
    old_gazette = store.get_postings("title", "gazette", first)
    assert any(entry.doc_id == "doc-002" for entry in old_gazette.entries)
    new_gazette = store.get_postings("title", "gazette", second)
    assert all(entry.doc_id != "doc-002" for entry in new_gazette.entries)

    quantum = store.get_postings("title", "quantum", second)
    doc_two = [entry for entry in quantum.entries if entry.doc_id == "doc-002"]
    assert len(doc_two) == 1
    assert doc_two[0].term_frequency == 1
    assert doc_two[0].positions == tuple(sorted(set(doc_two[0].positions)))

    document = store.get_document("doc-002", second)
    assert document is not None
    store.write_batch(updates=[document], batch_id="same-document-again")
    repeated = store.get_postings("title", "quantum", None)
    doc_two_again = [entry for entry in repeated.entries if entry.doc_id == "doc-002"]
    assert len(doc_two_again) == 1
    assert doc_two_again[0].term_frequency == 1


def test_failed_batch_is_atomic_and_creates_no_version(store: IndexStore) -> None:
    first = store.write_batch(inserts=[{"_id": "a", "body": "alpha"}], batch_id="first")
    with pytest.raises(BusinessError) as exc:
        store.write_batch(
            inserts=[{"_id": "b", "body": "beta"}, {"_id": "a", "body": "duplicate"}],
            batch_id="bad",
        )
    assert exc.value.code == "E_DUPLICATE_ID"
    assert store.current_version() == first.version
    assert search_terms(store, "body", "beta").total == 0
    assert store.get_document("b") is None
    assert store.list_versions()[-1]["version"] == first.version


def test_unknown_and_bad_paths_are_distinct(store: IndexStore) -> None:
    store.write_batch(inserts=[{"_id": "a", "body": "alpha"}], batch_id="one")
    with pytest.raises(BusinessError) as unknown:
        store.get_postings("not_registered", "alpha")
    assert unknown.value.code == "E_UNKNOWN_PATH"
    with pytest.raises(BusinessError) as bad:
        store.get_postings("body[0]", "alpha")
    assert bad.value.code == "E_BAD_PATH"


def test_pagination_is_stable_without_duplicates(loaded_store) -> None:
    store, _, second = loaded_store
    full = search_terms(store, "tags", "local", second)
    combined = []
    for offset in range(0, full.total, 2):
        page = store.search(
            clauses=[SearchClause(kind="term", field_path="tags", term="local")],
            version=second,
            limit=2,
            offset=offset,
        )
        combined.extend(hit.doc_id for hit in page.items)
    assert combined == [hit.doc_id for hit in full.items]
    assert len(combined) == len(set(combined))
    assert combined == sorted(combined)


def test_phrase_and_or_share_one_version(loaded_store) -> None:
    store, first, second = loaded_store
    phrase = SearchClause(kind="phrase", field_path="body", terms=("local", "search", "index"))
    quantum = SearchClause(kind="term", field_path="title", term="quantum")
    old_or = store.search(clauses=[phrase, quantum], operator="OR", version=first)
    new_or = store.search(clauses=[phrase, quantum], operator="OR", version=second)
    assert old_or.version == first and new_or.version == second
    assert set(hit.doc_id for hit in new_or.items) - set(hit.doc_id for hit in old_or.items) == {
        "doc-013",
    }
    conjunction = store.search(clauses=[phrase, quantum], operator="AND", version=second)
    assert "doc-002" in {hit.doc_id for hit in conjunction.items}
