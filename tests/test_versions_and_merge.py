from __future__ import annotations

import pytest

from transcript_workbench.errors import WorkbenchError
from tests.conftest import first_word


def test_edit_versions_are_monotonic_and_stale_publish_is_rejected(boot_workspace):
    state = boot_workspace.load_state()
    word = first_word(state)
    edited = boot_workspace.edit(
        [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + "改",
            }
        ],
        expected_version=1,
    )
    assert edited["version"] == 2
    assert edited["changes"]["modified"][0]["source_version"] == 2
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.edit(
            [
                {
                    "op": "update_word",
                    "entry_id": word.entry_id,
                    "text": "late",
                }
            ],
            expected_version=1,
        )
    error = raised.value
    assert error.code == "E_STALE_VERSION"
    assert error.details["latest_version"] == 2
    assert error.details["replayable_edits"]
    assert boot_workspace.load_state().version == 2
    history = boot_workspace.version_history()
    assert [entry["version"] for entry in history] == [1, 2]
    assert history[1]["changes"]["modified"][0]["entry_id"] == word.entry_id
    diff = boot_workspace.diff_versions(1, 2)
    assert diff["counts"] == {"added": 0, "deleted": 0, "modified": 1}


def test_fork_then_last_wins_merge_keeps_audit_of_both_changes(boot_workspace):
    state = boot_workspace.load_state()
    word = first_word(state)
    left = boot_workspace.edit(
        [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + "左",
                "confidence": 0.911,
            }
        ],
        expected_version=1,
    )
    right = boot_workspace.fork_edit(
        [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + "右",
                "confidence": 0.877,
            }
        ],
        parent_version=1,
        expected_version=left["version"],
    )
    assert right["version"] == 3
    merged = boot_workspace.merge_versions(
        left_version=2,
        right_version=3,
        strategy="last-wins",
        expected_version=3,
    )
    assert merged["version"] == 4
    assert merged["merge"]["conflict_count"] >= 1
    conflict = merged["merge"]["conflicts"][0]
    assert conflict["left"]["text"].endswith("左")
    assert conflict["right"]["text"].endswith("右")
    assert conflict["strategy"] == "last-wins"
    merged_state = boot_workspace.load_state()
    chosen = merged_state.word_map()[word.entry_id]
    assert chosen.text.endswith("右")
    assert 2 in chosen.merged_from_versions and 3 in chosen.merged_from_versions
    assert merged_state.version == 4
    assert [item["version"] for item in boot_workspace.version_history()] == [1, 2, 3, 4]


def test_mark_conflict_marks_active_entry_and_never_silently_loses_candidate(
    boot_workspace,
):
    state = boot_workspace.load_state()
    word = first_word(state)
    boot_workspace.edit(
        [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + "A",
            }
        ],
        expected_version=1,
    )
    boot_workspace.fork_edit(
        [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + "B",
            }
        ],
        parent_version=1,
        expected_version=2,
    )
    merged = boot_workspace.merge_versions(
        left_version=2,
        right_version=3,
        strategy="mark-conflict",
        expected_version=3,
    )
    assert merged["merge"]["conflict_count"] == 1
    conflict = merged["merge"]["conflicts"][0]
    assert conflict["resolution"].startswith("conflict_marked")
    assert conflict["left"]["text"].endswith("A")
    assert conflict["right"]["text"].endswith("B")
    active = boot_workspace.load_state().word_map()[word.entry_id]
    assert active.conflict_id == conflict["conflict_id"]


def test_merge_strategy_validation(boot_workspace):
    with pytest.raises(WorkbenchError) as raised:
        boot_workspace.merge_versions(
            left_version=1,
            right_version=1,
            strategy="magic",
            expected_version=1,
        )
    assert raised.value.code == "E_INVALID_REQUEST"


def test_overlapping_new_entries_from_different_branches_conflict(boot_workspace):
    state = boot_workspace.load_state()
    first_two = [
        word
        for word in sorted(state.words, key=lambda item: item.start_ms)
        if word.chunk_id == "chunk-01"
    ][:2]
    start_ms = first_two[0].start_ms
    end_ms = first_two[-1].end_ms
    delete_ops = [
        {"op": "delete_word", "entry_id": first_two[0].entry_id},
        {"op": "delete_word", "entry_id": first_two[1].entry_id},
    ]
    left = boot_workspace.edit(
        delete_ops
        + [
            {
                "op": "add_word",
                "entry_id": "branch-a-overlap",
                "chunk_id": "chunk-01",
                "start_ms": start_ms,
                "end_ms": end_ms,
                "text": "甲区间",
                "language": "zh-CN",
                "confidence": 0.8,
            }
        ],
        expected_version=1,
    )
    right = boot_workspace.fork_edit(
        delete_ops
        + [
            {
                "op": "add_word",
                "entry_id": "branch-b-overlap",
                "chunk_id": "chunk-01",
                "start_ms": start_ms + 20,
                "end_ms": end_ms,
                "text": "乙区间",
                "language": "zh-CN",
                "confidence": 0.9,
            }
        ],
        parent_version=1,
        expected_version=left["version"],
    )
    merged = boot_workspace.merge_versions(
        left_version=left["version"],
        right_version=right["version"],
        strategy="last-wins",
        expected_version=right["version"],
    )
    overlap_conflicts = [
        conflict
        for conflict in merged["merge"]["conflicts"]
        if "overlap" in conflict["reason"]
    ]
    assert overlap_conflicts
    active_ids = {word.entry_id for word in boot_workspace.load_state().words}
    assert "branch-b-overlap" in active_ids
    assert "branch-a-overlap" not in active_ids
    assert overlap_conflicts[0]["left"]["entry_id"] == "branch-a-overlap"
    assert overlap_conflicts[0]["right"]["entry_id"] == "branch-b-overlap"
