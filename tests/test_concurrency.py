from __future__ import annotations

import multiprocessing as mp
from pathlib import Path

from transcript_workbench.errors import WorkbenchError
from transcript_workbench.workspace import Workspace
from tests.conftest import first_word


def _edit_worker(root: str, expected_version: int, suffix: str, start, queue) -> None:
    try:
        workspace = Workspace(root)
        state = workspace.load_state()
        word = first_word(state)
        operations = [
            {
                "op": "update_word",
                "entry_id": word.entry_id,
                "text": word.text + suffix,
            }
        ]
        start.wait()
        result = workspace.edit(operations, expected_version=expected_version)
        queue.put(
            {
                "ok": True,
                "version": result["version"],
                "operations": operations,
            }
        )
    except WorkbenchError as exc:
        queue.put(
            {
                "ok": False,
                "code": exc.code,
                "details": exc.details,
            }
        )
    except BaseException as exc:  # pragma: no cover - diagnostics path
        queue.put({"ok": False, "code": "PYTHON_ERROR", "error": repr(exc)})


def test_concurrent_publishers_only_allow_contiguous_version(boot_workspace):
    context = mp.get_context("spawn")
    start = context.Event()
    queue = context.Queue()
    processes = [
        context.Process(
            target=_edit_worker,
            args=(str(boot_workspace.root), 1, suffix, start, queue),
        )
        for suffix in ("甲", "乙")
    ]
    for process in processes:
        process.start()
    start.set()
    results = [queue.get(timeout=20) for _ in processes]
    for process in processes:
        process.join(timeout=20)
        assert process.exitcode == 0
    successes = [result for result in results if result["ok"]]
    stale = [result for result in results if not result["ok"]]
    assert len(successes) == 1
    assert len(stale) == 1
    assert successes[0]["version"] == 2
    assert stale[0]["code"] == "E_STALE_VERSION"
    assert stale[0]["details"]["latest_version"] == 2
    assert stale[0]["details"]["replayable_edits"]
    history = boot_workspace.version_history()
    assert [entry["version"] for entry in history] == [1, 2]
    assert not (boot_workspace.versions_dir / "v00000003.json").exists()

    replay = boot_workspace.edit(
        stale[0]["details"]["replayable_edits"], expected_version=2
    )
    assert replay["version"] == 3
    assert [entry["version"] for entry in boot_workspace.version_history()] == [1, 2, 3]
