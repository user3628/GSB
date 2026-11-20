#!/usr/bin/env python3
"""Concurrent editor B: open before A commits, then demonstrate stale save rejection."""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mvcc_engine.client import ApiError, MVCCClient  # noqa: E402


def write_json(path: Path, payload: dict[str, object]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def wait_for(predicate, timeout: float = 8.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise TimeoutError("editor barrier timed out")


def main() -> int:
    base_url = os.environ["MVCC_BASE_URL"]
    runtime_dir = Path(os.environ["MVCC_RUNTIME_DIR"])
    run_id = os.environ["MVCC_RUN_ID"]
    runtime_dir.mkdir(parents=True, exist_ok=True)
    client = MVCCClient(base_url)
    txn_id = f"editor-b-{run_id}"
    try:
        begin = client.begin("REPEATABLE_READ", txn_id)
        opened = client.open_edit(txn_id, "accounts", "A-1001")
        write_json(
            runtime_dir / f"editor-b-{run_id}.ready.json",
            {
                "editor": "B",
                "txn_id": txn_id,
                "row_version": opened["row_version"],
                "version_id": opened["version_id"],
                "snapshot_id": opened["snapshot_id"],
            },
        )
        wait_for(
            lambda: len(list(runtime_dir.glob(f"editor-*-{run_id}.ready.json"))) >= 2
        )
        wait_for(lambda: (runtime_dir / f"editor-a-{run_id}.committed.json").exists())
        stale_error: ApiError | None = None
        try:
            client.save(
                txn_id,
                opened["session_id"],
                opened["row_version"],
                {
                    "name": f"B stale overwrite {run_id}",
                    "balance": 9999,
                    "owner": "bob",
                    "revision_note": "editor B",
                },
            )
        except ApiError as error:
            stale_error = error
        client.rollback(txn_id, "editor_b_finished")
        if stale_error is None:
            raise RuntimeError("editor B save unexpectedly succeeded after editor A committed")
        if stale_error.code != "E_EDIT_STALE":
            raise RuntimeError(f"unexpected editor B error: {stale_error.as_dict()}")
        latest = stale_error.context.get("latest")
        print(
            json.dumps(
                {
                    "ok": True,
                    "editor": "B",
                    "txn_id": txn_id,
                    "snapshot_id": begin["snapshot_id"] or opened["snapshot_id"],
                    "opened": {
                        "row_version": opened["row_version"],
                        "version_id": opened["version_id"],
                        "snapshot_id": opened["snapshot_id"],
                        "lock": opened["lock"],
                    },
                    "save_result": "rejected",
                    "error": stale_error.as_dict(),
                    "latest_editable": {
                        "row_version": latest["row_version"] if latest else None,
                        "version_id": latest["version_id"] if latest else None,
                        "data": latest["data"] if latest else None,
                    },
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 0
    except BaseException as error:
        print(
            json.dumps(
                {"ok": False, "editor": "B", "error": repr(error)},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

