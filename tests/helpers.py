"""Shared deterministic fixtures for the workbench test suite."""
from __future__ import annotations

import os
import shutil
import tempfile
from typing import Any, Dict, List

from ragwb import generator
from ragwb.io_utils import read_jsonl, write_jsonl
from ragwb.pipeline import Pipeline
from ragwb.yaml_lite import dump as yaml_dump


def make_workspace() -> str:
    base = tempfile.mkdtemp(prefix="ragwb-test-")
    data_dir = os.path.join(base, "data")
    generator.generate(data_dir)
    return data_dir


def fresh_run_dir(data_dir: str, name: str = "run") -> str:
    base = os.path.dirname(data_dir)
    run_dir = os.path.join(base, name)
    shutil.rmtree(run_dir, ignore_errors=True)
    return run_dir


def cleanup(*paths: str) -> None:
    for path in paths:
        shutil.rmtree(path, ignore_errors=True)


def load_config(data_dir: str) -> Dict[str, Any]:
    from ragwb.yaml_lite import load
    return load(os.path.join(data_dir, "config.yaml"))


def write_config(data_dir: str, config: Dict[str, Any]) -> None:
    yaml_dump(os.path.join(data_dir, "config.yaml"), config)


def copy_data(data_dir: str, suffix: str) -> str:
    target = data_dir + suffix
    shutil.rmtree(target, ignore_errors=True)
    shutil.copytree(data_dir, target)
    return target


def all_sessions(data_dir: str) -> Dict[str, List[Dict[str, Any]]]:
    import glob
    sessions: Dict[str, List[Dict[str, Any]]] = {}
    for path in glob.glob(os.path.join(data_dir, "sessions", "*.jsonl")):
        for row in read_jsonl(path):
            sessions.setdefault(row["session_id"], []).append(row)
    return sessions


def modify_session_question(data_dir: str, session_id: str,
                            turn_index: int, new_question: str) -> None:
    path = os.path.join(data_dir, "sessions", f"{session_id}.jsonl")
    rows = read_jsonl(path)
    for row in rows:
        if row["turn_index"] == turn_index:
            row["question"] = new_question
            row["coref_spans"] = []
    write_jsonl(path, rows)


def modify_kb_paragraph(data_dir: str, doc_id: str,
                        *, text: str | None = None,
                        version: int | None = None) -> None:
    import glob
    for path in glob.glob(os.path.join(data_dir, "kb", "*.jsonl")):
        rows = read_jsonl(path)
        changed = False
        for row in rows:
            if row["doc_id"] == doc_id:
                if text is not None:
                    row["text"] = text
                if version is not None:
                    row["version"] = version
                changed = True
        if changed:
            write_jsonl(path, rows)
            return
    raise KeyError(doc_id)


def full_run(data_dir: str, run_dir: str) -> Dict[str, Any]:
    return Pipeline(data_dir, run_dir).run()
