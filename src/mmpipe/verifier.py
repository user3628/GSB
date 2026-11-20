"""结果验证工具：检查点完整性、序号连续性、损坏恢复、确定性比对与错误码。"""

import json
import os

from . import STAGES
from .codec import canonical_dumps, digest_bytes
from .errors import PipeError
from .storage import Workspace


def verify_workspace(workspace: Workspace) -> dict:
    """结构与一致性校验，返回带 check 列表的报告。"""
    checks: list[dict] = []

    def add(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    checkpoints = workspace.load_valid_checkpoints()
    total_files, valid_count = workspace.corrupt_checkpoint_count()

    seqs = [cp["seq"] for cp in checkpoints]
    add("checkpoint_seq_starts_at_one", seqs[:1] == [1], f"seqs={seqs}")
    add("checkpoint_seq_continuous",
        seqs == list(range(1, len(seqs) + 1)),
        f"seqs={seqs}")
    add("checkpoint_seq_monotonic",
        all(a < b for a, b in zip(seqs, seqs[1:])), f"seqs={seqs}")
    add("corrupt_records_ignored",
        valid_count <= total_files,
        f"files={total_files}, valid={valid_count}")

    for cp in checkpoints:
        add(f"product_exists_seq{cp['seq']}",
            os.path.exists(workspace.path(cp["product_path"])),
            cp["product_path"])
        with open(workspace.path(cp["product_path"]), "rb") as handle:
            raw = handle.read()
        add(f"product_digest_seq{cp['seq']}",
            digest_bytes(raw) == cp["product_digest"],
            f"stage={cp['stage']}")
        add(f"product_canonical_seq{cp['seq']}",
            canonical_dumps(json.loads(raw)) == raw,
            f"stage={cp['stage']}")

    state = workspace.state()["current"]
    published_stages = [cp["stage"] for cp in checkpoints]
    for index, stage in enumerate(STAGES):
        cur = state.get(stage)
        if cur is None:
            continue
        if cur.get("status") == "published":
            add(f"stage_order_{stage}",
                all(prev in published_stages
                    for prev in STAGES[:index]),
                f"stage={stage}")
            add(f"state_matches_checkpoint_{stage}",
                any(cp["stage"] == stage and cp["seq"] == cur["seq"]
                    for cp in checkpoints), f"seq={cur['seq']}")

    aggregate = workspace.load_current_product("aggregation")
    if aggregate is not None:
        counts = aggregate["summary"]["conclusion_counts"]
        total = sum(counts.values())
        add("aggregate_counts_complete",
            total == aggregate["summary"]["sample_count"],
            f"counts={counts}")
        conflicts = aggregate["summary"]["conflict_sample_ids"]
        retained = [s["text_id"] for s in aggregate["samples"] if s["has_conflict"]]
        add("conflicts_not_dropped", conflicts == retained,
            f"conflicts={conflicts}")
        add("no_non_finite_values",
            _all_finite(aggregate), "aggregation product finite")

    add("invalidation_log_consistent",
        _invalidation_consistent(workspace),
        f"count={len(workspace.invalidations())}")
    return {"ok": all(c["ok"] for c in checks), "checks": checks,
            "head_seq": workspace.head_seq(),
            "checkpoint_files": total_files,
            "valid_checkpoints": valid_count}


def _all_finite(obj) -> bool:
    if isinstance(obj, float):
        return obj == obj and obj not in (float("inf"), float("-inf"))
    if isinstance(obj, dict):
        return all(_all_finite(v) for v in obj.values())
    if isinstance(obj, list):
        return all(_all_finite(v) for v in obj)
    return True


def _invalidation_consistent(workspace: Workspace) -> bool:
    invalidations = workspace.invalidations()
    if not invalidations:
        return True
    stages_seen: set[str] = set()
    for inv in invalidations:
        if inv["stage"] in stages_seen:
            return False
        stages_seen.add(inv["stage"])
        if not all(key in inv for key in
                   ("reason", "component", "old_fingerprint",
                    "new_fingerprint", "superseded_seq")):
            return False
    return True


def compare_products(path_a: str, path_b: str) -> dict:
    with open(path_a, "rb") as handle:
        raw_a = handle.read()
    with open(path_b, "rb") as handle:
        raw_b = handle.read()
    return {
        "ok": raw_a == raw_b,
        "byte_identical": raw_a == raw_b,
        "size_a": len(raw_a),
        "size_b": len(raw_b),
        "digest_a": digest_bytes(raw_a),
        "digest_b": digest_bytes(raw_b),
    }


def assert_error(exc: PipeError, expected_code: str, expected_stage: str,
                 sample_id: str | None = None) -> dict:
    ok = (exc.code == expected_code and exc.stage == expected_stage
          and (sample_id is None or exc.sample_id == sample_id))
    return {"ok": ok, "expected": expected_code, "actual": exc.to_dict()}
