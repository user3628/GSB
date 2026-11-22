"""Version-change detection and downstream invalidation propagation.

Each turn stores per-stage input and product fingerprints.  When input data or
configuration changes, the current input fingerprints are recomputed and
compared against the checkpointed ones; the earliest stage whose input
fingerprint changed (or whose upstream product changed) is marked invalid, and
every downstream stage of that turn is invalidated with it.  Unaffected
upstream stages stay reusable.  An incremental rebuild can then be compared
byte-for-byte against a clean full build, raising
``E_INCREMENTAL_MISMATCH`` on any divergence.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .checkpoint import STAGE_ORDER
from .errors import E_INCREMENTAL_MISMATCH, WorkbenchError
from .io_utils import digest_json, read_json, write_json
from .pipeline import _input_fingerprints

STAGE_TITLES = {
    "query_rewrite": "query rewrite and coreference resolution",
    "retrieval": "retrieval and scoring",
    "alignment": "alignment and conflict adjudication",
    "answer_assembly": "answer assembly and evidence chain",
}


@dataclass
class InvalidationReport:
    session_id: str
    turn_index: int
    earliest_stage: Optional[str]
    invalid_stages: List[str] = field(default_factory=list)
    reasons: Dict[str, str] = field(default_factory=dict)
    reused_stages: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "turn_index": self.turn_index,
            "earliest_stage": self.earliest_stage,
            "invalid_stages": self.invalid_stages,
            "reused_stages": self.reused_stages,
            "reasons": self.reasons,
        }


def detect_change(pipeline, session_id: str,
                  turn: Dict[str, Any], prior_focus: Dict[str, Any]
                  ) -> InvalidationReport:
    """Compare current input fingerprints with stored checkpoint records."""
    store = pipeline.store
    state = store.load_state(session_id)
    fingerprints = _input_fingerprints(
        turn, pipeline.docs, pipeline.config, prior_focus)
    stored = [
        record for record in state["records"]
        if record["turn_index"] == turn["turn_index"]
    ]
    stored_by_stage = {record["stage"]: record for record in stored}

    report = InvalidationReport(
        session_id=session_id, turn_index=turn["turn_index"],
        earliest_stage=None)
    upstream_changed = False
    for stage in STAGE_ORDER:
        reason = ""
        if upstream_changed:
            reason = "upstream_stage_invalidated"
        elif stage not in stored_by_stage:
            reason = "stage_checkpoint_missing"
        else:
            record = stored_by_stage[stage]
            if record.get("config_fingerprint") != \
                    pipeline.config_fingerprint:
                reason = "configuration_version_changed"
            elif record.get("input_fingerprint") != fingerprints[stage]:
                reason = "input_content_fingerprint_changed"
        if reason:
            report.earliest_stage = report.earliest_stage or stage
            report.invalid_stages.append(stage)
            report.reasons[stage] = reason
            upstream_changed = True
        elif stage in stored_by_stage:
            report.reused_stages.append(stage)
    return report


def rebuild_incremental(pipeline, data_dir_baseline: str,
                        run_dir_clean: str,
                        changed_data_dir: str,
                        changed_run_dir: str) -> Dict[str, Any]:
    """Run an incremental rebuild and verify it against a clean reference.

    Both directories must contain the same logical sessions.  The clean run
    digest and per-answer digests are the reference; any difference raises
    ``E_INCREMENTAL_MISMATCH``.
    """
    from .pipeline import Pipeline

    clean_summary_path = os.path.join(run_dir_clean, "summary.json")
    if not os.path.exists(clean_summary_path):
        raise WorkbenchError(
            E_INCREMENTAL_MISMATCH,
            "clean reference run summary does not exist",
            stage="incremental")
    clean_summary = read_json(clean_summary_path)

    changed = Pipeline(changed_data_dir, changed_run_dir)
    changed_summary = changed.run()

    mismatches: List[str] = []
    if changed_summary.get("run_digest") != clean_summary.get("run_digest"):
        mismatches.append("run_digest")
    for session_id, digest in clean_summary.get(
            "answer_digests", {}).items():
        changed_digest = changed_summary.get("answer_digests", {}) \
            .get(session_id)
        if changed_digest != digest:
            mismatches.append(f"answer_digest:{session_id}")
    if mismatches:
        raise WorkbenchError(
            E_INCREMENTAL_MISMATCH,
            "incremental rebuild differs byte-for-byte from clean run",
            stage="incremental",
            context={"mismatches": mismatches})
    return {
        "status": "match",
        "run_digest": changed_summary["run_digest"],
        "compared_sessions": sorted(clean_summary.get(
            "answer_digests", {}).keys()),
    }


def write_report(run_dir: str, reports: List[InvalidationReport]) -> str:
    path = os.path.join(run_dir, "invalidation.json")
    payload = {
        "reports": [report.to_dict() for report in reports],
        "fingerprint": digest_json([report.to_dict() for report in reports]),
    }
    write_json(path, payload, pretty=True)
    return path
