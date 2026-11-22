"""End-to-end deterministic pipeline orchestration and crash recovery.

The runner keeps the run directory (checkpoints, answers, diagnostics) separate
from the imported data directory.  Every turn is an indivisible work unit:
a stop request is honored only between turns, so each stage sequence stays
contiguous.  On recovery the budget ledger is rebuilt from checkpoint records,
which makes a resumed run byte-identical to a clean full run.
"""
from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from . import alignment as alignment_mod
from . import answer as answer_mod
from . import query as query_mod
from . import retrieval as retrieval_mod
from .checkpoint import CheckpointStore, make_record
from .errors import E_RETRIEVAL_EMPTY, WorkbenchError
from .io_utils import (digest_json, ensure_dir, read_json, read_jsonl,
                       write_json)
from .planner import BudgetLedger, plan_turn
from .config import load_config as _load_validated_config


@dataclass
class StopController:
    """File-backed stop flag so HTTP/CLI requests cross process boundaries."""

    run_dir: str

    @property
    def path(self) -> str:
        return os.path.join(self.run_dir, "STOP_REQUESTED")

    def request_stop(self) -> None:
        ensure_dir(self.run_dir)
        write_json(self.path, {"stop_requested": True})

    def stop_requested(self) -> bool:
        return os.path.exists(self.path)

    def clear(self) -> None:
        if os.path.exists(self.path):
            os.unlink(self.path)


def load_workspace(data_dir: str) -> Dict[str, Any]:
    config_path = os.path.join(data_dir, "config.yaml")
    config = _load_validated_config(config_path)
    sessions: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(glob.glob(os.path.join(data_dir, "sessions", "*.jsonl"))):
        for row in read_jsonl(path):
            sessions.setdefault(row["session_id"], []).append(row)
    docs: List[Dict[str, Any]] = []
    for path in sorted(glob.glob(os.path.join(data_dir, "kb", "*.jsonl"))):
        docs.extend(read_jsonl(path))
    docs.sort(key=lambda doc: doc["doc_id"])
    return {"config": config, "sessions": sessions, "docs": docs,
            "config_path": config_path}


def _input_fingerprints(turn: Dict[str, Any], docs: List[Dict[str, Any]],
                        config: Dict[str, Any], prior_focus: Dict[str, Any]
                        ) -> Dict[str, str]:
    query_input = digest_json({
        "question": turn["question"],
        "coref_spans": turn.get("coref_spans", []),
        "focus_hint": turn.get("focus_hint"),
        "lang": turn.get("lang"),
        "prior_focus": prior_focus,
    })
    retrieval_input = digest_json({
        "docs": docs,
        "retrieval_config": config["retrieval"],
        "scale_tiers": config["scale_tiers"],
        "budget": config["budget"],
    })
    alignment_input = digest_json({
        "alignment_tolerance": config["retrieval"]["alignment_tolerance"],
        "channel_prior": config["retrieval"]["channel_prior"],
        "weights": config["retrieval"]["weights"],
    })
    answer_input = digest_json({"answer_config": config["answer"]})
    return {
        "query_rewrite": query_input,
        "retrieval": digest_json({"query_input": query_input,
                                  "doc_input": retrieval_input}),
        "alignment": digest_json({"retrieval_input": retrieval_input,
                                  "alignment_config": alignment_input}),
        "answer_assembly": digest_json({"alignment_input": alignment_input,
                                        "answer_config": answer_input}),
    }


class Pipeline:
    def __init__(self, data_dir: str, run_dir: str) -> None:
        self.data_dir = data_dir
        self.run_dir = run_dir
        ensure_dir(run_dir)
        ensure_dir(os.path.join(run_dir, "answers"))
        ensure_dir(os.path.join(run_dir, "diagnostics"))
        workspace = load_workspace(data_dir)
        self.config = workspace["config"]
        self.sessions = workspace["sessions"]
        self.docs = workspace["docs"]
        self.config_fingerprint = digest_json({
            "config_version": self.config.get("config_version"),
            "retrieval": self.config["retrieval"],
            "answer": self.config["answer"],
            "scale_tiers": self.config["scale_tiers"],
            "budget": self.config["budget"],
        })
        self.store = CheckpointStore(run_dir)
        self.stop = StopController(run_dir)
        manifest_path = os.path.join(data_dir, "manifest.json")
        if os.path.exists(manifest_path):
            manifest = read_json(manifest_path)
            self.session_order = [
                session_id for session_id in manifest.get("session_order", [])
                if session_id in self.sessions
            ]
        else:
            self.session_order = sorted(self.sessions)

    def _next_sequence(self) -> int:
        value = self._sequence_counter
        self._sequence_counter += 1
        return value

    def _answer_path(self, session_id: str) -> str:
        return os.path.join(self.run_dir, "answers", f"{session_id}.json")

    def _diagnostics_path(self, session_id: str) -> str:
        return os.path.join(self.run_dir, "diagnostics",
                            f"{session_id}.diagnostics.json")

    def _write_session_outputs(self, session_id: str,
                               diagnostics: Dict[str, Any]) -> None:
        write_json(self._diagnostics_path(session_id), diagnostics,
                   pretty=True)
        answers = [turn_record.get("answer")
                   for turn_record in diagnostics["turns"]
                   if turn_record.get("answer")]
        write_json(self._answer_path(session_id),
                   {"session_id": session_id, "answers": answers},
                   pretty=True)

    def _run_turn(self, session_id: str, turn: Dict[str, Any],
                  focus: query_mod.Focus, ledger: BudgetLedger,
                  diagnostics: Dict[str, Any]) -> query_mod.Focus:
        inputs = _input_fingerprints(turn, self.docs, self.config,
                                     focus.to_dict())
        turn_diag: Dict[str, Any] = {
            "turn_index": turn["turn_index"],
            "input_fingerprints": inputs,
            "stage_fingerprints": {},
            "status": "ok",
        }

        query_product = query_mod.rewrite_turn(turn, focus)
        self.store.publish(
            make_record("query_rewrite", session_id, turn["turn_index"],
                        inputs["query_rewrite"], query_product,
                        self._next_sequence()),
            query_product, config_fingerprint=self.config_fingerprint)
        turn_diag["stage_fingerprints"]["query_rewrite"] = \
            query_product["fingerprint"]

        try:
            plan = plan_turn(
                self.config,
                paragraph_count=len(self.docs),
                turn_count=len(self.sessions[session_id]),
                remaining_budget=ledger.remaining,
                focus_present=bool(query_product["focus_after"].get("entity")),
            )
            requested = plan["estimated_cost"]
            budget_charge = ledger.charge(
                session_id, turn["turn_index"], requested)
            retrieval_product = retrieval_mod.retrieve(
                query_product, self.docs, self.config, plan, budget_charge)
            self.store.publish(
                make_record("retrieval", session_id, turn["turn_index"],
                            inputs["retrieval"], retrieval_product,
                            self._next_sequence()),
                retrieval_product,
                config_fingerprint=self.config_fingerprint)
            turn_diag["stage_fingerprints"]["retrieval"] = \
                retrieval_product["fingerprint"]
            turn_diag["strategy"] = plan

            alignment_product = alignment_mod.align(
                retrieval_product, self.config)
            self.store.publish(
                make_record("alignment", session_id, turn["turn_index"],
                            inputs["alignment"], alignment_product,
                            self._next_sequence()),
                alignment_product,
                config_fingerprint=self.config_fingerprint)
            turn_diag["stage_fingerprints"]["alignment"] = \
                alignment_product["fingerprint"]
            turn_diag["clusters"] = alignment_product["clusters"]
            turn_diag["demoted"] = [
                {
                    "doc_id": entry["doc_id"],
                    "disposition": entry["disposition"],
                    "reason": entry["reason"],
                }
                for entry in alignment_product["demoted"]
            ]

            answer_product = answer_mod.assemble(
                alignment_product, retrieval_product, self.config)
            self.store.publish(
                make_record("answer_assembly", session_id,
                            turn["turn_index"], inputs["answer_assembly"],
                            answer_product, self._next_sequence()),
                answer_product,
                config_fingerprint=self.config_fingerprint)
            turn_diag["stage_fingerprints"]["answer_assembly"] = \
                answer_product["fingerprint"]
            turn_diag["answer"] = answer_product
            turn_diag["evidence_chain"] = answer_product["evidence_chain"]
        except WorkbenchError as exc:
            turn_diag["status"] = "error"
            turn_diag["error"] = exc.to_dict()
            turn_diag["answer"] = None
            diagnostics["turns"].append(turn_diag)
            diagnostics["status"] = "error"
            self._write_session_outputs(session_id, diagnostics)
            raise

        diagnostics["turns"].append(turn_diag)
        if turn.get("focus_hint"):
            return query_mod.Focus(
                entity=turn["focus_hint"], doc_id=focus.doc_id)
        return query_mod.Focus(
            entity=query_product["focus_after"]["entity"],
            doc_id=focus.doc_id)

    def run(self, *, session_ids: Optional[List[str]] = None,
            max_turns: Optional[int] = None) -> Dict[str, Any]:
        """Execute the full pipeline, honoring stop requests at turn bounds."""
        existing = self.store.all_records()
        self._sequence_counter = len(existing)
        ledger = BudgetLedger.from_checkpoints(
            int(self.config["budget"]["global_evidence_evaluations"]),
            existing)
        selected = session_ids or self.session_order
        summary: Dict[str, Any] = {
            "data_dir": self.data_dir, "run_dir": self.run_dir,
            "config_fingerprint": self.config_fingerprint,
            "sessions": {}, "stopped": False,
        }

        for session_id in selected:
            turns = sorted(self.sessions[session_id],
                           key=lambda row: row["turn_index"])
            state = self.store.load_state(session_id)
            start_turn = state["next_turn"]
            if state["next_stage_seq"] != 0:
                # A turn was interrupted mid-flight; it is re-executed from
                # the beginning because no checkpoint beyond the last safe
                # boundary exists.
                start_turn = state["next_turn"]
            diagnostics = read_json(self._diagnostics_path(session_id)) \
                if os.path.exists(self._diagnostics_path(session_id)) else \
                {"session_id": session_id, "status": "ok", "turns": []}
            focus = query_mod.Focus()
            if start_turn > 0:
                seed_turn = turns[start_turn - 1]
                if seed_turn.get("focus_hint"):
                    focus = query_mod.Focus(
                        entity=seed_turn["focus_hint"])
                else:
                    prior_focus = None
                    for prior_turn in reversed(turns[:start_turn]):
                        if prior_turn.get("focus_hint"):
                            prior_focus = prior_turn["focus_hint"]
                            break
                    focus = query_mod.Focus(entity=prior_focus)

            session_error = None
            for turn in turns[start_turn:]:
                if max_turns is not None and \
                        turn["turn_index"] >= start_turn + max_turns:
                    break
                try:
                    focus = self._run_turn(
                        session_id, turn, focus, ledger, diagnostics)
                except WorkbenchError as exc:
                    session_error = exc.code
                    break
                # Honor a stop request only after the indivisible turn unit
                # has been fully checkpointed.
                if self.stop.stop_requested():
                    summary["stopped"] = True
                    self._write_session_outputs(session_id, diagnostics)
                    summary["sessions"][session_id] = {
                        "status": "stopped",
                        "completed_turns": self.store.load_state(
                            session_id)["next_turn"]}
                    self._write_summary(summary, ledger)
                    return summary

            if session_error is None:
                diagnostics["status"] = "ok"
            self._write_session_outputs(session_id, diagnostics)
            summary["sessions"][session_id] = {
                "status": diagnostics["status"],
                "completed_turns": self.store.load_state(
                    session_id)["next_turn"],
                "error": session_error,
            }
            if session_error == E_RETRIEVAL_EMPTY:
                # Budget-driven empty retrieval ends this session only; the
                # remaining sessions continue so a fixed digest still covers
                # every importable shard.
                continue
            if session_error is not None:
                break

        self._write_summary(summary, ledger)
        return summary

    def _write_summary(self, summary: Dict[str, Any],
                       ledger: BudgetLedger) -> None:
        summary["budget"] = ledger.to_dict()
        answer_digests = {}
        for session_id in sorted(summary["sessions"]):
            path = self._answer_path(session_id)
            if os.path.exists(path):
                answer_digests[session_id] = digest_json(read_json(path))
        summary["answer_digests"] = answer_digests
        summary["run_digest"] = digest_json({
            "config_fingerprint": summary["config_fingerprint"],
            "sessions": summary["sessions"],
            "answer_digests": answer_digests,
            "budget_consumed": ledger.consumed,
        })
        write_json(os.path.join(self.run_dir, "summary.json"), summary,
                   pretty=True)
        return summary

    def run_digest(self) -> Optional[str]:
        path = os.path.join(self.run_dir, "summary.json")
        if not os.path.exists(path):
            return None
        return read_json(path).get("run_digest")
