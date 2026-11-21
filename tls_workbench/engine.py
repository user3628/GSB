"""Handshake orchestration engine: samples, single-writer advancement and
per-stage diagnostics.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass

from . import budget, chain as chain_mod, scoring, states, timeutil
from .certificates import load_chain
from .errors import Codes, WorkbenchError
from .persistence import Store

CRITICAL_FAILURE_CODES = frozenset(
    {
        Codes.SIGNATURE_INVALID,
        Codes.CERT_EXPIRED,
        Codes.CERT_NOT_YET_VALID,
        Codes.PATH_LOOP,
        Codes.ROOT_NOT_SELF_SIGNED,
    }
)


class SampleStore:
    """In-memory chain cache that preserves the last good snapshot on error."""

    def __init__(self, sample_root: str):
        self.sample_root = sample_root
        self._chains: dict[str, dict] = {}
        self._lock = threading.RLock()

    def load(self, chain_id: str) -> dict:
        with self._lock:
            if chain_id in self._chains:
                return self._chains[chain_id]
            chain_dir = os.path.join(self.sample_root, "certs", "der", chain_id)
            candidate = load_chain(chain_dir)
            self._chains[chain_id] = candidate
            return candidate

    def reload_preserving_snapshot(self, chain_id: str) -> dict:
        """Reload from disk; keep the previously loaded chain on failure."""
        with self._lock:
            previous = self._chains.get(chain_id)
            try:
                candidate = load_chain(
                    os.path.join(self.sample_root, "certs", "der", chain_id)
                )
            except WorkbenchError:
                if previous is None:
                    raise
                return previous
            self._chains[chain_id] = candidate
            return candidate

    def loaded_ids(self) -> list[str]:
        return sorted(self._chains)


@dataclass
class HandshakeConfig:
    continue_on_failure: bool = False
    downgrade_items: tuple[str, ...] = ()


class HandshakeEngine:
    def __init__(self, store: Store, samples: SampleStore):
        self.store = store
        self.samples = samples
        self._session_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        self._completed_results: dict[str, dict] = {}

    def _session_lock(self, session_id: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._session_locks.get(session_id)
            if lock is None:
                lock = threading.Lock()
                self._session_locks[session_id] = lock
            return lock

    def create_session(self, session_id: str, chain_id: str,
                       params: dict) -> dict:
        chain = self.samples.load(chain_id)
        self.store.create_session(session_id, chain["chain_id"], params)
        return self.store.get_session(session_id)

    def get_or_advance(self, session_id: str, request_params: dict | None = None,
                       config: HandshakeConfig | None = None) -> dict:
        """Idempotent single-writer advancement.

        Concurrent callers for the same session block on a per-session lock;
        the first caller runs the handshake and later callers reuse the
        persisted result. Conflicting handshake parameters on replay raise
        ``E_CONFLICTING_SESSION``.
        """
        lock = self._session_lock(session_id)
        with lock:
            session = self.store.get_session(session_id)
            if session is None:
                raise WorkbenchError(
                    Codes.SAMPLE_NOT_FOUND, f"unknown session {session_id}"
                )
            stored_params = json.loads(session["params_json"])
            if request_params is not None and stored_params != request_params:
                raise WorkbenchError(
                    Codes.CONFLICTING_SESSION,
                    f"session {session_id} was created with different parameters",
                    {
                        "stored_params": stored_params,
                        "request_params": request_params,
                    },
                )
            if session["status"] in {"COMPLETED", "COMPLETED_WITH_FAILURES", "FAILED", "ABORTED"}:
                if session_id in self._completed_results:
                    return self._completed_results[session_id]
                return self._assemble_from_persistence(session_id)
            result = self._run_handshake(session_id, stored_params, config or HandshakeConfig())
            self._completed_results[session_id] = result
            return result

    def _run_handshake(self, session_id: str, params: dict,
                       config: HandshakeConfig) -> dict:
        chain_id = self.store.get_session(session_id)["chain_id"]
        chain = self.samples.load(chain_id)
        instant = timeutil.build_verification_instant(
            nominal_wall_ms=params["verification_wall_ms"],
            clock_offset_ms=params["clock_offset_ms"],
            truncation_granularity_ms=params.get(
                "truncation_granularity_ms", timeutil.VERIFICATION_GRANULARITY_MS
            ),
        )
        failure_records: list[dict] = []
        current_stage = "INIT"
        path = None
        path_holder: dict = {}
        score_report = None
        budget_report = None
        budget_audit = None
        downgrade = budget.apply_downgrade(list(config.downgrade_items))
        estimates = self._build_estimates(params, downgrade["skipped_items"])

        for stage in states.STAGES:
            states.assert_transition(current_stage, stage)
            detail, stage_status = self._execute_stage(
                stage, chain, instant, params, estimates, failure_records,
                path_holder,
            )
            self.store.append_stage(session_id, stage, stage_status, detail)
            current_stage = stage
            if stage == states.CERTIFICATE:
                path = path_holder.get("path")
            if stage == states.CERT_VERIFY and path is not None and score_report is None:
                score_report = scoring.score_chain(chain, path)
            if stage == states.FINISHED:
                budget_report = detail.get("budget")
                budget_audit = detail.get("budget_audit")
            critical_failure = any(
                item["stage"] == stage and item["code"] in CRITICAL_FAILURE_CODES
                for item in failure_records
            )
            if critical_failure and not config.continue_on_failure:
                self.store.mark_status(session_id, "FAILED")
                break
        else:
            final_status = (
                "COMPLETED_WITH_FAILURES" if failure_records else "COMPLETED"
            )
            self.store.mark_status(session_id, final_status)

        diagnostic = self._assemble(
            session_id, chain, params, instant, failure_records,
            score_report, budget_report, budget_audit, downgrade,
        )
        session = self.store.get_session(session_id)
        self.store.save_diagnostic(session_id, session["version"], diagnostic)
        diagnostic["session"] = session
        budget_failure = next(
            (item for item in failure_records
             if item["code"] == Codes.BUDGET_EXCEEDED),
            None,
        )
        if budget_failure is not None:
            raise WorkbenchError(
                Codes.BUDGET_EXCEEDED,
                budget_failure["message"],
                {"diagnostic": _strip_internal(diagnostic),
                 **budget_failure["details"]},
            )
        return diagnostic

    def _execute_stage(self, stage: str, chain: dict,
                       instant: timeutil.VerificationInstant, params: dict,
                       estimates: list[budget.StageEstimate],
                       failure_records: list[dict],
                       path_holder: dict) -> tuple[dict, str]:
        detail: dict = {"stage": stage, "checks": []}
        status = "OK"
        try:
            if stage == states.CLIENT_HELLO:
                self._check_client_hello(params, detail)
            elif stage == states.SERVER_HELLO:
                self._check_server_hello(params, detail)
            elif stage == states.CERTIFICATE:
                try:
                    path = chain_mod.validate_chain(chain, instant)
                    path_holder["path"] = path
                except WorkbenchError:
                    # Record margin/path facts that were computed before the
                    # signature/validity failure, then re-raise for handling.
                    built = chain_mod.build_path(chain)
                    margin_items = []
                    for role in built:
                        info = chain["certificates"][role]
                        margin = timeutil.evaluate_validity(
                            info["not_before_ms"], info["not_after_ms"], instant
                        )
                        margin_items.append(
                            {"role": role, "serial_number": info["serial_number"],
                             **margin.__dict__}
                        )
                    detail["partial_margins"] = margin_items
                    path_holder["path"] = chain_mod.ChainPath(
                        chain_id=chain["chain_id"],
                        ordered_roles=tuple(built),
                        edges=(),
                        margins=tuple(margin_items),
                        signature_checks=(),
                        root_self_signed=False,
                    )
                    raise
                detail["path"] = _serialize_path(path)
                detail["checks"].append({"name": "chain_validation", "ok": True})
                invalid_margins = [
                    entry for entry in path.margins if not entry["is_valid"]
                ]
                if invalid_margins:
                    first_invalid = invalid_margins[0]
                    raise WorkbenchError(
                        first_invalid["status"],
                        f"certificate {first_invalid['role']} validity failed: "
                        f"{first_invalid['status']}",
                        {
                            "role": first_invalid["role"],
                            "margin_before_ms": first_invalid["margin_before_ms"],
                            "margin_after_ms": first_invalid["margin_after_ms"],
                            "effective_verify_ms": first_invalid[
                                "effective_verify_ms"
                            ],
                        },
                    )
            elif stage == states.CERT_VERIFY:
                self._check_cert_verify(chain, params, detail)
            elif stage == states.FINISHED:
                budget_report = budget.accumulate_budget(estimates)
                budget_audit = budget.naive_vs_compensated_audit(estimates)
                budget.adjudicate(
                    budget_report,
                    params["total_budget_limit_ms_fixed"],
                    params["per_stage_budget_limits_ms_fixed"],
                )
                detail["budget"] = budget_report
                detail["budget_audit"] = budget_audit
        except WorkbenchError as exc:
            status = "FAILED"
            record = {
                "stage": stage,
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            }
            failure_records.append(record)
            detail["failure"] = record
            if exc.code == Codes.BUDGET_EXCEEDED:
                detail["budget_audit"] = budget.naive_vs_compensated_audit(estimates)
        return detail, status

    def _check_client_hello(self, params: dict, detail: dict) -> None:
        if params["protocol_version"] not in {"TLSv1.2", "TLSv1.3"}:
            raise WorkbenchError(
                Codes.INVALID_TRANSITION,
                "unsupported protocol version",
                {"protocol_version": params["protocol_version"]},
            )
        if not params["sni"]:
            raise WorkbenchError(Codes.CHAIN_INVALID, "empty SNI")
        if params["session_ticket_length_ms"] < 0:
            raise WorkbenchError(Codes.CHAIN_INVALID, "negative ticket length")
        detail["checks"].append({"name": "client_hello_shape", "ok": True})

    def _check_server_hello(self, params: dict, detail: dict) -> None:
        if params["cipher_suite"] not in params.get("supported_cipher_suites", []):
            raise WorkbenchError(
                Codes.CHAIN_INVALID,
                "negotiated cipher suite is not in client offer",
                {"cipher_suite": params["cipher_suite"]},
            )
        detail["checks"].append({"name": "cipher_negotiation", "ok": True})

    def _check_cert_verify(self, chain: dict, params: dict, detail: dict) -> None:
        leaf = chain["certificates"]["leaf"]
        if leaf["signature_algorithm"] not in scoring.SIGNATURE_WEIGHTS:
            raise WorkbenchError(
                Codes.CHAIN_INVALID,
                "unsupported leaf signature algorithm",
                {"signature_algorithm": leaf["signature_algorithm"]},
            )
        detail["checks"].append({"name": "cert_verify_signature_support", "ok": True})

    def _build_estimates(self, params: dict, skipped_items: list[str]):
        # Downgrade only suppresses non-critical diagnostic outputs; every
        # handshake stage still consumes budget, including FINISHED.
        table = params["stage_costs_ms_fixed"]
        overhead = params["chain_overhead_ms_fixed"]
        return [
            budget.stage_estimate(
                stage=stage,
                base_ms_fixed=table[stage],
                chain_overhead_ms_fixed=overhead[stage],
                skipped=False,
            )
            for stage in states.STAGES
        ]

    def _assemble(self, session_id, chain, params, instant, failure_records,
                  score_report, budget_report, budget_audit, downgrade) -> dict:
        session = self.store.get_session(session_id)
        return {
            "session_id": session_id,
            "chain_id": chain["chain_id"],
            "verification_instant": instant.__dict__,
            "stage_states": self.store.get_stages(session_id),
            "failures": failure_records,
            "score_report": _strip_internal(score_report),
            "budget_report": budget_report,
            "numeric_error_audit": budget_audit,
            "downgrade_report": downgrade,
            "parameters": params,
            "session_status": session["status"],
            "version": session["version"],
        }

    def _assemble_from_persistence(self, session_id: str) -> dict:
        diagnostic = self.store.get_diagnostic(session_id)
        if diagnostic is None:
            raise WorkbenchError(
                Codes.RECOVERY_ABORTED,
                f"session {session_id} has no persisted diagnostic",
            )
        result = dict(diagnostic["payload"])
        result["session"] = self.store.get_session(session_id)
        return result


def _serialize_path(path) -> dict:
    return {
        "chain_id": path.chain_id,
        "ordered_roles": list(path.ordered_roles),
        "edges": list(path.edges),
        "margins": [dict(item) for item in path.margins],
        "signature_checks": [dict(item) for item in path.signature_checks],
        "root_self_signed": path.root_self_signed,
    }


def _strip_internal(report: dict | None) -> dict | None:
    if report is None:
        return None
    clean = json.loads(json.dumps(report, default=str))
    return clean
