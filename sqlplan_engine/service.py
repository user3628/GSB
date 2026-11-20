"""Application service: parse, plan, normalize, execute, and persist atomically."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from .business import (
    BadRequestError,
    EngineError,
    InvalidAggregateError,
    NotFoundError,
    PlanTooLargeError,
    SemanticError,
    ok_envelope,
)
from .executor import execute_plan
from .expressions import columns_in, walk
from .parser import parse_sql
from .plan import (
    format_plan_text,
    iter_nodes,
    node_count,
    node_summary,
    serialize_plan,
)
from .planner import build_logical_plan
from .rewriter import normalize_plan
from .sample import Catalog, DEFAULT_RUNTIME_DB, DEFAULT_SAMPLE_DB, load_manifest
from .store import PlanStore, statement_key_for


INJECTION_KINDS = {
    "malformed_sql",
    "plan_too_large",
    "invalid_aggregate",
    "rewrite_aborted",
    "crash_after_intent",
}


class PlanService:
    def __init__(
        self,
        *,
        sample_db: str | Path = DEFAULT_SAMPLE_DB,
        store_db: str | Path = DEFAULT_RUNTIME_DB,
        max_nodes: int = 64,
        max_rewrite_steps: int = 500,
        auto_recover: bool = True,
        allow_crash_exit: bool = True,
    ) -> None:
        self.sample_db = Path(sample_db)
        self.store_db = Path(store_db)
        self.max_nodes = int(max_nodes)
        self.max_rewrite_steps = int(max_rewrite_steps)
        self.allow_crash_exit = allow_crash_exit
        self.catalog = Catalog(self.sample_db)
        self.store = PlanStore(self.store_db)
        self.recovery_report = self.store.recover_pending_intents() if auto_recover else None

    def health(self) -> dict[str, Any]:
        return ok_envelope(
            service="local-sql-logical-plan-engine",
            sample_db=str(self.sample_db),
            store_db=str(self.store_db),
            max_nodes=self.max_nodes,
            max_rewrite_steps=self.max_rewrite_steps,
            store=self.store.status(),
        )

    def samples(self) -> dict[str, Any]:
        return ok_envelope(statements=load_manifest(), sample_db=str(self.sample_db))

    def recovery(self) -> dict[str, Any]:
        report = self.store.latest_recovery_report()
        if report is None:
            raise NotFoundError("recovery_report", "latest")
        return ok_envelope(recovery_report=report)

    def get_request(self, request_id: str) -> dict[str, Any]:
        return ok_envelope(**self.store.require_request_result(request_id))

    def get_intent(self, intent_id: str) -> dict[str, Any]:
        return ok_envelope(intent=self.store.require_intent(intent_id))

    def intents_for_request(self, request_id: str) -> dict[str, Any]:
        return ok_envelope(request_id=request_id, intents=self.store.intents_for_request(request_id))

    def safe_exit_summary(self) -> dict[str, Any]:
        return ok_envelope(exit_summary=self.store.safe_exit_summary())

    def submit(
        self,
        sql: str,
        *,
        request_id: str | None = None,
        injection: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(sql, str):
            raise BadRequestError("sql must be a string")
        injection = _validate_injection(injection or {})
        request_id = request_id or _default_request_id(sql, injection)
        _validate_request_id(request_id)
        statement_key = statement_key_for(sql)

        with self.store.writer_lock:
            existing = self.store.ensure_request_compatible(request_id, sql)
            if existing is not None:
                result = self.store.require_request_result(request_id)
                return _with_response_metadata(
                    result,
                    request_id=request_id,
                    reused=True,
                    reuse_kind="request_id",
                    injection=injection,
                )

            injection_kind = str(injection.get("kind", ""))
            if not injection_kind:
                statement = self.store.find_statement(statement_key)
                if statement is not None:
                    intent_id = self.store.begin_intent(
                        request_id, sql, statement_key, injection=None
                    )
                    stored = self.store.register_reused_request(
                        intent_id=intent_id,
                        request_id=request_id,
                        sql=sql,
                        statement_key=statement_key,
                        version_id=int(statement["version_id"]),
                        result=json.loads(statement["result_json"]),
                    )
                    response = _with_response_metadata(
                        stored,
                        request_id=request_id,
                        reused=True,
                        reuse_kind="statement",
                        injection=injection,
                    )
                    return response

            intent_id = self.store.begin_intent(request_id, sql, statement_key, injection)
            if injection_kind == "crash_after_intent":
                if not self.allow_crash_exit:
                    self.store.abort_intent(
                        intent_id,
                        "E_BAD_REQUEST",
                        "crash injection is disabled in this process",
                        {"injection": injection},
                    )
                    raise BadRequestError("crash injection is disabled", injection=injection)
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(70)

            try:
                response = self._build_and_commit(
                    request_id=request_id,
                    sql=sql,
                    statement_key=statement_key,
                    intent_id=intent_id,
                    injection=injection,
                )
                return _with_response_metadata(
                    response,
                    request_id=request_id,
                    reused=False,
                    reuse_kind=None,
                    injection=injection,
                )
            except EngineError as exc:
                self.store.abort_intent(
                    intent_id, exc.business_code, exc.message, exc.context
                )
                raise
            except Exception as exc:
                context = {"exception_type": type(exc).__name__, "detail": str(exc)[:500]}
                self.store.abort_intent(intent_id, "E_INTERNAL", "unexpected engine failure", context)
                raise EngineError("E_INTERNAL", "unexpected engine failure", context) from exc

    def _build_and_commit(
        self,
        *,
        request_id: str,
        sql: str,
        statement_key: str,
        intent_id: str,
        injection: dict[str, Any],
    ) -> dict[str, Any]:
        effective_sql = sql
        injection_kind = str(injection.get("kind", ""))
        if injection_kind == "malformed_sql":
            effective_sql = sql + (" " if sql and not sql[-1].isspace() else "") + "WHERE"

        statement = parse_sql(effective_sql)
        force_invalid_aggregate = injection_kind == "invalid_aggregate"
        initial_plan = build_logical_plan(
            statement,
            self.catalog,
            force_invalid_aggregate=False,
        )
        initial_node_count = node_count(initial_plan)
        node_limit = self.max_nodes
        if injection_kind == "plan_too_large":
            requested_limit = injection.get("limit", 1)
            if not isinstance(requested_limit, int) or isinstance(requested_limit, bool):
                raise BadRequestError("plan_too_large.limit must be an integer")
            node_limit = min(self.max_nodes, max(0, requested_limit))
        if initial_node_count > node_limit:
            raise PlanTooLargeError(
                initial_node_count,
                node_limit,
                stage="logical_plan_built",
                injected=injection_kind == "plan_too_large",
            )

        if force_invalid_aggregate:
            raise self._forced_aggregate_error(initial_plan)

        abort_after = None
        if injection_kind == "rewrite_aborted":
            abort_after = injection.get("abort_after", 0)
            if not isinstance(abort_after, int) or isinstance(abort_after, bool) or abort_after < 0:
                raise BadRequestError("rewrite_aborted.abort_after must be a non-negative integer")
        normalized = normalize_plan(
            initial_plan,
            max_steps=self.max_rewrite_steps,
            abort_after=abort_after,
        )
        final_node_count = node_count(normalized.plan)
        if final_node_count > node_limit:
            raise PlanTooLargeError(
                final_node_count,
                node_limit,
                stage="normalized_plan",
                injected=injection_kind == "plan_too_large",
            )

        original_execution = execute_plan(initial_plan, self.sample_db)
        normalized_execution = execute_plan(normalized.plan, self.sample_db)
        equivalent = original_execution == normalized_execution
        if not equivalent:
            raise SemanticError(
                "normalized plan result differs from the initial logical plan",
                before_fingerprint=normalized.before_fingerprint,
                after_fingerprint=normalized.after_fingerprint,
                rule="semantic_equivalence_guard",
            )
        core = {
            "sql": sql,
            "business_code": "OK",
            "initial_node_count": initial_node_count,
            "node_count": final_node_count,
            "plan": normalized.plan.to_dict(),
            "normalized_plan": serialize_plan(normalized.plan),
            "plan_text": format_plan_text(normalized.plan),
            "node_summary": node_summary(normalized.plan),
            "rewrite_log": normalized.log,
            "rejections": normalized.rejections,
            "node_mapping": normalized.node_mapping,
            "before_fingerprint": normalized.before_fingerprint,
            "after_fingerprint": normalized.after_fingerprint,
            "rounds": normalized.rounds,
            "rule_attempts": normalized.rule_attempts,
            "rule_applications": sum(1 for entry in normalized.log if entry["status"] == "applied"),
            "rule_rejections": len(normalized.rejections),
            "query_result": normalized_execution.as_dict(),
            "semantic_equivalence": {
                "original_plan_equals_normalized_plan_result": equivalent,
                "checked_with": "local_plan_executor_over_fixed_sqlite_sample",
            },
            "injection": injection,
        }
        _, stored = self.store.commit_result(
            intent_id=intent_id,
            request_id=request_id,
            sql=sql,
            statement_key=statement_key,
            normalized_plan=core["normalized_plan"],
            plan_json=serialize_plan(normalized.plan),
            result=core,
        )
        return stored

    def _forced_aggregate_error(self, plan: Any) -> InvalidAggregateError:
        aggregate_node = next(
            (node for node, _, _ in iter_nodes(plan) if node.node_type == "Aggregate"), None
        )
        grouped: set[str] = set()
        aggregate_inputs: set[str] = set()
        if aggregate_node is not None:
            for entry in aggregate_node.attributes.get("group_by", []):
                grouped.update(columns_in(entry["expr"]))
            for entry in aggregate_node.attributes.get("aggregates", []):
                aggregate_inputs.update(columns_in(entry["expr"]))
        conflicting = None
        for node, _, _ in iter_nodes(plan):
            for expression in _node_expressions(node):
                for item in walk(expression):
                    if (
                        item.get("kind") == "column"
                        and item["name"] not in grouped
                        and item["name"] not in aggregate_inputs
                    ):
                        conflicting = item["name"]
                        break
                if conflicting:
                    break
            if conflicting:
                break
        return InvalidAggregateError(
            "injected aggregate validation conflict: non-grouped column referenced",
            clause="INJECTION",
            rule="aggregate_validation",
            injected=True,
            node_id=aggregate_node.node_id if aggregate_node else None,
            conflicting_column=conflicting or "<synthetic-ungrouped-column>",
            group_by=sorted(grouped),
        )


def _node_expressions(node: Any) -> list[Any]:
    attrs = node.attributes
    if node.node_type == "Filter":
        return [attrs["predicate"]]
    if node.node_type == "Join" and attrs.get("condition") is not None:
        return [attrs["condition"]]
    if node.node_type == "Project":
        return [item["expr"] for item in attrs.get("items", [])]
    if node.node_type == "Sort":
        return [item["expr"] for item in attrs.get("keys", [])]
    if node.node_type == "Aggregate":
        return [entry["expr"] for entry in attrs.get("group_by", [])] + [
            entry["expr"] for entry in attrs.get("aggregates", [])
        ]
    return []


def _default_request_id(sql: str, injection: dict[str, Any]) -> str:
    suffix = hashlib.sha256(
        (sql + "\0" + str(sorted(injection.items()))).encode("utf-8")
    ).hexdigest()[:16]
    return f"req_{suffix}"


def _validate_request_id(request_id: str) -> None:
    if not isinstance(request_id, str) or not request_id:
        raise BadRequestError("request_id must be a non-empty string")
    if len(request_id) > 160:
        raise BadRequestError("request_id is too long", max_length=160)
    if any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-" for char in request_id):
        raise BadRequestError(
            "request_id may contain only letters, digits, '.', '_', ':', and '-'",
            request_id=request_id,
        )


def _validate_injection(injection: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(injection, dict):
        raise BadRequestError("injection must be an object")
    kind = injection.get("kind")
    if kind is None:
        return {}
    if not isinstance(kind, str) or kind not in INJECTION_KINDS:
        raise BadRequestError(
            "unknown injection kind", injection_kind=kind, supported=sorted(INJECTION_KINDS)
        )
    return dict(injection)


def _with_response_metadata(
    result: dict[str, Any],
    *,
    request_id: str,
    reused: bool,
    reuse_kind: str | None,
    injection: dict[str, Any],
) -> dict[str, Any]:
    payload = ok_envelope(**result)
    payload["request_id"] = request_id
    payload["reused"] = reused
    payload["reuse_kind"] = reuse_kind
    payload["injection"] = injection
    return payload


def build_injection_from_request(
    body: dict[str, Any], query: dict[str, list[str]]
) -> dict[str, Any]:
    raw = body.get("inject", query.get("inject", [None])[0])
    if isinstance(raw, dict):
        injection = dict(raw)
    elif isinstance(raw, str):
        injection = {"kind": raw}
    elif raw is None:
        injection = {}
    else:
        raise BadRequestError("inject must be a string or object")

    if body.get("inject_malformed_sql") in (True, "true", "1", 1) or query.get(
        "inject_malformed_sql", ["false"]
    )[0] in ("true", "1"):
        injection = {"kind": "malformed_sql"}
    if body.get("inject_invalid_aggregate") in (True, "true", "1", 1) or query.get(
        "inject_invalid_aggregate", ["false"]
    )[0] in ("true", "1"):
        injection = {"kind": "invalid_aggregate"}
    plan_limit = body.get("inject_plan_node_limit", query.get("inject_plan_node_limit", [None])[0])
    if plan_limit is not None:
        try:
            limit_value = int(plan_limit)
        except (TypeError, ValueError) as exc:
            raise BadRequestError("inject_plan_node_limit must be an integer") from exc
        injection = {"kind": "plan_too_large", "limit": limit_value}
    abort_after = body.get("inject_rewrite_abort_after", query.get("inject_rewrite_abort_after", [None])[0])
    if abort_after is not None:
        try:
            abort_value = int(abort_after)
        except (TypeError, ValueError) as exc:
            raise BadRequestError("inject_rewrite_abort_after must be an integer") from exc
        injection = {"kind": "rewrite_aborted", "abort_after": abort_value}
    return _validate_injection(injection)

