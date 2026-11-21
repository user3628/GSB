from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any

from .config import WorkbenchConfig
from .errors import invalid_snapshot
from .sql_analysis import (
    MissingIndex,
    append_limit,
    build_reordered_predicates,
    build_split_or,
    detect_missing_indexes,
    estimate_result_rows,
    low_ndv_predicate_columns,
    parse_query,
)
from .util import canonical_json, quote_identifier


CAUSE_MISSING_INDEX = "MISSING_INDEX"
CAUSE_SELECTIVITY_BIAS = "SELECTIVITY_ESTIMATE_BIAS"
CAUSE_LOCK_WAIT = "LOCK_WAIT"
CAUSE_EXCESSIVE_ROWS = "EXCESSIVE_RETURNED_ROWS"
CAUSE_OR_PREDICATE = "OR_PREDICATE"

KIND_ADD_INDEX = "ADD_INDEX"
KIND_REORDER = "REORDER_PREDICATES"
KIND_SPLIT_OR = "SPLIT_OR"
KIND_ADD_LIMIT = "ADD_LIMIT"


@dataclass(slots=True)
class DiagnosisPlan:
    is_slow: bool
    analysis: dict[str, Any]
    suggestions: list[dict[str, Any]]


def _safe_rule_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").upper()
    return token[:80] or "OBJECT"


def _suggestion_id(session_id: str, rule_id: str, kind: str, rewritten_sql: str) -> str:
    seed = f"{session_id}|{rule_id}|{kind}|{rewritten_sql}"
    return "sug_" + uuid.uuid5(uuid.NAMESPACE_URL, seed).hex


class DiagnosisPlanner:
    def __init__(self, config: WorkbenchConfig) -> None:
        self.config = config

    def plan(
        self,
        *,
        session_id: str,
        sample: dict[str, Any],
        snapshot: dict[str, Any] | None,
    ) -> DiagnosisPlan:
        if not snapshot:
            raise invalid_snapshot(
                "A statistics snapshot must be loaded before diagnosis suggestions can be generated."
            )
        snapshot_version = int(snapshot["version"])
        sql = sample["sql"]
        parsed = parse_query(sql)
        estimated_rows = estimate_result_rows(parsed, snapshot)
        triggers: list[dict[str, Any]] = []
        if sample["duration_ms"] >= self.config.slow_duration_ms:
            triggers.append(
                {
                    "code": "DURATION_THRESHOLD",
                    "actual": sample["duration_ms"],
                    "threshold": self.config.slow_duration_ms,
                }
            )
        if sample["scanned_rows"] >= self.config.slow_scan_rows:
            triggers.append(
                {
                    "code": "SCAN_ROWS_THRESHOLD",
                    "actual": sample["scanned_rows"],
                    "threshold": self.config.slow_scan_rows,
                }
            )
        if sample["lock_wait_ms"] >= self.config.lock_wait_threshold_ms:
            triggers.append(
                {
                    "code": "LOCK_WAIT_THRESHOLD",
                    "actual": sample["lock_wait_ms"],
                    "threshold": self.config.lock_wait_threshold_ms,
                }
            )
        is_slow = bool(triggers)
        if not is_slow:
            analysis = {
                "is_slow": False,
                "snapshot_version": snapshot_version,
                "thresholds": self._thresholds(),
                "decision": "NOT_SLOW",
                "threshold_triggers": [],
                "estimated_returned_rows": round(estimated_rows, 2),
                "slow_reasons": [],
                "suggestion_ids": [],
                "query_shape": self._shape(parsed),
            }
            return DiagnosisPlan(False, analysis, [])

        missing_indexes = detect_missing_indexes(parsed, snapshot)
        low_ndv_columns = low_ndv_predicate_columns(parsed, snapshot)
        suggestions: list[dict[str, Any]] = []
        seen: dict[tuple[str, str, str], dict[str, Any]] = {}

        def add_suggestion(
            *,
            rule_id: str,
            kind: str,
            title: str,
            rewritten_sql: str,
            expected_benefit: str,
            cause_code: str,
            setup_sql: list[str] | None = None,
        ) -> str:
            setup = setup_sql or []
            dedupe_key = (kind, rewritten_sql, canonical_json(setup))
            existing = seen.get(dedupe_key)
            if existing:
                if cause_code not in existing["cause_codes"]:
                    existing["cause_codes"].append(cause_code)
                return existing["suggestion_id"]
            suggestion = {
                "suggestion_id": _suggestion_id(session_id, rule_id, kind, rewritten_sql),
                "session_id": session_id,
                "rule_id": rule_id,
                "kind": kind,
                "title": title,
                "original_sql": sql,
                "rewritten_sql": rewritten_sql,
                "setup_sql": setup,
                "expected_benefit": expected_benefit,
                "snapshot_version": snapshot_version,
                "cause_codes": [cause_code],
            }
            suggestions.append(suggestion)
            seen[dedupe_key] = suggestion
            return suggestion["suggestion_id"]

        reasons: list[dict[str, Any]] = []
        missing_suggestion_ids: list[str] = []
        for missing in missing_indexes:
            suggestion_id = add_suggestion(
                rule_id=self._missing_index_rule_id(missing),
                kind=KIND_ADD_INDEX,
                title=f"为 {missing.table}({', '.join(missing.columns)}) 增加复合索引",
                rewritten_sql=sql,
                setup_sql=[self._create_index_sql(missing)],
                expected_benefit=(
                    f"预计将 {missing.table} 的等值访问从宽扫描收窄到复合索引查找，"
                    f"索引列按不同值数从高到低排列：{', '.join(missing.columns)}。"
                ),
                cause_code=CAUSE_MISSING_INDEX,
            )
            missing_suggestion_ids.append(suggestion_id)
            reasons.append(
                {
                    "code": CAUSE_MISSING_INDEX,
                    "message": "查询的等值谓词没有可由最左列使用的索引。",
                    "evidence": {
                        "table": missing.table,
                        "columns": list(missing.columns),
                        "ndv_estimates": list(missing.ndv_estimates),
                    },
                    "suggestion_ids": [suggestion_id],
                }
            )

        split_suggestion_id: str | None = None
        if parsed.has_top_level_or:
            split_sql = build_split_or(parsed)
            if split_sql:
                split_suggestion_id = add_suggestion(
                    rule_id="R_SPLIT_OR",
                    kind=KIND_SPLIT_OR,
                    title="将顶层 OR 拆分为 UNION 分支",
                    rewritten_sql=split_sql,
                    expected_benefit=(
                        "让每个 OR 分支独立利用索引，避免 OR 条件退化为宽扫描；"
                        "使用 UNION 保持原查询的行集合语义。"
                    ),
                    cause_code=CAUSE_OR_PREDICATE,
                )
                reasons.append(
                    {
                        "code": CAUSE_OR_PREDICATE,
                        "message": "顶层 OR 谓词可能阻止稳定地使用单一索引路径。",
                        "evidence": {
                            "branch_count": len(parsed.or_segments),
                            "branches": parsed.or_segments,
                        },
                        "suggestion_ids": [split_suggestion_id],
                    }
                )

        reorder_suggestion_id: str | None = None
        if not missing_indexes and not parsed.has_top_level_or:
            selectivity_ratio = estimated_rows / max(float(sample["scanned_rows"]), 1.0)
            if (
                low_ndv_columns
                and sample["scanned_rows"] >= self.config.slow_scan_rows
                and selectivity_ratio < self.config.selectivity_bias_ratio
            ):
                reordered_sql = build_reordered_predicates(parsed, snapshot)
                if reordered_sql:
                    reorder_suggestion_id = add_suggestion(
                        rule_id="R_REORDER_PREDICATES",
                        kind=KIND_REORDER,
                        title="按选择率重写谓词顺序",
                        rewritten_sql=reordered_sql,
                        expected_benefit=(
                            "先放置不同值数更高的等值谓词，降低基数估算偏差对访问路径的影响，"
                            "并让 DBA 更容易审阅高选择性条件。"
                        ),
                        cause_code=CAUSE_SELECTIVITY_BIAS,
                    )
                    reasons.append(
                        {
                            "code": CAUSE_SELECTIVITY_BIAS,
                            "message": "低不同值列导致统计估算值显著低于实际扫描量。",
                            "evidence": {
                                "estimated_returned_rows": round(estimated_rows, 2),
                                "scanned_rows": sample["scanned_rows"],
                                "ratio": round(selectivity_ratio, 4),
                                "threshold_ratio": self.config.selectivity_bias_ratio,
                                "low_ndv_columns": [
                                    {"table": table, "column": column, "ndv": ndv}
                                    for table, column, ndv in low_ndv_columns
                                ],
                            },
                            "suggestion_ids": [reorder_suggestion_id],
                        }
                    )

        limit_suggestion_id: str | None = None
        excessive_rows = (
            estimated_rows >= self.config.returned_rows_threshold and not parsed.has_limit
        )
        if excessive_rows:
            limited_sql = append_limit(sql, self.config.limit_ceiling)
            limit_suggestion_id = add_suggestion(
                rule_id="R_ADD_LIMIT_GUARD",
                kind=KIND_ADD_LIMIT,
                title=f"增加 LIMIT {self.config.limit_ceiling} 保护",
                rewritten_sql=limited_sql,
                expected_benefit=(
                    f"把单次返回行数上限约束为 {self.config.limit_ceiling}，"
                    "限制客户端传输与上层处理成本。"
                ),
                cause_code=CAUSE_EXCESSIVE_ROWS,
            )
            reasons.append(
                {
                    "code": CAUSE_EXCESSIVE_ROWS,
                    "message": "统计快照估算的返回行数超过工作台阈值且查询没有 LIMIT。",
                    "evidence": {
                        "estimated_returned_rows": round(estimated_rows, 2),
                        "threshold": self.config.returned_rows_threshold,
                        "limit_ceiling": self.config.limit_ceiling,
                    },
                    "suggestion_ids": [limit_suggestion_id],
                }
            )

        if sample["lock_wait_ms"] >= self.config.lock_wait_threshold_ms:
            lock_suggestion_ids: list[str] = []
            if missing_suggestion_ids:
                lock_suggestion_ids.extend(missing_suggestion_ids)
            if limit_suggestion_id:
                lock_suggestion_ids.append(limit_suggestion_id)
            if split_suggestion_id:
                lock_suggestion_ids.append(split_suggestion_id)
            if not lock_suggestion_ids:
                lock_guard_sql = (
                    sql if parsed.has_limit else append_limit(sql, self.config.limit_ceiling)
                )
                limit_suggestion_id = add_suggestion(
                    rule_id="R_LOCK_WINDOW_LIMIT",
                    kind=KIND_ADD_LIMIT,
                    title="限制结果集以缩短锁持有窗口",
                    rewritten_sql=lock_guard_sql,
                    expected_benefit=(
                        "减少结果读取与客户端传输时长，降低连接和锁占用窗口。"
                    ),
                    cause_code=CAUSE_LOCK_WAIT,
                )
                lock_suggestion_ids.append(limit_suggestion_id)
            for suggestion in suggestions:
                if suggestion["suggestion_id"] in lock_suggestion_ids:
                    if CAUSE_LOCK_WAIT not in suggestion["cause_codes"]:
                        suggestion["cause_codes"].append(CAUSE_LOCK_WAIT)
            reasons.append(
                {
                    "code": CAUSE_LOCK_WAIT,
                    "message": "样本锁等待时间达到工作台阈值。",
                    "evidence": {
                        "lock_wait_ms": sample["lock_wait_ms"],
                        "threshold": self.config.lock_wait_threshold_ms,
                    },
                    "suggestion_ids": list(dict.fromkeys(lock_suggestion_ids)),
                }
            )

        for reason in reasons:
            if not reason["suggestion_ids"]:
                raise RuntimeError(f"slow reason {reason['code']} has no rewrite suggestion")

        suggestion_ids = [item["suggestion_id"] for item in suggestions]
        analysis = {
            "is_slow": True,
            "snapshot_version": snapshot_version,
            "thresholds": self._thresholds(),
            "decision": "SLOW",
            "threshold_triggers": triggers,
            "estimated_returned_rows": round(estimated_rows, 2),
            "slow_reasons": reasons,
            "suggestion_ids": suggestion_ids,
            "query_shape": self._shape(parsed),
        }
        return DiagnosisPlan(True, analysis, suggestions)

    def _missing_index_rule_id(self, missing: MissingIndex) -> str:
        columns = "_".join(_safe_rule_token(column) for column in missing.columns)
        return f"R_ADD_INDEX_{_safe_rule_token(missing.table)}_{columns}"

    def _create_index_sql(self, missing: MissingIndex) -> str:
        columns = "_".join(_safe_rule_token(column).lower() for column in missing.columns)
        index_name = f"idx_sq_{_safe_rule_token(missing.table).lower()}_{columns}"
        index_name = index_name[:120]
        column_sql = ", ".join(quote_identifier(column) for column in missing.columns)
        return (
            f"CREATE INDEX {quote_identifier(index_name)} ON "
            f"{quote_identifier(missing.table)} ({column_sql})"
        )

    def _thresholds(self) -> dict[str, Any]:
        return {
            "duration_ms": self.config.slow_duration_ms,
            "scanned_rows": self.config.slow_scan_rows,
            "lock_wait_ms": self.config.lock_wait_threshold_ms,
            "returned_rows": self.config.returned_rows_threshold,
            "selectivity_bias_ratio": self.config.selectivity_bias_ratio,
            "limit_ceiling": self.config.limit_ceiling,
        }

    def _shape(self, parsed) -> dict[str, Any]:
        return {
            "tables": parsed.tables,
            "has_where": parsed.where_text is not None,
            "has_top_level_or": parsed.has_top_level_or,
            "has_limit": parsed.has_limit,
            "has_order": parsed.has_order,
            "has_group": parsed.has_group,
            "has_union": parsed.has_union,
        }
