"""Stage orchestration, checkpoints, invalidation, and query-summary stage."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .checkpoints import CheckpointStore
from .config import STAGES, IndexConfig
from .errors import GracefulStop, error
from .locking import FileLock
from .stages import (
    load_stage2,
    load_stage3,
    stage1_tokenize,
    stage2_statistics,
    stage3_postings,
    verify_compressed_postings,
)
from .utils import (
    atomic_write_json,
    canonical_json_bytes,
    changed_paths,
    digest_files,
    load_json,
    sha256_bytes,
)


StopCheck = Callable[[], bool]


class Pipeline:
    def __init__(self, config: IndexConfig, workspace: str | Path | None = None):
        self.config = config
        self.workspace = Path(workspace or Path.cwd()).resolve()
        self.artifacts_dir = self.workspace / "artifacts"
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints = CheckpointStore(self.artifacts_dir)
        self.snapshot_path = self.artifacts_dir / "build_state.json"
        self.invalidation_path = self.artifacts_dir / "invalidation_history.json"
        self.build_lock_path = self.artifacts_dir / ".build.lock"

    def stage_dir(self, stage: int) -> Path:
        return self.artifacts_dir / f"stage{stage}"

    def stage_artifact_paths(self, stage: int) -> list[Path]:
        if stage == 1:
            return [self.stage_dir(1) / "terms.json"]
        if stage == 2:
            return [self.stage_dir(2) / "statistics.json"]
        if stage == 3:
            return [self.stage_dir(3) / "postings.bin", self.stage_dir(3) / "postings.json"]
        if stage == 4:
            return [self.stage_dir(4) / "query_results.json"]
        raise error("E_INVALID_STAGE", "阶段编号非法", stage=stage)

    def artifact_digest(self, stage: int) -> str | None:
        paths = self.stage_artifact_paths(stage)
        if not all(path.exists() for path in paths):
            return None
        return digest_files(self.artifacts_dir, paths)

    def _snapshot(self) -> dict[str, Any] | None:
        payload = load_json(self.snapshot_path)
        return payload if isinstance(payload, dict) else None

    def _old_stage_fingerprints(self, active: list[dict[str, Any]]) -> dict[str, str]:
        return {
            record["stage"]: record["stage_fingerprint"]
            for record in active
        }

    def _invalidation_reasons(
        self,
        *,
        earliest: int,
        active: list[dict[str, Any]],
        artifact_mismatch: bool,
        force: bool,
    ) -> list[str]:
        reasons: list[str] = []
        if force:
            reasons.append("force_rebuild")
        snapshot = self._snapshot()
        if snapshot is None:
            reasons.append("no_consistent_build_state")
        else:
            if snapshot.get("dataset_digest") != self.config.dataset_digest:
                reasons.append("documents")
            previous_config = snapshot.get("config_snapshot")
            if isinstance(previous_config, dict):
                current_config = self.config.normalized_snapshot()
                for path in changed_paths(previous_config, current_config):
                    if path not in reasons:
                        reasons.append(path)
        if len(active) < 4:
            reasons.append("missing_checkpoint")
        if artifact_mismatch:
            reasons.append("artifact_missing_or_digest_mismatch")
        if not reasons:
            reasons.append("stage_fingerprint_changed")
        return sorted(set(reasons))

    def inspect(self, force: bool = False) -> dict[str, Any]:
        active, ignored = self.checkpoints.active()
        current = self.config.stage_fingerprints()
        earliest: int | None = 1 if force else None
        artifact_mismatch = False
        reason_entries: list[dict[str, Any]] = []
        for stage in range(1, 5):
            record = active[stage - 1] if len(active) >= stage else None
            current_fingerprint = current[STAGES[stage]]
            if record is None:
                earliest = earliest or stage
                reason_entries.append({"stage": stage, "reason": "missing_checkpoint"})
                break
            if record["stage_fingerprint"] != current_fingerprint:
                earliest = earliest or stage
                reason_entries.append({"stage": stage, "reason": "stage_fingerprint_changed"})
                break
            expected_input = self.config.stage_input_digest(stage)
            if record["input_digest"] != expected_input:
                earliest = earliest or stage
                reason_entries.append({"stage": stage, "reason": "input_digest_changed"})
                break
            actual_artifact_digest = self.artifact_digest(stage)
            if actual_artifact_digest != record["artifact_digest"]:
                earliest = earliest or stage
                artifact_mismatch = True
                reason_entries.append({"stage": stage, "reason": "artifact_missing_or_digest_mismatch"})
                break
        if force:
            earliest = 1
        if earliest is not None and not reason_entries:
            reason_entries.append({"stage": earliest, "reason": "force_rebuild"})
        reasons = (
            self._invalidation_reasons(
                earliest=earliest or 1,
                active=active,
                artifact_mismatch=artifact_mismatch,
                force=force,
            )
            if earliest is not None
            else []
        )
        return {
            "up_to_date": earliest is None,
            "earliest_affected_stage": earliest,
            "affected_stages": [STAGES[stage] for stage in range(earliest, 5)] if earliest else [],
            "reasons": reasons,
            "reason_entries": reason_entries,
            "old_stage_fingerprints": self._old_stage_fingerprints(active),
            "new_stage_fingerprints": current,
            "old_config_fingerprint": (
                active[-1]["config_version"] if active else (self._snapshot() or {}).get("config_fingerprint")
            ),
            "new_config_fingerprint": self.config.config_fingerprint,
            "checkpoints": active,
            "ignored_checkpoints": ignored,
        }

    def status(self) -> dict[str, Any]:
        inspection = self.inspect()
        cache_payload = load_json(self.artifacts_dir / "cache" / "query_cache.json")
        cache_status: dict[str, Any] = {"present": False}
        if isinstance(cache_payload, dict):
            cache_status = {
                "present": True,
                "config_fingerprint": cache_payload.get("config_fingerprint"),
                "capacity": cache_payload.get("capacity"),
                "size": len(cache_payload.get("entries", []))
                if isinstance(cache_payload.get("entries"), list)
                else None,
            }
        history = load_json(self.invalidation_path, default=[])
        return {
            "ok": True,
            "index_version": self.config.index_version,
            "config_fingerprint": self.config.config_fingerprint,
            "dataset_digest": self.config.dataset_digest,
            "stage_fingerprints": self.config.stage_fingerprints(),
            "old_stage_fingerprints": inspection["old_stage_fingerprints"],
            "new_stage_fingerprints": inspection["new_stage_fingerprints"],
            "old_config_fingerprint": inspection["old_config_fingerprint"],
            "new_config_fingerprint": inspection["new_config_fingerprint"],
            "checkpoints": inspection["checkpoints"],
            "ignored_checkpoints": inspection["ignored_checkpoints"],
            "checkpoint_highwater": self.checkpoints.highwater(),
            "up_to_date": inspection["up_to_date"],
            "earliest_affected_stage": inspection["earliest_affected_stage"],
            "affected_stages": inspection["affected_stages"],
            "invalidation_reasons": inspection["reasons"],
            "invalidation_plan": {
                "earliest_affected_stage": inspection["earliest_affected_stage"],
                "affected_stages": inspection["affected_stages"],
                "reasons": inspection["reasons"],
                "old_stage_fingerprints": inspection["old_stage_fingerprints"],
                "new_stage_fingerprints": inspection["new_stage_fingerprints"],
            },
            "invalidation_history": history if isinstance(history, list) else [],
            "cache": cache_status,
            "artifact_digests": {
                STAGES[stage]: self.artifact_digest(stage) for stage in range(1, 5)
            },
        }

    def _history(self) -> list[dict[str, Any]]:
        history = load_json(self.invalidation_path, default=[])
        return history if isinstance(history, list) else []

    def _record_invalidation(self, *, earliest: int, active: list[dict[str, Any]], reasons: list[str], archived: list[dict[str, Any]], force: bool) -> dict[str, Any]:
        current = self.config.stage_fingerprints()
        event_body: dict[str, Any] = {
            "schema_version": 1,
            "earliest_affected_stage": earliest,
            "affected_stages": [STAGES[stage] for stage in range(earliest, 5)],
            "reasons": reasons,
            "old_config_fingerprint": (
                active[-1]["config_version"] if active else (self._snapshot() or {}).get("config_fingerprint")
            ),
            "new_config_fingerprint": self.config.config_fingerprint,
            "old_stage_fingerprints": self._old_stage_fingerprints(active),
            "new_stage_fingerprints": current,
            "archived_checkpoints": [
                {"seq": record["seq"], "stage": record["stage"]} for record in archived
            ],
            "force": force,
        }
        event_body["event_id"] = sha256_bytes(canonical_json_bytes(event_body))
        history = self._history()
        if not any(item.get("event_id") == event_body["event_id"] for item in history if isinstance(item, dict)):
            history.append(event_body)
            atomic_write_json(self.invalidation_path, history)
        return event_body

    def _clear_cache(self) -> None:
        cache_path = self.artifacts_dir / "cache" / "query_cache.json"
        if cache_path.exists():
            cache_path.unlink()

    def invalidate(self, inspection: dict[str, Any], *, force: bool = False) -> dict[str, Any] | None:
        earliest = inspection["earliest_affected_stage"]
        if earliest is None:
            return None
        active = inspection["checkpoints"]
        archived = self.checkpoints.archive_from_stage(earliest)
        for stage in range(earliest, 5):
            stage_dir = self.stage_dir(stage)
            if stage_dir.exists():
                for child in sorted(stage_dir.rglob("*"), reverse=True):
                    if child.is_file() or child.is_symlink():
                        child.unlink()
                    elif child.is_dir():
                        child.rmdir()
                stage_dir.rmdir()
        self._clear_cache()
        if active and archived or (active and earliest <= 4):
            return self._record_invalidation(
                earliest=earliest,
                active=active,
                reasons=inspection["reasons"],
                archived=archived,
                force=force,
            )
        return None

    def _publish_stage(self, stage: int) -> dict[str, Any]:
        artifact_digest = self.artifact_digest(stage)
        if artifact_digest is None:
            raise error(
                "E_INVALID_STAGE",
                "阶段产物缺失，拒绝发布检查点",
                stage=STAGES[stage],
            )
        expected_seq = self.checkpoints.highwater() + 1
        return self.checkpoints.publish(
            stage=STAGES[stage],
            input_digest=self.config.stage_input_digest(stage),
            config_version=self.config.config_fingerprint,
            artifact_digest=artifact_digest,
            stage_fingerprint=self.config.stage_fingerprint(stage),
            dataset_digest=self.config.dataset_digest,
            expected_seq=expected_seq,
        )

    def _write_build_snapshot(self, built_stages: list[int]) -> None:
        active, _ = self.checkpoints.active()
        snapshot = {
            "schema_version": 1,
            "index_version": self.config.index_version,
            "config_fingerprint": self.config.config_fingerprint,
            "dataset_digest": self.config.dataset_digest,
            "query_digest": self.config.query_digest,
            "config_snapshot": self.config.normalized_snapshot(),
            "stage_fingerprints": self.config.stage_fingerprints(),
            "artifact_digests": {
                record["stage"]: record["artifact_digest"] for record in active
            },
            "built_stages": [STAGES[stage] for stage in built_stages],
        }
        atomic_write_json(self.snapshot_path, snapshot)

    def build(self, *, force: bool = False, stop_check: StopCheck | None = None) -> dict[str, Any]:
        with FileLock(self.build_lock_path):
            inspection = self.inspect(force=force)
            event = self.invalidate(inspection, force=force)
            if inspection["up_to_date"]:
                if self._snapshot() is None:
                    self._write_build_snapshot([])
                return {
                    "ok": True,
                    "status": "up_to_date",
                    "stages_built": [],
                    "stage_fingerprints": self.config.stage_fingerprints(),
                    "checkpoints": self.checkpoints.active()[0],
                    "invalidation": event,
                }
            earliest = inspection["earliest_affected_stage"]
            built: list[int] = []
            for stage in range(int(earliest), 5):
                if stage == 1:
                    stage1_tokenize(self.config, self.stage_dir(1), stop_check=stop_check)
                elif stage == 2:
                    stage2_statistics(
                        self.config,
                        self.stage_dir(1),
                        self.stage_dir(2),
                        stop_check=stop_check,
                    )
                elif stage == 3:
                    stage3_postings(
                        self.config,
                        self.stage_dir(2),
                        self.stage_dir(3),
                        stop_check=stop_check,
                    )
                    verify_compressed_postings(
                        self.config,
                        self.stage_dir(2),
                        self.stage_dir(3),
                    )
                else:
                    self.stage4_query_summary(stop_check=stop_check)
                self._publish_stage(stage)
                built.append(stage)
            self._write_build_snapshot(built)
            return {
                "ok": True,
                "status": "complete",
                "stages_built": [STAGES[stage] for stage in built],
                "stage_fingerprints": self.config.stage_fingerprints(),
                "checkpoints": self.checkpoints.active()[0],
                "invalidation": event,
            }

    def stage4_query_summary(self, stop_check: StopCheck | None = None) -> dict[str, Any]:
        """Run benchmark queries through both compressed and reference paths."""

        from .query import QueryEngine
        from .reference import reference_query

        if not self.config.query_path.exists():
            raise error(
                "E_INVALID_STAGE",
                "缺少基准查询集",
                path=str(self.config.query_path),
            )
        queries: list[dict[str, Any]] = []
        with self.config.query_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    query = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise error(
                        "E_DATA",
                        "基准查询行无法解析",
                        line=line_number,
                        reason=str(exc),
                    )
                if not isinstance(query, dict):
                    raise error("E_DATA", "基准查询必须是对象", line=line_number)
                queries.append(query)
        if not queries:
            raise error("E_DATA", "基准查询集为空")
        engine = QueryEngine(self.config, self.artifacts_dir)
        results: list[dict[str, Any]] = []
        total_hits = 0
        returned_hits = 0
        mode_counts = {"and": 0, "or": 0}
        for query in queries:
            if stop_check is not None and stop_check():
                raise GracefulStop("收到停止请求，已在查询单元边界退出")
            query_id = str(query.get("query_id", f"query-{len(results) + 1}"))
            terms = query.get("terms")
            mode = str(query.get("mode", "and")).lower()
            limit = query.get("limit", int(self.config.value("query.default_limit", 20)))
            compressed = engine.execute(
                query_id=query_id,
                terms=terms,
                mode=mode,
                limit=limit,
                use_cache=False,
            )["result"]
            sequential = reference_query(
                self.config,
                self.artifacts_dir,
                terms=terms,
                mode=mode,
                limit=limit,
            )
            if compressed != sequential:
                mismatched_terms = [
                    int(row["term_id"])
                    for row in compressed.get("terms", [])
                    if row.get("term_id") is not None
                ]
                raise error(
                    "E_QUERY_MISMATCH",
                    "压缩查询与顺序遍历参考结果不一致",
                    query_id=query_id,
                    term_ids=mismatched_terms,
                    compressed_total=compressed.get("total_hits"),
                    reference_total=sequential.get("total_hits"),
                )
            total_hits += int(compressed["total_hits"])
            returned_hits += len(compressed["hits"])
            if mode in mode_counts:
                mode_counts[mode] += 1
            results.append({"query_id": query_id, "result": compressed})
        summary_body = {
            "query_count": len(results),
            "total_hits": total_hits,
            "returned_hits": returned_hits,
            "mode_counts": mode_counts,
            "result_digest": sha256_bytes(canonical_json_bytes(results)),
        }
        payload = {
            "schema_version": 1,
            "stage": "query_summary",
            "stage_number": 4,
            "index_version": self.config.index_version,
            "stage_fingerprint": self.config.stage_fingerprint(4),
            "input_digest": self.config.stage_fingerprint(3),
            "query_digest": self.config.query_digest,
            "verification": {"compressed_equals_sequential": True},
            "summary": summary_body,
            "queries": results,
        }
        atomic_write_json(self.stage_dir(4) / "query_results.json", payload)
        return payload
