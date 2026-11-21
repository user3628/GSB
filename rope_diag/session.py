"""Persistent diagnostic session orchestration."""

from __future__ import annotations

import json
import os
import shutil
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .artifacts import ArtifactRef, ArtifactStore, safe_component
from .config import STAGE_ORDER, RopeConfig, earliest_affected_stage
from .data import ShardData, load_manifest
from .digest import atomic_write_bytes, atomic_write_json, digest_json, read_json, sha256_bytes
from .engine import RopeEngine, refs_from_report
from .errors import RopeError
from .queries import DiagnosticQueries


SESSION_SCHEMA_VERSION = 1


class DiagnosticSession:
    def __init__(
        self,
        workspace: str | Path,
        manifest_path: str | Path,
        config: RopeConfig,
        shards: list[ShardData],
        *,
        status: str = "ready",
        stage_refs: Mapping[str, Mapping[str, Mapping[str, Any]]] | None = None,
        recompute_report: Mapping[str, Any] | None = None,
        consistency: Mapping[str, Any] | None = None,
        last_error: Mapping[str, Any] | None = None,
        session_id: str | None = None,
    ) -> None:
        self.workspace = Path(workspace).resolve()
        self.manifest_path = Path(manifest_path).resolve()
        self.config = config
        self.shards = shards
        self.shard_by_id = {shard.shard_id: shard for shard in shards}
        self.status = status
        self.stage_refs: dict[str, dict[str, ArtifactRef]] = {
            shard_id: {
                stage: ArtifactRef.from_dict(ref)
                for stage, ref in dict(stages).items()
            }
            for shard_id, stages in dict(stage_refs or {}).items()
        }
        self.recompute_report = dict(recompute_report or {})
        self.consistency = dict(consistency) if consistency else None
        self.last_error = dict(last_error) if last_error else None
        self.session_id = session_id or digest_json(
            {
                "manifest": [shard.content_fingerprint for shard in shards],
                "initial_config_version": config.version,
            }
        )[:20]
        self._mutex = threading.RLock()
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "exports").mkdir(parents=True, exist_ok=True)

    @property
    def state_path(self) -> Path:
        return self.workspace / "session.json"

    @property
    def current_config_path(self) -> Path:
        return self.workspace / "current_config.yaml"

    @property
    def stop_path(self) -> Path:
        return self.workspace / "stop_requested.json"

    @classmethod
    def create(
        cls,
        workspace: str | Path,
        manifest_path: str | Path,
        config_path: str | Path,
        *,
        reset: bool = False,
    ) -> "DiagnosticSession":
        target = Path(workspace).resolve()
        if reset:
            cls._reset_runtime(target)
        elif (target / "session.json").exists():
            raise RopeError(
                "E_INVALID_STAGE",
                "工作区已有会话，如需覆盖请显式 reset",
                {"workspace": str(target)},
            )
        config = RopeConfig.from_yaml(config_path)
        shards, _ = load_manifest(manifest_path)
        session = cls(target, manifest_path, config, shards)
        atomic_write_bytes(session.current_config_path, config.to_yaml_bytes())
        session._save()
        return session

    @classmethod
    def load(cls, workspace: str | Path) -> "DiagnosticSession | None":
        target = Path(workspace).resolve()
        value = read_json(target / "session.json")
        if value is None:
            return None
        try:
            if int(value.get("schema_version", 0)) != SESSION_SCHEMA_VERSION:
                raise RopeError(
                    "E_INVALID_STAGE",
                    "不支持的会话状态版本",
                    {"schema_version": value.get("schema_version")},
                )
            config = RopeConfig.from_mapping(value["config"])
            manifest_path = Path(str(value["manifest_path"]))
            shards, _ = load_manifest(manifest_path)
            expected_fingerprints = dict(value.get("shard_fingerprints", {}))
            for shard in shards:
                expected = expected_fingerprints.get(shard.shard_id)
                if expected is not None and expected != shard.content_fingerprint:
                    raise RopeError(
                        "E_STALE_CHECKPOINT",
                        "分片内容已变更，不能恢复原检查点",
                        {
                            "shard_id": shard.shard_id,
                            "expected_fingerprint": expected,
                            "actual_fingerprint": shard.content_fingerprint,
                        },
                    )
            status = str(value.get("status", "ready"))
            recovered = False
            if status == "running":
                status = "stopped"
                recovered = True
            session = cls(
                target,
                manifest_path,
                config,
                shards,
                status=status,
                stage_refs=value.get("stage_refs", {}),
                recompute_report=value.get("recompute_report", {}),
                consistency=value.get("consistency"),
                last_error=value.get("last_error"),
                session_id=value.get("session_id"),
            )
            if recovered:
                atomic_write_json(
                    session.stop_path,
                    {
                        "requested": True,
                        "requested_at_utc": _utc_now(),
                        "reason": "process_restart_recovery",
                    },
                )
                session._save()
            return session
        except RopeError:
            raise
        except Exception as exc:
            raise RopeError(
                "E_INVALID_STAGE", f"会话状态损坏: {exc}", {"workspace": str(target)}
            ) from exc

    @staticmethod
    def _reset_runtime(workspace: Path) -> None:
        if workspace in {Path("/"), Path.home()}:
            raise RopeError(
                "E_INVALID_CONFIG", "拒绝清空高风险管理根目录", {"workspace": str(workspace)}
            )
        workspace.mkdir(parents=True, exist_ok=True)
        for name in ("artifacts", "checkpoints", "cache", "exports", "locks"):
            path = workspace / name
            if path.exists():
                shutil.rmtree(path)
        for name in ("session.json", "stop_requested.json", "current_config.yaml"):
            path = workspace / name
            if path.exists():
                path.unlink()

    def _state_value(self) -> dict[str, Any]:
        return {
            "schema_version": SESSION_SCHEMA_VERSION,
            "session_id": self.session_id,
            "workspace": str(self.workspace),
            "manifest_path": str(self.manifest_path),
            "config": self.config.to_mapping(),
            "config_version": self.config.version,
            "status": self.status,
            "shard_fingerprints": {
                shard.shard_id: shard.content_fingerprint for shard in self.shards
            },
            "shard_shapes": {shard.shard_id: list(shard.shape) for shard in self.shards},
            "stage_refs": {
                shard_id: {
                    stage: ref.to_dict() for stage, ref in stages.items()
                }
                for shard_id, stages in self.stage_refs.items()
            },
            "recompute_report": self.recompute_report,
            "consistency": self.consistency,
            "last_error": self.last_error,
            "updated_at_utc": _utc_now(),
        }

    def _save(self) -> None:
        atomic_write_json(self.state_path, self._state_value())

    def is_stop_requested(self) -> bool:
        return self.stop_path.exists()

    def request_stop(self, *, reason: str = "user_request") -> dict[str, Any]:
        with self._mutex:
            if self.status in {"ready", "running"}:
                atomic_write_json(
                    self.stop_path,
                    {"requested": True, "requested_at_utc": _utc_now(), "reason": reason},
                )
                self.status = "stopping" if self.status == "running" else "stopped"
                self._save()
            return self.status_payload()

    def _clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def _ensure_no_stop_for_execution(self) -> None:
        if self.is_stop_requested():
            raise RopeError(
                "E_INVALID_STAGE",
                "停止请求仍有效，请先执行 resume",
                {"status": self.status},
            )

    def run(self, *, stop_after_stage: str | None = None) -> dict[str, Any]:
        with self._mutex:
            if self.status == "running":
                raise RopeError("E_BUSY", "会话正在运行", {"status": self.status})
            if stop_after_stage is not None and stop_after_stage not in STAGE_ORDER:
                raise RopeError(
                    "E_INVALID_STAGE",
                    "未知阶段",
                    {"stage": stop_after_stage, "valid_stages": list(STAGE_ORDER)},
                )
            if self.is_stop_requested():
                self.status = "stopped"
                self._save()
                return self.status_payload()
            self.status = "running"
            self.last_error = None
            self._save()
        return self._execute(
            reuse_refs={}, stop_after_stage=stop_after_stage, incremental=False
        )

    def resume(self) -> dict[str, Any]:
        with self._mutex:
            if self.status == "running":
                raise RopeError("E_BUSY", "会话正在运行", {"status": self.status})
            if self.status not in {"stopped", "ready", "failed"}:
                raise RopeError(
                    "E_INVALID_STAGE",
                    "当前状态不可恢复",
                    {"status": self.status},
                )
            self._clear_stop()
            self.status = "running"
            self.last_error = None
            self._save()
        return self._execute(reuse_refs={}, stop_after_stage=None, incremental=False)

    def _execute(
        self,
        *,
        reuse_refs: Mapping[str, Mapping[str, ArtifactRef]],
        stop_after_stage: str | None,
        incremental: bool,
    ) -> dict[str, Any]:
        engine = RopeEngine(
            self.workspace,
            self.shards,
            self.config,
            external_stop=self.is_stop_requested,
        )
        try:
            report = engine.run(
                reuse_refs=reuse_refs, stop_after_stage=stop_after_stage
            )
        except Exception as exc:
            with self._mutex:
                try:
                    self.stage_refs = self._checkpoint_refs()
                except Exception:
                    self.stage_refs = {}
                self.status = "failed"
                if isinstance(exc, RopeError):
                    self.last_error = exc.to_dict()["error"]
                else:
                    self.last_error = {
                        "code": "E_IO",
                        "message": str(exc),
                        "details": {"type": type(exc).__name__},
                    }
                self._save()
            raise

        refs = refs_from_report(report, shard_ids=[shard.shard_id for shard in self.shards])
        with self._mutex:
            self.stage_refs = refs
            self._merge_recompute_report(report)
            if report["stopped"]:
                self.status = "stopped"
                atomic_write_json(
                    self.stop_path,
                    {
                        "requested": True,
                        "requested_at_utc": _utc_now(),
                        "reason": "cooperative_stop",
                        "stopped_at": report.get("stopped_at"),
                    },
                )
                self._save()
            else:
                self.status = "completed"
                self._clear_stop()
                self._save()

        if not report["stopped"] and incremental:
            try:
                consistency = engine.verify_full(refs)
            except RopeError as exc:
                with self._mutex:
                    self.status = "failed"
                    self.last_error = exc.to_dict()["error"]
                    self._save()
                raise
            with self._mutex:
                self.consistency = consistency
                self._save()

        return self.status_payload()

    def _checkpoint_refs(self) -> dict[str, dict[str, ArtifactRef]]:
        from .checkpoints import CheckpointStore

        store = CheckpointStore(self.workspace)
        refs: dict[str, dict[str, ArtifactRef]] = {}
        for shard in self.shards:
            records = store.progress(
                shard.shard_id, self.config.version, shard.content_fingerprint
            )
            refs[shard.shard_id] = {
                record.stage: record.artifact for record in records
            }
        return refs

    def _merge_recompute_report(self, report: Mapping[str, Any]) -> None:
        previous = dict(self.recompute_report or {})
        stage_summary: dict[str, dict[str, Any]] = {}
        for shard in self.shards:
            shard_report = dict(report.get("shards", {}).get(shard.shard_id, {}))
            for stage, stage_report in dict(shard_report.get("stages", {})).items():
                bucket = stage_summary.setdefault(
                    str(stage), {"statuses": {}, "schedulers": {}}
                )
                bucket["statuses"][shard.shard_id] = stage_report.get("status")
                bucket["schedulers"][shard.shard_id] = stage_report.get("scheduler", {})
        self.recompute_report = {
            "changed_fields": previous.get("changed_fields", []),
            "earliest_affected_stage": previous.get("earliest_affected_stage"),
            "stage_summary": stage_summary,
            "run_elapsed_ms": report.get("elapsed_ms"),
            "stopped": report.get("stopped", False),
        }

    def update_config(
        self,
        patch: Mapping[str, Any],
        *,
        run: bool = True,
        verify: bool = True,
        stop_after_stage: str | None = None,
    ) -> dict[str, Any]:
        with self._mutex:
            if self.status == "running":
                raise RopeError("E_BUSY", "不能修改运行中的配置", {"status": self.status})
            old_config = self.config
            new_config = old_config.with_patch(patch)
            changed = old_config.changed_fields(new_config)
            affected = earliest_affected_stage(changed)
            reuse: dict[str, dict[str, ArtifactRef]] = {}
            affected_index = STAGE_ORDER.index(affected) if affected else len(STAGE_ORDER)
            for shard in self.shards:
                existing = self.stage_refs.get(shard.shard_id, {})
                reuse[shard.shard_id] = {
                    stage: existing[stage]
                    for stage in STAGE_ORDER[:affected_index]
                    if stage in existing
                }
            self.config = new_config
            atomic_write_bytes(self.current_config_path, new_config.to_yaml_bytes())
            self.stage_refs = {}
            self.consistency = None
            self.last_error = None
            self.status = "ready"
            self.recompute_report = {
                "changed_fields": changed,
                "earliest_affected_stage": affected,
                "computed_stages": [
                    stage for stage in STAGE_ORDER if not affected or STAGE_ORDER.index(stage) >= affected_index
                ],
                "reused_stages": [
                    stage for stage in STAGE_ORDER if affected and STAGE_ORDER.index(stage) < affected_index
                ],
                "stage_summary": {},
                "run_elapsed_ms": None,
                "stopped": False,
            }
            self._save()

        if run:
            with self._mutex:
                self.status = "running"
                self._save()
            return self._execute(
                reuse_refs=reuse,
                stop_after_stage=stop_after_stage,
                incremental=verify,
            )
        return self.status_payload()

    def verify(self) -> dict[str, Any]:
        with self._mutex:
            refs = {shard_id: dict(stages) for shard_id, stages in self.stage_refs.items()}
        engine = RopeEngine(self.workspace, self.shards, self.config)
        try:
            consistency = engine.verify_full(refs)
        except RopeError as exc:
            with self._mutex:
                self.last_error = exc.to_dict()["error"]
                self._save()
            raise
        with self._mutex:
            self.consistency = consistency
            self.last_error = None
            self._save()
        return consistency

    def query(
        self,
        *,
        shard_id: str,
        query_type: str,
        options: Mapping[str, Any] | None = None,
        latency_budget_ms: float | None = None,
        bypass_cache: bool = False,
    ) -> dict[str, Any]:
        shard = self.shard_by_id.get(shard_id)
        if shard is None:
            raise RopeError(
                "E_NOT_FOUND",
                "分片不存在",
                {"shard_id": shard_id, "available": sorted(self.shard_by_id)},
            )
        refs = dict(self.stage_refs.get(shard_id, {}))
        queries = DiagnosticQueries(self.workspace)
        return queries.query(
            shard=shard,
            config=self.config,
            stage_refs=refs,
            query_type=query_type,
            options=options,
            latency_budget_ms=latency_budget_ms,
            bypass_cache=bypass_cache,
        )

    def fingerprints(self) -> dict[str, Any]:
        shards: dict[str, Any] = {}
        for shard in self.shards:
            refs = self.stage_refs.get(shard.shard_id, {})
            shards[shard.shard_id] = {
                "shape": list(shard.shape),
                "content_fingerprint": shard.content_fingerprint,
                "angle_table_fingerprint": (
                    refs["frequency_generation"].artifact_digest
                    if "frequency_generation" in refs
                    else None
                ),
                "stage_artifact_fingerprints": {
                    stage: ref.artifact_digest for stage, ref in refs.items()
                },
            }
        return {
            "session_id": self.session_id,
            "config_version": self.config.version,
            "config": self.config.to_mapping(),
            "shards": shards,
        }

    def status_payload(
        self, *, shard_id: str | None = None, stage: str | None = None
    ) -> dict[str, Any]:
        if shard_id is not None and shard_id not in self.shard_by_id:
            raise RopeError(
                "E_NOT_FOUND",
                "分片不存在",
                {"shard_id": shard_id, "available": sorted(self.shard_by_id)},
            )
        if stage is not None and stage not in STAGE_ORDER:
            raise RopeError(
                "E_INVALID_STAGE",
                "未知阶段",
                {"stage": stage, "valid_stages": list(STAGE_ORDER)},
            )
        shards: dict[str, Any] = {}
        for shard in self.shards:
            if shard_id is not None and shard.shard_id != shard_id:
                continue
            refs = self.stage_refs.get(shard.shard_id, {})
            stages = {}
            for stage_name in STAGE_ORDER:
                if stage is not None and stage_name != stage:
                    continue
                ref = refs.get(stage_name)
                scheduler = (
                    self.recompute_report.get("stage_summary", {})
                    .get(stage_name, {})
                    .get("schedulers", {})
                    .get(shard.shard_id)
                )
                stages[stage_name] = {
                    "status": "completed" if ref else "pending",
                    "artifact_digest": ref.artifact_digest if ref else None,
                    "artifact_path": ref.path if ref else None,
                    "scheduler": scheduler,
                }
            shards[shard.shard_id] = {
                **shard.summary(),
                "stages": stages,
            }
        return {
            "session_id": self.session_id,
            "workspace": str(self.workspace),
            "manifest_path": str(self.manifest_path),
            "status": self.status,
            "stop_requested": self.is_stop_requested(),
            "config_version": self.config.version,
            "config": self.config.to_mapping(),
            "shards": shards,
            "recompute_report": self.recompute_report,
            "consistency": self.consistency,
            "last_error": self.last_error,
        }

    def shards_payload(self) -> dict[str, Any]:
        return {
            "manifest_path": str(self.manifest_path),
            "shards": [shard.summary() for shard in self.shards],
        }

    def export(self, output_path: str | Path | None = None) -> dict[str, Any]:
        store = ArtifactStore(self.workspace)
        summary = {
            "schema_version": 1,
            "session": self.status_payload(),
            "fingerprints": self.fingerprints(),
        }
        if output_path is None:
            output = (
                self.workspace
                / "exports"
                / f"rope-export-{safe_component(self.config.version[:16])}.zip"
            )
        else:
            output = Path(output_path).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temp = output.with_name(f".{output.name}.tmp-{os.getpid()}")
        entries: list[str] = []
        try:
            with zipfile.ZipFile(temp, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                _zip_write(archive, "summary.json", json.dumps(
                    summary, ensure_ascii=False, sort_keys=True, indent=2
                ).encode("utf-8") + b"\n")
                entries.append("summary.json")
                for shard in self.shards:
                    refs = self.stage_refs.get(shard.shard_id, {})
                    for stage in STAGE_ORDER:
                        ref = refs.get(stage)
                        if ref is None:
                            continue
                        arrays = store.load(ref)
                        for name, array in sorted(arrays.items()):
                            import io

                            buffer = io.BytesIO()
                            import numpy as np

                            np.save(buffer, np.ascontiguousarray(array), allow_pickle=False)
                            entry = (
                                f"artifacts/{safe_component(shard.shard_id)}/"
                                f"{safe_component(stage)}/{safe_component(name)}.npy"
                            )
                            _zip_write(archive, entry, buffer.getvalue())
                            entries.append(entry)
                        ref_entry = (
                            f"artifacts/{safe_component(shard.shard_id)}/"
                            f"{safe_component(stage)}/reference.json"
                        )
                        _zip_write(
                            archive,
                            ref_entry,
                            json.dumps(
                                ref.to_dict(),
                                ensure_ascii=False,
                                sort_keys=True,
                                indent=2,
                            ).encode("utf-8")
                            + b"\n",
                        )
                        entries.append(ref_entry)
            os.replace(temp, output)
        except BaseException:
            try:
                temp.unlink()
            except OSError:
                pass
            raise
        data = output.read_bytes()
        return {
            "path": str(output),
            "filename": output.name,
            "sha256": sha256_bytes(data),
            "size_bytes": len(data),
            "entries": entries,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _zip_write(archive: zipfile.ZipFile, name: str, data: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, data)
