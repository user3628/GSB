"""High-level workbench service tying shards, config and engine together."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .config import WorkbenchConfig, load_config
from .engine import Engine, RunReport, reduce_results
from .errors import E_NOT_FOUND, WorkbenchError
from .shards import Shard, load_shards, write_manifest

IMPORT_FILENAME = "import.json"


@dataclass
class Workspace:
    root: Path
    checkpoint_root: Path
    shards_dir: Path
    manifest_path: Path
    config_path: Path

    @classmethod
    def create(cls, root: str | Path, checkpoint_root: str | Path | None = None,
               shards_dir: str | Path | None = None) -> "Workspace":
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        checkpoints = Path(checkpoint_root) if checkpoint_root else root / "checkpoints"
        checkpoints.mkdir(parents=True, exist_ok=True)
        shards_dir = Path(shards_dir) if shards_dir else root / "shards"
        shards_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            root=root, checkpoint_root=checkpoints, shards_dir=shards_dir,
            manifest_path=shards_dir / "manifest.json",
            config_path=root / "config.yaml",
        )


class WorkbenchService:
    def __init__(self, workspace: Workspace):
        self.workspace = workspace

    def import_bundle(self, source_shards_dir: str | Path,
                      manifest_path: str | Path,
                      config_path: str | Path) -> dict[str, Any]:
        # Validate everything before mutating the workspace.
        shards = load_shards(source_shards_dir, manifest_path)
        config = load_config(config_path)
        source_root = Path(source_shards_dir)
        config_source = Path(config_path)
        for shard in shards:
            target = self.workspace.shards_dir / shard.filename
            target.write_text(
                (source_root / shard.filename).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        write_manifest(self.workspace.manifest_path, shards)
        self.workspace.config_path.write_text(
            config_source.read_text(encoding="utf-8"), encoding="utf-8")
        record = {
            "shard_fingerprints": [s.fingerprint for s in shards],
            "config_fingerprint": config.config_fingerprint,
            "model_fingerprint": config.model_fingerprint,
            "budget_fingerprint": config.budget_fingerprint,
            "shard_count": len(shards),
        }
        (self.workspace.root / IMPORT_FILENAME).write_text(
            json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return record

    def _load(self) -> tuple[list[Shard], WorkbenchConfig]:
        if not self.workspace.manifest_path.exists():
            raise WorkbenchError(E_NOT_FOUND, "no imported manifest in workspace",
                                 {"path": str(self.workspace.manifest_path)})
        shards = load_shards(self.workspace.shards_dir, self.workspace.manifest_path)
        config = load_config(self.workspace.config_path)
        return shards, config

    def _engine(self, shard_complete_callback=None) -> tuple[Engine, list[Shard],
                                                           WorkbenchConfig]:
        shards, config = self._load()
        store = CheckpointStore(self.workspace.checkpoint_root)
        return Engine(self.workspace.root, shards, config, store,
                      shard_complete_callback=shard_complete_callback), shards, config

    def plan(self) -> dict[str, Any]:
        engine, _, _ = self._engine()
        return engine.plan().to_dict()

    def run(self, *, sequential: bool = False,
            shard_complete_callback=None) -> RunReport:
        engine, _, _ = self._engine(shard_complete_callback)
        return engine.run(sequential=sequential)

    def resume(self, *, sequential: bool = False,
               shard_complete_callback=None) -> RunReport:
        engine, _, _ = self._engine(shard_complete_callback)
        state = engine.read_state()
        if state.get("stage") not in ("stopped", "failed", "planned", "running",
                                      "completed", "new"):
            raise WorkbenchError(
                E_NOT_FOUND, f"cannot resume from stage {state.get('stage')}",
                {"stage": state.get("stage")},
            )
        engine.clear_stop()
        return engine.run(sequential=sequential)

    def stop(self) -> dict[str, Any]:
        engine, _, _ = self._engine()
        engine.request_stop()
        return {"stop_requested": True, "stop_file": str(engine.stop_path)}

    def status(self) -> dict[str, Any]:
        engine, shards, config = self._engine()
        state = engine.read_state()
        store = CheckpointStore(self.workspace.checkpoint_root)
        records, warnings = store.load_consistent_prefix()
        record_by_shard = {r.shard_index: r for r in records}
        plan_data = (
            json.loads(engine.plan_path.read_text(encoding="utf-8"))
            if engine.plan_path.exists() else None
        )
        decision_by_shard = (
            {d["shard_index"]: d for d in plan_data["decisions"]}
            if plan_data else {}
        )
        shard_status: list[dict[str, Any]] = []
        for shard in shards:
            record = record_by_shard.get(shard.index)
            decision = decision_by_shard.get(shard.index)
            invalidation = None
            if record is not None:
                if record.input_fingerprint != shard.fingerprint:
                    invalidation = "INPUT_CHANGED"
                elif record.model_fingerprint != config.model_fingerprint:
                    invalidation = "MODEL_CHANGED"
                elif record.config_fingerprint != config.config_fingerprint:
                    invalidation = "BUDGET_OR_CONFIG_CHANGED"
            shard_status.append({
                **shard.to_status(),
                "state": "checkpointed" if record else (
                    decision["reason_code"] if decision and not decision["admitted"]
                    else "pending"
                ),
                "worker_id": decision["worker_id"] if decision else None,
                "admitted": decision["admitted"] if decision else None,
                "reason_code": decision["reason_code"] if decision else None,
                "record_config_fingerprint": (
                    record.config_fingerprint if record else None),
                "record_model_fingerprint": (
                    record.model_fingerprint if record else None),
                "invalidation": invalidation,
                "sequence": record.sequence if record else None,
            })
        return {
            "stage": state.get("stage", "new"),
            "stop_requested": engine.stop_requested(),
            "config_fingerprint": config.config_fingerprint,
            "model_fingerprint": config.model_fingerprint,
            "budget_fingerprint": config.budget_fingerprint,
            "checkpoint_prefix": sorted(record_by_shard),
            "checkpoint_warnings": [w.to_dict() for w in warnings],
            "shards": shard_status,
            "plan": plan_data,
        }

    def stats(self, up_to_shard: int | None = None) -> dict[str, Any]:
        engine, _, _ = self._engine()
        return engine.aggregate(up_to_shard)

    def export(self, export_path: str | Path) -> Path:
        engine, _, _ = self._engine()
        return engine.export_results(export_path)

    def reset(self) -> None:
        store = CheckpointStore(self.workspace.checkpoint_root)
        store.reset()
        for path in (self.workspace.root / "results").glob("shard_*.json"):
            path.unlink()
        for name in ("run_state.json", "plan.json", "STOP"):
            target = self.workspace.root / name
            if target.exists():
                target.unlink()
