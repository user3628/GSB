"""Persistent local workspace state."""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any, Iterable

from .checkpoints import CheckpointStore, StopController
from .config import Config
from .formats import apply_units_annotation, import_manifests, load_wav_sidecar
from .models import Project
from .util import atomic_write_json, file_lock, read_json
from .validation import validate_project


class Workspace:
    def __init__(
        self,
        root: str | Path,
        *,
        config: Config | None = None,
        config_path: str | Path | None = None,
    ) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.project_path = self.root / "project.json"
        self.config_path = self.root / "config.json"
        self.state_path = self.root / "state.json"
        self.stages_dir = self.root / "stages"
        self.exports_dir = self.root / "exports"
        self.checkpoints_dir = self.root / "checkpoints"
        self.tmp_dir = self.root / ".tmp"
        self.lock_path = self.root / ".workspace.lock"
        for directory in (self.stages_dir, self.exports_dir, self.checkpoints_dir, self.tmp_dir):
            directory.mkdir(parents=True, exist_ok=True)
        if config is not None:
            self.config = config
            self._write_config()
        elif self.config_path.exists():
            self.config = Config(read_json(self.config_path), source=str(self.config_path))
        else:
            self.config = Config.load(config_path)
            self._write_config()
        self.checkpoints = CheckpointStore(self.checkpoints_dir)
        self.stop = StopController(self.root)

    def _write_config(self) -> None:
        atomic_write_json(self.config_path, self.config.data)

    def has_project(self) -> bool:
        return self.project_path.exists()

    def save_project(self, project: Project) -> None:
        atomic_write_json(self.project_path, project.to_internal_dict())

    def load_project(self) -> Project:
        if not self.project_path.exists():
            raise FileNotFoundError(f"workspace has no project: {self.project_path}")
        return Project.from_internal_dict(read_json(self.project_path))

    def import_manifests(self, paths: Iterable[str | Path], *, project_id: str | None = None) -> Project:
        project = import_manifests(paths, self.config, project_id=project_id or self.root.name)
        self.save_project(project)
        return project

    def import_wav(self, wav_path: str | Path, sidecar_path: str | Path | None = None) -> Project:
        if sidecar_path is None:
            candidate = Path(wav_path).with_suffix(".pitch.json")
            if not candidate.exists():
                raise FileNotFoundError(f"missing WAV sidecar: {candidate}")
            sidecar_path = candidate
        shard = load_wav_sidecar(wav_path, sidecar_path, self.config)
        project = Project(
            project_id=self.root.name,
            shards=[shard],
            declared_total_samples=shard.sample_count,
            metadata={"format": "wav"},
        )
        self.save_project(project)
        return project

    def add_wav_shard(self, wav_path: str | Path, sidecar_path: str | Path | None = None) -> Project:
        if sidecar_path is None:
            candidate = Path(wav_path).with_suffix(".pitch.json")
            if not candidate.exists():
                raise FileNotFoundError(f"missing WAV sidecar: {candidate}")
            sidecar_path = candidate
        shard = load_wav_sidecar(wav_path, sidecar_path, self.config)
        if not self.has_project():
            project = Project(
                project_id=self.root.name,
                shards=[shard],
                declared_total_samples=shard.sample_count,
                metadata={"format": "wav"},
            )
        else:
            project = self.load_project()
            if any(existing.shard_id == shard.shard_id for existing in project.shards):
                from .errors import ToneLabError

                raise ToneLabError(
                    "E_VALIDATION",
                    "shard already exists in project",
                    shard_id=shard.shard_id,
                )
            project.shards.append(shard)
            project.declared_total_samples = max(
                project.declared_total_samples + shard.sample_count,
                project.total_samples,
            )
        validate_project(project, self.config)
        self.save_project(project)
        return project

    def apply_units_file(self, path: str | Path) -> Project:
        project = self.load_project()
        apply_units_annotation(project, path, self.config)
        self.save_project(project)
        return project

    def read_state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            return read_json(self.state_path)
        except Exception:
            return {}

    def write_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def update_state(self, **values: Any) -> dict[str, Any]:
        with file_lock(self.lock_path):
            state = self.read_state()
            state.update(copy.deepcopy(values))
            self.write_state(state)
            return state

    def append_mutation(self, entry: dict[str, Any]) -> None:
        with file_lock(self.lock_path):
            state = self.read_state()
            log = list(state.get("mutations", []))
            log.append(entry)
            state["mutations"] = log[-200:]
            self.write_state(state)

    def clear_derived(self) -> None:
        shutil.rmtree(self.stages_dir, ignore_errors=True)
        shutil.rmtree(self.checkpoints_dir, ignore_errors=True)
        shutil.rmtree(self.exports_dir, ignore_errors=True)
        for directory in (self.stages_dir, self.exports_dir, self.checkpoints_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.checkpoints = CheckpointStore(self.checkpoints_dir)

    def clone_derived_to(self, destination: Workspace) -> None:
        for name in ("stages", "checkpoints", "exports"):
            src = self.root / name
            dst = destination.root / name
            if src.exists():
                shutil.copytree(src, dst, dirs_exist_ok=True)
        destination.checkpoints = CheckpointStore(destination.checkpoints_dir)

