"""Engine orchestration: planning, incremental rebuilds, stop/resume, verify.

Every stage of every tile is content-addressed by a fingerprint over
(tile id, stage, engine version, upstream content digest, stage config).
Planning walks the four stages in order; the first stage whose fingerprint
no longer matches its checkpoint (or whose artifacts fail verification) is
the earliest invalidated stage, and it plus all downstream stages are
rebuilt. Unaffected upstream artifacts keep being reused.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from . import ENGINE_VERSION, STAGES
from .checkpoints import CheckpointStore
from .config import WorkspaceConfig
from .errors import IncrementalMismatchError, WorkspaceError
from .stages import common as stage_io
from .stages.classify import run_classify
from .stages.instances import run_instances
from .stages.postprocess import run_postprocess
from .stages.quantize import build_summary, write_semantic_mask
from .util import (atomic_write_text, canonical_json, pretty_json, sha256_file,
                   sha256_text)

QUANTIZE_OUTPUTS = ("semantic.png", "instance.png", "summary.json")


def stage_fingerprint(tile_id: str, stage: str, input_fingerprint: str,
                      config_version: str) -> str:
    payload = {
        "engine": ENGINE_VERSION,
        "tile_id": tile_id,
        "stage": stage,
        "input": input_fingerprint,
        "config": config_version,
    }
    return sha256_text(canonical_json(payload))


class Engine:
    def __init__(self, workspace):
        self.workspace = Path(workspace)
        self.store = CheckpointStore(self.workspace)

    # ------------------------------------------------------------------ paths
    @property
    def config_path(self) -> Path:
        return self.workspace / "config.json"

    @property
    def state_dir(self) -> Path:
        return self.workspace / "state"

    @property
    def stop_file(self) -> Path:
        return self.state_dir / "STOP"

    @property
    def last_run_path(self) -> Path:
        return self.state_dir / "last_run.json"

    def artifacts_dir(self, tile_id: str, stage: str) -> Path:
        return self.workspace / "artifacts" / tile_id / stage

    # ------------------------------------------------------------------ config
    def load_config(self) -> WorkspaceConfig:
        return WorkspaceConfig.load(self.config_path)

    # ------------------------------------------------------------- fingerprints
    def image_fingerprint(self, tile) -> str:
        image_path = self.workspace / tile.image
        if not image_path.exists():
            raise WorkspaceError(
                f"tile {tile.tile_id}: image not found: {image_path}",
                {"tile_id": tile.tile_id, "image": tile.image},
            )
        return sha256_file(image_path)

    def _verify_artifacts(self, record) -> bool:
        stage_dir = self.artifacts_dir(record.tile_id, record.stage)
        for name, digest in sorted(record.artifacts.items()):
            path = stage_dir / name
            if not path.exists() or sha256_file(path) != digest:
                return False
        return True

    # ------------------------------------------------------------------ planning
    def inspect_tile(self, tile) -> dict:
        """Read-only plan: per-stage status, fingerprints and invalidation reason."""
        image_fp = self.image_fingerprint(tile)
        latest = self.store.latest_by_stage(tile.tile_id)
        stages = {}
        upstream = image_fp
        invalidated = False
        for stage in STAGES:
            config_version = tile.stage_config_version(stage)
            desired_fp = stage_fingerprint(tile.tile_id, stage, upstream, config_version)
            record = latest.get(stage)
            status, reason = "valid", None
            if invalidated:
                status, reason = "invalid", "upstream_invalidated"
                desired_fp = None  # depends on not-yet-rebuilt upstream output
            elif record is None:
                status, reason = "invalid", "no_checkpoint"
            elif not self._verify_artifacts(record):
                status, reason = "invalid", "artifact_mismatch"
            elif record.fingerprint != desired_fp:
                if record.input_fingerprint != upstream:
                    status, reason = "invalid", "input_changed"
                else:
                    status, reason = "invalid", "config_changed"
            if status == "valid":
                upstream = record.artifact_digest
            else:
                invalidated = True
            stages[stage] = {
                "status": status,
                "reason": reason,
                "desired_fingerprint": desired_fp,
                "checkpoint_fingerprint": record.fingerprint if record else None,
                "config_version": config_version,
                "seq": record.seq if record else None,
            }
        return {"tile_id": tile.tile_id, "image_fingerprint": image_fp, "stages": stages}

    def inspect(self, tile_ids=None) -> dict:
        config = self.load_config()
        tiles = self._select_tiles(config, tile_ids)
        return {tile.tile_id: self.inspect_tile(tile) for tile in tiles}

    @staticmethod
    def _select_tiles(config, tile_ids):
        if tile_ids is None:
            return list(config.tiles)
        return [config.tile(tid) for tid in tile_ids]

    # ------------------------------------------------------------------ stop flag
    def request_stop(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(self.stop_file, "stop\n")

    def clear_stop(self) -> None:
        try:
            self.stop_file.unlink()
        except FileNotFoundError:
            pass

    @property
    def stop_requested(self) -> bool:
        return self.stop_file.exists()

    # ------------------------------------------------------------------ stage exec
    def _run_stage(self, tile, stage: str) -> dict:
        """Execute one stage; returns {file_name: sha256} for its artifacts."""
        stage_dir = self.artifacts_dir(tile.tile_id, stage)
        stage_dir.mkdir(parents=True, exist_ok=True)
        artifacts = {}

        if stage == "classify":
            image = stage_io.read_rgb(self.workspace / tile.image)
            labels = run_classify(image, tile)
            stage_io.write_label_map(stage_dir / "labels.png", labels)
            artifacts["labels.png"] = sha256_file(stage_dir / "labels.png")

        elif stage == "instances":
            labels = stage_io.read_label_map(self.artifacts_dir(tile.tile_id, "classify") / "labels.png")
            mask, items = run_instances(labels, tile.connectivity, tile.min_area)
            stage_io.write_instance_mask(stage_dir / "instances.png", mask)
            atomic_write_text(stage_dir / "instances.json", pretty_json({"instances": items}))
            artifacts["instances.png"] = sha256_file(stage_dir / "instances.png")
            artifacts["instances.json"] = sha256_file(stage_dir / "instances.json")

        elif stage == "postprocess":
            labels = stage_io.read_label_map(self.artifacts_dir(tile.tile_id, "classify") / "labels.png")
            refined = run_postprocess(labels, tile)
            stage_io.write_label_map(stage_dir / "labels.png", refined)
            artifacts["labels.png"] = sha256_file(stage_dir / "labels.png")

        elif stage == "quantize":
            refined = stage_io.read_label_map(self.artifacts_dir(tile.tile_id, "postprocess") / "labels.png")
            mask = stage_io.read_instance_mask(self.artifacts_dir(tile.tile_id, "instances") / "instances.png")
            import json
            with open(self.artifacts_dir(tile.tile_id, "instances") / "instances.json",
                      "r", encoding="utf-8") as handle:
                items = json.load(handle)["instances"]
            write_semantic_mask(stage_dir / "semantic.png", refined, tile)
            stage_io.write_instance_mask(stage_dir / "instance.png", mask)
            summary = build_summary(tile, refined, items)
            atomic_write_text(stage_dir / "summary.json", pretty_json(summary))
            for name in QUANTIZE_OUTPUTS:
                artifacts[name] = sha256_file(stage_dir / name)

        else:  # pragma: no cover - guarded by STAGES
            raise WorkspaceError(f"unknown stage {stage!r}")
        return artifacts

    @staticmethod
    def _manifest_digest(artifacts: dict) -> str:
        return sha256_text(canonical_json({"files": artifacts}))

    # ------------------------------------------------------------------ run
    def run(self, tile_ids=None, stop_after_units=None, honor_stop=True) -> dict:
        """Incrementally bring the selected tiles up to date.

        Stop requests are honored between indivisible stage units: the current
        unit always completes and is checkpointed before the engine exits.
        ``stop_after_units`` deterministically simulates a stop request after
        that many completed units (used by tests and the demo).
        """
        config = self.load_config()
        tiles = self._select_tiles(config, tile_ids)
        self.clear_stop()
        report = {"stopped": False, "units_completed": 0, "tiles": {}}

        for tile in tiles:
            tile_report = self._run_tile(tile, report, stop_after_units, honor_stop)
            report["tiles"][tile.tile_id] = tile_report
            if report["stopped"]:
                break

        atomic_write_text(self.last_run_path, pretty_json(report))
        return report

    def _run_tile(self, tile, report: dict, stop_after_units, honor_stop) -> dict:
        image_fp = self.image_fingerprint(tile)
        latest = self.store.latest_by_stage(tile.tile_id)
        upstream = image_fp
        tile_report = {"stages": {}}

        # Phase 1: walk the chain and reuse every still-valid prefix stage.
        # The first stage whose checkpoint fingerprint does not match (or whose
        # artifacts fail verification) invalidates itself and ALL downstream
        # stages; downstream stages are rebuilt even if their content would
        # not change, per the invalidation-propagation contract.
        invalid_from = None
        for stage in STAGES:
            config_version = tile.stage_config_version(stage)
            desired_fp = stage_fingerprint(tile.tile_id, stage, upstream, config_version)
            record = latest.get(stage)
            reusable = (
                record is not None
                and record.fingerprint == desired_fp
                and self._verify_artifacts(record)
            )
            if not reusable:
                invalid_from = STAGES.index(stage)
                first_reason = self._invalid_reason(record, desired_fp, upstream)
                break
            tile_report["stages"][stage] = {
                "status": "reused", "reason": None,
                "seq": record.seq, "fingerprint": record.fingerprint,
            }
            upstream = record.artifact_digest

        if invalid_from is None:
            return tile_report

        # Phase 2: rebuild the invalidated chain (first invalid stage + all
        # downstream stages), checkpointing each completed unit.
        for offset, stage in enumerate(STAGES[invalid_from:]):
            if honor_stop and self.stop_requested:
                report["stopped"] = True
                tile_report["stages"][stage] = {"status": "pending", "reason": "stop_requested"}
                return tile_report
            reason = first_reason if offset == 0 else "upstream_invalidated"
            artifacts = self._run_stage(tile, stage)
            artifact_digest = self._manifest_digest(artifacts)
            config_version = tile.stage_config_version(stage)
            desired_fp = stage_fingerprint(tile.tile_id, stage, upstream, config_version)
            published = self.store.publish(
                tile.tile_id, stage,
                fingerprint=desired_fp,
                input_fingerprint=upstream,
                config_version=config_version,
                artifact_digest=artifact_digest,
                artifacts=artifacts,
            )
            upstream = artifact_digest
            report["units_completed"] += 1
            tile_report["stages"][stage] = {
                "status": "rebuilt", "reason": reason,
                "seq": published.seq, "fingerprint": published.fingerprint,
            }
            if stop_after_units is not None and report["units_completed"] >= stop_after_units:
                report["stopped"] = True
                return tile_report
        return tile_report

    @staticmethod
    def _invalid_reason(record, desired_fp, upstream) -> str:
        if record is None:
            return "no_checkpoint"
        if record.input_fingerprint != upstream:
            return "input_changed"
        if record.fingerprint != desired_fp:
            return "config_changed"
        return "artifact_mismatch"

    # ------------------------------------------------------------------ verify
    def verify(self, tile_ids=None) -> dict:
        """Byte-compare incremental outputs against a clean from-scratch build.

        The baseline full build runs in an isolated temporary workspace; the
        incremental workspace never writes into it. On any difference an
        E_INCREMENTAL_MISMATCH error is raised with the differing file list.
        """
        config = self.load_config()
        tiles = self._select_tiles(config, tile_ids)
        self.run([tile.tile_id for tile in tiles])  # make sure incremental state is current

        tmp = Path(tempfile.mkdtemp(prefix="incseg-verify-", dir=str(self._ensure_state_dir())))
        try:
            shutil.copyfile(self.config_path, tmp / "config.json")
            shutil.copytree(self.workspace / "tiles", tmp / "tiles")
            fresh = Engine(tmp)
            fresh.run([tile.tile_id for tile in tiles], honor_stop=False)

            diffs = []
            checked = []
            for tile in tiles:
                for name in QUANTIZE_OUTPUTS:
                    rel = f"{tile.tile_id}/quantize/{name}"
                    incremental = self.artifacts_dir(tile.tile_id, "quantize") / name
                    baseline = fresh.artifacts_dir(tile.tile_id, "quantize") / name
                    checked.append(rel)
                    if not incremental.exists():
                        diffs.append(f"{rel}: missing incremental artifact")
                    elif not baseline.exists():
                        diffs.append(f"{rel}: missing baseline artifact")
                    elif sha256_file(incremental) != sha256_file(baseline):
                        diffs.append(f"{rel}: digest mismatch")
            if diffs:
                raise IncrementalMismatchError(
                    "incremental outputs differ from a clean full build",
                    {"files": diffs, "checked": checked},
                )
            return {"status": "ok", "tiles": [t.tile_id for t in tiles], "checked_files": checked}
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def _ensure_state_dir(self) -> Path:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        return self.state_dir

    # ------------------------------------------------------------------ export
    def export_tile(self, tile_id: str, out_dir) -> dict:
        out_dir = Path(out_dir) / tile_id
        out_dir.mkdir(parents=True, exist_ok=True)
        stage_dir = self.artifacts_dir(tile_id, "quantize")
        exported = {}
        for name in QUANTIZE_OUTPUTS:
            src = stage_dir / name
            if not src.exists():
                raise WorkspaceError(
                    f"tile {tile_id}: quantize output {name} not found; run the pipeline first",
                    {"tile_id": tile_id, "file": name},
                )
            dst = out_dir / name
            shutil.copyfile(src, dst)
            exported[name] = str(dst)
        return exported
