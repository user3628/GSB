"""Stage orchestration: import -> dedup -> score -> sample -> export.

Each stage, when completed, publishes an atomic checkpoint containing the
stage name, its input content fingerprint, its config version fingerprint,
per-file artifact digests and a monotonic sequence number.  Re-running the
pipeline skips stages whose input/config fingerprints still match
(incremental resume); a mismatch invalidates the stage and everything
downstream (incremental invalidation propagation).

Graceful stop: the stop condition is checked after every indivisible
sample unit; when triggered, the current unit is finished and the pipeline
exits without publishing the interrupted stage.  Resuming re-executes the
incomplete stage deterministically, so the final kept list, quality scores
and mixture are byte-identical to a single uninterrupted run.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .checkpoint import (
    STAGES,
    CheckpointRecord,
    CheckpointStore,
    atomic_write,
    compute_version_fingerprint,
)
from .codec import V1ImportRules, decode_sample, encode_sample
from .dedup import DedupConfig, dedup_samples
from .errors import E_DECODE_SAMPLE, E_INVALID_INPUT, E_INVALID_STAGE, WorkbenchError
from .models import InternalSample
from .quality import ScoringConfig, score_samples
from .sampling import SamplingConfig, stratified_mixture
from .util import canonical_bytes, digest_obj, sha256_hex

logger = logging.getLogger("sft_workbench.pipeline")

STAGE_ARTIFACTS: dict[str, list[str]] = {
    "import": ["samples.internal.jsonl", "import_errors.json"],
    "dedup": ["kept.jsonl", "dedup_summary.json"],
    "score": ["quality.json"],
    "sample": ["allocation.json"],
    "export": ["mixture.jsonl", "export_summary.json"],
}

STAGE_INPUTS: dict[str, list[str]] = {
    "import": [],
    "dedup": ["samples.internal.jsonl"],
    "score": ["kept.jsonl"],
    "sample": ["kept.jsonl", "quality.json"],
    "export": ["kept.jsonl", "allocation.json"],
}


class GracefulStop(Exception):
    """Raised after the current indivisible sample unit when stopping."""


@dataclass
class PipelineConfig:
    v1_rules: V1ImportRules = field(default_factory=V1ImportRules)
    dedup: DedupConfig = field(default_factory=DedupConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    sampling: SamplingConfig = field(default_factory=SamplingConfig)
    export_version: int = 2

    def stage_config(self, stage: str) -> dict[str, Any]:
        if stage == "import":
            return {"v1_rules": self.v1_rules.to_dict()}
        if stage == "dedup":
            return {"dedup": self.dedup.to_dict()}
        if stage == "score":
            return {"scoring": self.scoring.to_dict()}
        if stage == "sample":
            return {"sampling": self.sampling.to_dict()}
        if stage == "export":
            return {"export_version": self.export_version}
        raise ValueError(f"unknown stage: {stage}")


class Pipeline:
    def __init__(
        self,
        workspace: str | Path,
        config: PipelineConfig | None = None,
        stop_checker: Callable[[], bool] | None = None,
    ):
        self.workspace = Path(workspace)
        self.config = config or PipelineConfig()
        self.store = CheckpointStore(self.workspace)
        self.sources_path = self.workspace / "sources.json"
        self.stop_path = self.workspace / "STOP"
        self._stop_checker = stop_checker

    # ------------------------------------------------------------ stop flag
    def request_stop(self) -> None:
        self.stop_path.touch(exist_ok=True)

    def clear_stop(self) -> None:
        if self.stop_path.exists():
            self.stop_path.unlink()

    def _should_stop(self) -> bool:
        if self._stop_checker is not None:
            return bool(self._stop_checker())
        return self.stop_path.exists()

    def _check_stop(self) -> None:
        if self._should_stop():
            raise GracefulStop("stop requested; finishing current unit and halting")

    # -------------------------------------------------------------- helpers
    def _artifact_path(self, name: str) -> Path:
        return self.store.artifacts_dir / name

    def _write_artifact(self, name: str, payload: Any) -> str:
        if isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        else:
            data = canonical_bytes(payload) + b"\n"
        atomic_write(self._artifact_path(name), data)
        return sha256_hex(data)

    def _read_artifact(self, name: str) -> Any:
        with open(self._artifact_path(name), "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))

    def _read_internal_samples(self, name: str) -> list[InternalSample]:
        samples = []
        with open(self._artifact_path(name), "rb") as handle:
            for line in handle.read().decode("utf-8").splitlines():
                if line.strip():
                    samples.append(InternalSample.from_dict(json.loads(line)))
        return samples

    def _config_fingerprint(self, stage: str) -> str:
        return digest_obj(self.config.stage_config(stage))

    def _input_fingerprint(self, stage: str) -> str:
        if stage == "import":
            sources = self._load_sources(required=False)
            entries = []
            for path in sources:
                p = Path(path)
                if not p.exists():
                    raise WorkbenchError(
                        E_INVALID_INPUT,
                        f"input shard is missing: {path}",
                        {"shard": str(path)},
                    )
                entries.append({"path": str(path), "sha256": sha256_hex(p.read_bytes())})
            return digest_obj({"sources": entries})
        digests = {}
        for name in STAGE_INPUTS[stage]:
            path = self._artifact_path(name)
            if not path.exists():
                return ""  # prerequisite missing; fingerprint cannot be computed
            digests[name] = sha256_hex(path.read_bytes())
        return digest_obj(digests)

    def _check_prerequisites(self, stage: str) -> None:
        if stage == "import":
            self._load_sources(required=True)
            return
        for name in STAGE_INPUTS[stage]:
            if not self._artifact_path(name).exists():
                raise WorkbenchError(
                    E_INVALID_STAGE,
                    f"stage '{stage}' requires artifact '{name}' from an earlier stage",
                    {"stage": stage, "missing_artifact": name},
                )

    def _load_sources(self, required: bool) -> list[str]:
        if not self.sources_path.exists():
            if required:
                raise WorkbenchError(
                    E_INVALID_STAGE,
                    "no input shards registered for the import stage",
                    {"stage": "import"},
                )
            return []
        with open(self.sources_path, "rb") as handle:
            data = json.loads(handle.read().decode("utf-8"))
        return list(data.get("shards", []))

    def _save_sources(self, shard_paths: list[str]) -> None:
        ordered = sorted(str(p) for p in shard_paths)
        atomic_write(self.sources_path, canonical_bytes({"shards": ordered}) + b"\n")

    # ------------------------------------------------------------------ run
    def run(self, shard_paths: list[str] | None = None, until: str = "export") -> dict[str, Any]:
        if until not in STAGES:
            raise WorkbenchError(
                E_INVALID_STAGE,
                f"unknown stage: {until}",
                {"stage": until, "known_stages": list(STAGES)},
            )
        self.store.verify_or_raise()
        if shard_paths is not None:
            self._save_sources(shard_paths)
        executed: list[dict[str, str]] = []
        try:
            for stage in STAGES:
                if STAGES.index(stage) > STAGES.index(until):
                    break
                input_fp = self._input_fingerprint(stage)
                config_fp = self._config_fingerprint(stage)
                existing = self.store.load(stage)
                if (
                    existing is not None
                    and existing.input_fingerprint == input_fp
                    and existing.config_fingerprint == config_fp
                ):
                    executed.append({"stage": stage, "status": "cached"})
                    continue
                if existing is not None:
                    removed = self.store.invalidate_from(stage)
                    logger.info("invalidated stages: %s", removed)
                self._check_prerequisites(stage)
                self._check_stop()
                artifacts = self._execute(stage)
                record = self._make_record(stage, input_fp, config_fp, artifacts)
                self.store.publish(record)
                executed.append({"stage": stage, "status": "executed"})
        except GracefulStop:
            logger.info("graceful stop during pipeline run")
            return {"status": "stopped", "stages": executed}
        head = self.store.head() or {}
        return {
            "status": "completed",
            "stages": executed,
            "version_fingerprint": head.get("version_fingerprint", ""),
        }

    def resume(self, until: str = "export") -> dict[str, Any]:
        self.clear_stop()
        return self.run(shard_paths=None, until=until)

    # -------------------------------------------------------------- execute
    def _execute(self, stage: str) -> dict[str, str]:
        handler = {
            "import": self._execute_import,
            "dedup": self._execute_dedup,
            "score": self._execute_score,
            "sample": self._execute_sample,
            "export": self._execute_export,
        }[stage]
        return handler()

    def _execute_import(self) -> dict[str, str]:
        sources = self._load_sources(required=True)
        samples: list[InternalSample] = []
        errors: list[dict[str, Any]] = []
        for path in sources:
            p = Path(path)
            if not p.exists():
                raise WorkbenchError(
                    E_INVALID_INPUT,
                    f"input shard is missing: {path}",
                    {"shard": str(path)},
                )
            with open(p, "rb") as handle:
                lines = handle.read().decode("utf-8").splitlines()
            for lineno, line in enumerate(lines, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    err = WorkbenchError(
                        E_DECODE_SAMPLE,
                        f"invalid JSON payload: {exc.msg}",
                        {"sample_id": None, "field": "$", "shard": str(path), "line": lineno},
                    )
                    err.log()
                    errors.append(err.to_dict()["error"])
                    continue
                try:
                    samples.append(decode_sample(raw, self.config.v1_rules))
                except WorkbenchError as err:
                    err.log()
                    errors.append(err.to_dict()["error"])
                self._check_stop()
        lines = [json.dumps(s.to_dict(), ensure_ascii=False, sort_keys=True) for s in samples]
        payload = "".join(line + "\n" for line in lines).encode("utf-8")
        digests = {
            "samples.internal.jsonl": self._write_artifact("samples.internal.jsonl", payload),
            "import_errors.json": self._write_artifact(
                "import_errors.json", {"imported": len(samples), "errors": errors}
            ),
        }
        return digests

    def _execute_dedup(self) -> dict[str, str]:
        samples = self._read_internal_samples("samples.internal.jsonl")
        result = dedup_samples(samples, self.config.dedup, should_stop=self._should_stop)
        lines = [json.dumps(s.to_dict(), ensure_ascii=False, sort_keys=True) for s in result.kept]
        payload = "".join(line + "\n" for line in lines).encode("utf-8")
        return {
            "kept.jsonl": self._write_artifact("kept.jsonl", payload),
            "dedup_summary.json": self._write_artifact("dedup_summary.json", result.summary()),
        }

    def _execute_score(self) -> dict[str, str]:
        kept = self._read_internal_samples("kept.jsonl")
        records = score_samples(kept, self.config.scoring, should_stop=self._should_stop)
        return {"quality.json": self._write_artifact("quality.json", {"scores": records})}

    def _execute_sample(self) -> dict[str, str]:
        kept = self._read_internal_samples("kept.jsonl")
        quality = self._read_artifact("quality.json")
        mixture = stratified_mixture(kept, quality["scores"], self.config.sampling)
        return {"allocation.json": self._write_artifact("allocation.json", mixture)}

    def _execute_export(self) -> dict[str, str]:
        kept = self._read_internal_samples("kept.jsonl")
        allocation = self._read_artifact("allocation.json")
        by_id = {s.sample_id: s for s in kept}
        lines = []
        for sample_id in allocation["mixture"]:
            sample = by_id[sample_id]
            encoded = encode_sample(sample, self.config.export_version)
            lines.append(json.dumps(encoded, ensure_ascii=False, sort_keys=True))
        payload = "".join(line + "\n" for line in lines).encode("utf-8")
        digests = {
            "mixture.jsonl": self._write_artifact("mixture.jsonl", payload),
        }
        summary = {
            "count": len(lines),
            "export_version": self.config.export_version,
            "mixture_digest": digests["mixture.jsonl"],
        }
        digests["export_summary.json"] = self._write_artifact("export_summary.json", summary)
        return digests

    # --------------------------------------------------------------- record
    def _make_record(
        self, stage: str, input_fp: str, config_fp: str, artifacts: dict[str, str]
    ) -> CheckpointRecord:
        head = self.store.head()
        seq = int(head["seq"]) + 1 if head else 1
        prev_fingerprint = head["version_fingerprint"] if head else ""
        stage_digests = {r.stage: r.artifact_digest for r in self.store.list_records()}
        artifact_digest = digest_obj(artifacts)
        stage_digests[stage] = artifact_digest
        version_fingerprint = compute_version_fingerprint(stage_digests, config_fp, prev_fingerprint)
        return CheckpointRecord(
            stage=stage,
            seq=seq,
            input_fingerprint=input_fp,
            config_fingerprint=config_fp,
            artifacts=dict(artifacts),
            artifact_digest=artifact_digest,
            version_fingerprint=version_fingerprint,
            prev_fingerprint=prev_fingerprint,
        )
