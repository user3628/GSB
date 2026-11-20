"""Local FastAPI service for streaming synthesis and experiment control."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from .config import load_config
from .engine import StreamingTTSEngine
from .errors import TTSError
from .inputs import parse_samples, samples_to_jsonl
from .utils import atomic_write_text, read_json, sha256_text


class SamplesImport(BaseModel):
    jsonl: str
    append: bool = False
    filename: str = "samples.jsonl"


class SplitSettings(BaseModel):
    first_chunk_min_chars: int | None = None
    subsequent_chunk_chars: int | None = None
    punctuation_forced: bool | None = None
    max_budget_splits: int | None = None


class BudgetSettings(BaseModel):
    first_packet_ms: float | None = None
    continuation_ms: float | None = None
    max_workers: int | None = None
    max_preprocess_depth: int | None = None


class RunRequest(BaseModel):
    run_id: str = "main"
    samples_file: str | None = None
    resume: bool = False
    stop_after_chunks: int | None = Field(default=None, ge=1)
    wait: bool = False


class RunControl(BaseModel):
    run_id: str = "main"


class JobManager:
    def __init__(self, *, workspace: Path, config_path: Path):
        self.workspace = workspace
        self.config_path = config_path
        self.lock = threading.Lock()
        self.threads: dict[str, threading.Thread] = {}
        self.errors: dict[str, dict[str, Any]] = {}

    def engine(self, run_id: str) -> StreamingTTSEngine:
        return StreamingTTSEngine(workspace=self.workspace, config_path=self.config_path, run_id=run_id)

    def is_running(self, run_id: str) -> bool:
        thread = self.threads.get(run_id)
        return bool(thread and thread.is_alive())

    def start(
        self,
        *,
        run_id: str,
        samples_file: str | None,
        resume: bool,
        stop_after_chunks: int | None,
        wait: bool,
    ) -> dict[str, Any]:
        with self.lock:
            if self.is_running(run_id):
                raise TTSError(
                    "E_RUN_ACTIVE",
                    "a run with this run_id is already active",
                    {"run_id": run_id},
                )
            self.errors.pop(run_id, None)
            samples_path = self._samples_path(samples_file)
            engine = self.engine(run_id)
            if wait:
                outcome = engine.run(
                    samples_path=samples_path,
                    resume=resume,
                    stop_after_chunks=stop_after_chunks,
                )
                return outcome.to_dict()

            def worker() -> None:
                try:
                    engine.run(
                        samples_path=samples_path,
                        resume=resume,
                        stop_after_chunks=stop_after_chunks,
                    )
                except TTSError as exc:
                    self.errors[run_id] = exc.to_dict()["error"]
                except Exception as exc:  # pragma: no cover - defensive API boundary
                    self.errors[run_id] = {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "type": type(exc).__name__,
                    }

            thread = threading.Thread(target=worker, name=f"tts-{run_id}", daemon=True)
            self.threads[run_id] = thread
            thread.start()
            return {"ok": True, "status": "started", "run_id": run_id}

    def _samples_path(self, samples_file: str | None) -> Path:
        if samples_file is None:
            config = load_config(self.config_path, workspace=self.workspace)
            return config.resolved_path("samples")
        candidate = Path(samples_file)
        resolved = (candidate if candidate.is_absolute() else self.workspace / candidate).resolve()
        try:
            resolved.relative_to(self.workspace)
        except ValueError as exc:
            raise TTSError(
                "E_INVALID_INPUT",
                "samples_file must stay inside the workspace",
                {"samples_file": samples_file},
            ) from exc
        return resolved

    def status(self, run_id: str) -> dict[str, Any]:
        engine = self.engine(run_id)
        status = engine.status()
        status["process_active"] = self.is_running(run_id)
        if run_id in self.errors:
            status["error"] = self.errors[run_id]
        return status


def _update_yaml_config(config_path: Path, workspace: Path, section: str, updates: dict[str, Any]) -> dict[str, Any]:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if section not in raw or not isinstance(raw[section], dict):
        raise TTSError("E_INVALID_CONFIG", f"configuration section {section!r} is missing")
    for key, value in updates.items():
        if value is not None:
            raw[section][key] = value
    candidate = yaml.safe_dump(raw, allow_unicode=True, sort_keys=False)
    verify_path = config_path.with_name(config_path.name + ".verify")
    atomic_write_text(verify_path, candidate)
    try:
        load_config(verify_path, workspace=workspace)
    except Exception:
        verify_path.unlink(missing_ok=True)
        raise
    verify_path.unlink(missing_ok=True)
    atomic_write_text(config_path, candidate)
    return raw


def create_app(*, workspace: str | Path, config_path: str | Path) -> FastAPI:
    workspace_path = Path(workspace).resolve()
    config = Path(config_path).resolve()
    manager = JobManager(workspace=workspace_path, config_path=config)
    app = FastAPI(title="Local Deterministic Streaming TTS", version="0.1.0")

    @app.exception_handler(TTSError)
    async def tts_error_handler(_request, exc: TTSError):
        status_code = 409 if exc.code in {"E_RUN_ACTIVE", "E_STALE_CHECKPOINT", "E_INVALID_STAGE"} else 400
        return JSONResponse(status_code=status_code, content=exc.to_dict())

    @app.get("/health")
    def health():
        engine = manager.engine("main")
        return {
            "ok": True,
            "service": "local-stream-tts",
            "config_version": engine.config_version,
            "calibration_unit_ms": engine.calibration["unit_ms"],
            "python_deterministic": True,
        }

    @app.get("/config")
    def get_config():
        engine = manager.engine("main")
        return {"ok": True, "config": engine.config.public_dict(), "config_version": engine.config_version}

    @app.get("/version")
    def version(run_id: str = Query(default="main")):
        engine = manager.engine(run_id)
        try:
            return {"ok": True, **engine.version_report()}
        except TTSError as exc:
            raise HTTPException(status_code=400, detail=exc.to_dict()["error"]) from exc

    @app.post("/samples/import")
    def import_samples(payload: SamplesImport):
        engine = manager.engine("main")
        utterances = parse_samples(payload.jsonl, source="http-import")
        target = workspace_path / "data" / payload.filename
        if target.parent.resolve() != (workspace_path / "data").resolve():
            raise TTSError("E_INVALID_INPUT", "filename must be a plain file name under data/")
        text = samples_to_jsonl(utterances)
        if payload.append and target.exists():
            text = target.read_text(encoding="utf-8") + text
            parse_samples(text, source=str(target))
        atomic_write_text(target, text)
        return {
            "ok": True,
            "path": str(target),
            "utterance_count": len(parse_samples(text)),
            "sha256": sha256_text(text),
        }

    @app.put("/settings/split")
    def set_split(payload: SplitSettings):
        update = _update_yaml_config(config, workspace_path, "split", payload.model_dump(exclude_none=True))
        return {"ok": True, "split": update["split"]}

    @app.put("/settings/budget")
    def set_budget(payload: BudgetSettings):
        update = _update_yaml_config(config, workspace_path, "budget", payload.model_dump(exclude_none=True))
        return {"ok": True, "budget": update["budget"]}

    @app.post("/runs")
    def start_run(payload: RunRequest):
        return manager.start(
            run_id=payload.run_id,
            samples_file=payload.samples_file,
            resume=payload.resume,
            stop_after_chunks=payload.stop_after_chunks,
            wait=payload.wait,
        )

    @app.get("/runs/{run_id}")
    def run_status(run_id: str):
        return {"ok": True, **manager.status(run_id)}

    @app.get("/runs/{run_id}/stages")
    def stage_status(run_id: str):
        engine = manager.engine(run_id)
        return {"ok": True, **engine.stage_report()}

    @app.get("/status")
    def status_alias(run_id: str = Query(default="main")):
        return {"ok": True, **manager.status(run_id)}

    @app.post("/runs/{run_id}/stop")
    def stop_run(run_id: str):
        engine = manager.engine(run_id)
        status = engine.request_stop()
        return {"ok": True, **status}

    @app.post("/stop")
    def stop_alias(payload: RunControl):
        return stop_run(payload.run_id)

    @app.post("/runs/{run_id}/resume")
    def resume_run(run_id: str, payload: RunRequest | None = None):
        request = payload or RunRequest(run_id=run_id)
        return manager.start(
            run_id=run_id,
            samples_file=request.samples_file,
            resume=True,
            stop_after_chunks=request.stop_after_chunks,
            wait=request.wait,
        )

    @app.post("/resume")
    def resume_alias(payload: RunRequest):
        return manager.start(
            run_id=payload.run_id,
            samples_file=payload.samples_file,
            resume=True,
            stop_after_chunks=payload.stop_after_chunks,
            wait=payload.wait,
        )

    def run_file(run_id: str, path: Path, media_type: str):
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"missing {path.name}")
        return PlainTextResponse(path.read_text(encoding="utf-8"), media_type=media_type)

    @app.get("/runs/{run_id}/trace", response_class=PlainTextResponse)
    def get_trace(run_id: str):
        engine = manager.engine(run_id)
        return run_file(run_id, engine.trace_path, "application/x-ndjson")

    @app.get("/runs/{run_id}/timing", response_class=PlainTextResponse)
    def get_timing(run_id: str):
        engine = manager.engine(run_id)
        return run_file(run_id, engine.timing_path, "application/x-ndjson")

    @app.get("/runs/{run_id}/summary")
    def get_summary(run_id: str):
        engine = manager.engine(run_id)
        payload = read_json(engine.summary_path)
        if payload is None:
            raise HTTPException(status_code=404, detail="run summary is not available")
        return payload

    @app.get("/runs/{run_id}/degrade")
    def get_degrade(run_id: str):
        engine = manager.engine(run_id)
        if not engine.trace_path.exists():
            raise HTTPException(status_code=404, detail="trace is not available")
        records = [
            json.loads(line)
            for line in engine.trace_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        degraded = [record for record in records if record.get("degraded")]
        return {
            "ok": True,
            "run_id": run_id,
            "degraded_block_count": len(degraded),
            "events": degraded,
        }

    @app.get("/runs/{run_id}/exports")
    def list_exports(run_id: str):
        engine = manager.engine(run_id)
        return {"ok": True, "run_id": run_id, **engine.export_paths()}

    @app.get("/runs/{run_id}/exports/{utt_id}.wav")
    def get_wav(run_id: str, utt_id: str):
        engine = manager.engine(run_id)
        path = engine.output_root / f"{utt_id}.wav"
        if not path.exists():
            raise HTTPException(status_code=404, detail="WAV is not available")
        return FileResponse(path, media_type="audio/wav", filename=f"{utt_id}.wav")

    return app
