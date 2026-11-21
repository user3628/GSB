"""Local-only HTTP workbench built on the Python standard library."""

from __future__ import annotations

import copy
import json
import re
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from .config import Config
from .data import (
    load_manifest,
    load_rows,
    register_manifest_observation,
    validate_manifest_truth,
    validate_rows,
)
from .errors import ErrorCode, PipelineError
from .pipeline import Pipeline
from .util import atomic_write_bytes, canonical_json_line, ensure_relative_path, resolve_under


JOB_PATH = re.compile(r"^/api/jobs/(\d+)(?:/(stop|resume))?$")


@dataclass
class Job:
    id: int
    config_path: str
    injection: str | None
    worker_override: int | None
    execution_mode: str | None
    stop_after_epochs: int | None
    resume_of: int | None = None
    status: str = "running"
    stop_requested: bool = False
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    stop_event: threading.Event = field(default_factory=threading.Event, repr=False)

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "config_path": self.config_path,
            "injection": self.injection,
            "worker_override": self.worker_override,
            "execution_mode": self.execution_mode,
            "stop_after_epochs": self.stop_after_epochs,
            "resume_of": self.resume_of,
            "stop_requested": self.stop_requested,
            "result": self.result,
            "error": self.error,
        }


class Workbench:
    def __init__(self, base_dir: str | Path, config_path: str = "configs/default.yaml"):
        self.base_dir = Path(base_dir).resolve()
        self.default_config_path = config_path
        self.lock = threading.RLock()
        self.jobs: dict[int, Job] = {}
        self.next_job_id = 1
        self.active_job_id: int | None = None

    def _config_path(self, value: str | None) -> str:
        candidate = value or self.default_config_path
        return ensure_relative_path(candidate, "config_path")

    def import_workspace(self, body: dict[str, Any]) -> dict[str, Any]:
        config_path = self._config_path(body.get("config_path"))
        config = Config.load(config_path, self.base_dir)
        manifest_override = body.get("manifest_path")
        raw = copy.deepcopy(config.raw)
        if manifest_override:
            raw["paths"]["manifest"] = ensure_relative_path(str(manifest_override), "manifest_path")
        runtime_config = self.base_dir / "runtime" / "imported-config.yaml"
        payload = yaml.safe_dump(raw, sort_keys=True, allow_unicode=True).encode("utf-8")
        atomic_write_bytes(runtime_config, payload)
        config = Config.load(runtime_config, self.base_dir)
        manifest = load_manifest(resolve_under(self.base_dir, config.manifest_path, "paths.manifest"))
        observation = register_manifest_observation(manifest, config.state_root)
        shards = load_rows(manifest)
        report = validate_rows(shards)
        if report.label_errors or report.funnel_violations:
            raise PipelineError(
                ErrorCode.E_FUNNEL_VIOLATION,
                "imported shards failed row/funnel validation",
                stage="funnel",
                details={
                    "label_errors": report.label_errors,
                    "funnel_violations": report.funnel_violations,
                },
            )
        validate_manifest_truth(shards, report)
        relative_config = runtime_config.relative_to(self.base_dir).as_posix()
        with self.lock:
            self.default_config_path = relative_config
        return {
            "status": "imported",
            "config_path": relative_config,
            "config_version": config.version,
            "manifest_sha256": manifest.digest,
            "observation_path": observation.relative_to(self.base_dir).as_posix(),
            "shards": len(manifest.shards),
            "rows": sum(shard.rows for shard in manifest.shards),
        }

    def start_job(
        self,
        *,
        config_path: str | None,
        injection: str | None,
        worker_override: int | None,
        execution_mode: str | None,
        stop_after_epochs: int | None,
        resume_of: int | None = None,
    ) -> Job:
        if worker_override is not None:
            if isinstance(worker_override, bool) or not isinstance(worker_override, int) or worker_override <= 0:
                raise PipelineError(
                    ErrorCode.E_INVALID_PARAM,
                    "worker_override must be a positive integer",
                    details={"worker_override": worker_override},
                )
        if stop_after_epochs is not None:
            if (
                isinstance(stop_after_epochs, bool)
                or not isinstance(stop_after_epochs, int)
                or stop_after_epochs < 0
            ):
                raise PipelineError(
                    ErrorCode.E_INVALID_PARAM,
                    "stop_after_epochs must be a non-negative integer",
                    details={"stop_after_epochs": stop_after_epochs},
                )
        if execution_mode not in {None, "sequential", "parallel"}:
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "execution_mode must be sequential or parallel",
                details={"execution_mode": execution_mode},
            )
        with self.lock:
            if self.active_job_id is not None:
                active = self.jobs.get(self.active_job_id)
                if active and active.status in {"running", "stopping"}:
                    raise PipelineError(
                        ErrorCode.E_JOB_ACTIVE,
                        "another pipeline job is active",
                        details={"active_job_id": active.id, "status": active.status},
                    )
            job = Job(
                id=self.next_job_id,
                config_path=self._config_path(config_path),
                injection=injection,
                worker_override=worker_override,
                execution_mode=execution_mode,
                stop_after_epochs=stop_after_epochs,
                resume_of=resume_of,
            )
            self.next_job_id += 1
            self.jobs[job.id] = job
            self.active_job_id = job.id
            thread = threading.Thread(target=self._run_job, args=(job,), name=f"ctr-cvr-job-{job.id}")
            thread.start()
            return job

    def _run_job(self, job: Job) -> None:
        try:
            result = Pipeline(
                job.config_path,
                base_dir=self.base_dir,
                injection=job.injection,
                stop_event=job.stop_event,
                stop_after_epochs=job.stop_after_epochs,
                worker_override=job.worker_override,
                execution_mode=job.execution_mode,
            ).run()
            job.result = result
            job.status = "stopped" if result.get("status") == "stopped" else "completed"
        except PipelineError as exc:
            job.error = exc.to_dict()
            job.status = "failed"
        except BaseException as exc:  # pragma: no cover - defensive job boundary
            job.error = {
                "code": ErrorCode.E_WORKER_FAILED,
                "message": str(exc),
                "stage": "job",
                "shard_id": None,
                "row_number": None,
                "impression_id": None,
                "details": {"error_type": type(exc).__name__},
            }
            job.status = "failed"
        finally:
            with self.lock:
                if self.active_job_id == job.id:
                    self.active_job_id = None

    def get_job(self, job_id: int) -> Job:
        with self.lock:
            job = self.jobs.get(job_id)
        if job is None:
            raise PipelineError(
                ErrorCode.E_NOT_FOUND,
                "job not found",
                details={"job_id": job_id},
            )
        return job

    def stop_job(self, job_id: int) -> dict[str, Any]:
        job = self.get_job(job_id)
        with self.lock:
            if job.status in {"running", "stopping"}:
                job.stop_requested = True
                job.stop_event.set()
                job.status = "stopping"
                return {
                    "status": "stopping",
                    "job_id": job.id,
                    "note": "cooperative stop is checked at the next epoch boundary; no computation is force-killed",
                }
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            "job is not stoppable in its current state",
            details={"job_id": job.id, "status": job.status},
        )

    def resume_job(self, job_id: int) -> Job:
        job = self.get_job(job_id)
        if job.status != "stopped":
            raise PipelineError(
                ErrorCode.E_INVALID_PARAM,
                "only a cooperatively stopped job can be resumed",
                details={"job_id": job.id, "status": job.status},
            )
        return self.start_job(
            config_path=job.config_path,
            injection=None,
            worker_override=job.worker_override,
            execution_mode=job.execution_mode,
            stop_after_epochs=None,
            resume_of=job.id,
        )

    def status(self, config_path: str | None) -> dict[str, Any]:
        return Pipeline.status(self._config_path(config_path), base_dir=self.base_dir)

    def losses(self, config_path: str | None, *, include_traversal: bool = False) -> dict[str, Any]:
        path, _config = self._artifact_path(config_path, "train", "metrics.json")
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                item = json.loads(line)
                if not include_traversal:
                    traversal = item.get("sample_traversal", {})
                    item["traversal_digest"] = traversal.get("digest")
                    item.pop("sample_traversal", None)
                records.append(item)
        return {"metrics_path": path.relative_to(self.base_dir).as_posix(), "epochs": len(records), "losses": records}

    def funnel_diagnostics(self, config_path: str | None) -> dict[str, Any]:
        config = Config.load(self._config_path(config_path), self.base_dir)
        path = config.outputs_root / "funnel" / "diagnostics.json"
        if not path.exists():
            path = config.outputs_root / "funnel" / "label-diagnostics.json"
        if not path.exists():
            raise PipelineError(ErrorCode.E_NOT_FOUND, "funnel diagnostics are not available", stage="funnel")
        return json.loads(path.read_text(encoding="utf-8"))

    def summary(self, config_path: str | None) -> dict[str, Any]:
        path, _ = self._artifact_path(config_path, "experiment_summary.json")
        return json.loads(path.read_text(encoding="utf-8"))

    def model(self, config_path: str | None) -> dict[str, Any]:
        path, _ = self._artifact_path(config_path, "train", "model.json")
        return json.loads(path.read_text(encoding="utf-8"))

    def scores(self, config_path: str | None, *, limit: int = 0) -> dict[str, Any]:
        path, _ = self._artifact_path(config_path, "infer", "scores.jsonl")
        records: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))
                    if limit and len(records) >= limit:
                        break
        return {"path": path.relative_to(self.base_dir).as_posix(), "records": records}

    def _artifact_path(self, config_path: str | None, *parts: str) -> tuple[Path, Config]:
        config = Config.load(self._config_path(config_path), self.base_dir)
        path = config.outputs_root.joinpath(*parts)
        if not path.exists():
            raise PipelineError(
                ErrorCode.E_NOT_FOUND,
                "requested artifact is not available",
                details={"path": path.as_posix()},
            )
        return path, config


class WorkbenchHandler(BaseHTTPRequestHandler):
    server_version = "CTRCVRWorkbench/1.0"

    @property
    def workbench(self) -> Workbench:
        return self.server.workbench  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: Any) -> None:
        print(f"http {self.address_string()} {format % args}")

    def _send_json(self, status: int, payload: Any) -> None:
        data = canonical_json_line(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_html(self) -> None:
        path = Path(__file__).parent / "static" / "index.html"
        data = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, exc: PipelineError, status: int = HTTPStatus.BAD_REQUEST) -> None:
        self._send_json(status, {"ok": False, "error": exc.to_dict()})

    def _read_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1024 * 1024:
            raise PipelineError(ErrorCode.E_INVALID_PARAM, "request body exceeds 1 MiB")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(ErrorCode.E_INVALID_PARAM, "request body must be UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise PipelineError(ErrorCode.E_INVALID_PARAM, "request body must be a JSON object")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path in {"/", "/index.html"}:
                self._send_html()
                return
            if parsed.path == "/health":
                self._send_json(HTTPStatus.OK, {"ok": True, "service": "ctr-cvr-workbench", "bind": "localhost"})
                return
            if parsed.path == "/api/status":
                self._send_json(HTTPStatus.OK, {"ok": True, **self.workbench.status(_query_one(query, "config"))})
                return
            if parsed.path == "/api/versions":
                status = self.workbench.status(_query_one(query, "config"))
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "config_version": status["config_version"],
                        "data_content_fingerprint": status["data_content_fingerprint"],
                        "feature_schema_fingerprint": status["feature_schema_fingerprint"],
                        "stages": status["stages"],
                    },
                )
                return
            if parsed.path == "/api/summary":
                self._send_json(HTTPStatus.OK, {"ok": True, "summary": self.workbench.summary(_query_one(query, "config"))})
                return
            if parsed.path == "/api/losses":
                include_traversal = _query_one(query, "include_traversal") in {"1", "true", "yes"}
                self._send_json(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        **self.workbench.losses(
                            _query_one(query, "config"), include_traversal=include_traversal
                        ),
                    },
                )
                return
            if parsed.path == "/api/funnel":
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, "diagnostics": self.workbench.funnel_diagnostics(_query_one(query, "config"))},
                )
                return
            if parsed.path == "/api/model":
                self._send_json(HTTPStatus.OK, {"ok": True, "model": self.workbench.model(_query_one(query, "config"))})
                return
            if parsed.path == "/api/scores":
                limit_value = _query_one(query, "limit")
                limit = int(limit_value) if limit_value else 0
                self._send_json(
                    HTTPStatus.OK,
                    {"ok": True, **self.workbench.scores(_query_one(query, "config"), limit=limit)},
                )
                return
            if parsed.path == "/api/jobs":
                with self.workbench.lock:
                    jobs = [job.public() for job in self.workbench.jobs.values()]
                self._send_json(HTTPStatus.OK, {"ok": True, "jobs": jobs})
                return
            match = JOB_PATH.match(parsed.path)
            if match:
                job = self.workbench.get_job(int(match.group(1)))
                self._send_json(HTTPStatus.OK, {"ok": True, "job": job.public()})
                return
            raise PipelineError(ErrorCode.E_NOT_FOUND, "HTTP route not found", details={"path": parsed.path})
        except PipelineError as exc:
            self._error(exc, HTTPStatus.NOT_FOUND if exc.code == ErrorCode.E_NOT_FOUND else HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - boundary
            self._error(PipelineError(ErrorCode.E_IO, str(exc)), HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            body = self._read_body()
            if parsed.path == "/api/import":
                self._send_json(HTTPStatus.OK, {"ok": True, **self.workbench.import_workspace(body)})
                return
            if parsed.path == "/api/run":
                job = self.workbench.start_job(
                    config_path=body.get("config_path"),
                    injection=body.get("injection"),
                    worker_override=body.get("worker_override"),
                    execution_mode=body.get("execution_mode"),
                    stop_after_epochs=body.get("stop_after_epochs"),
                )
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "job": job.public()})
                return
            if parsed.path == "/api/stop":
                with self.workbench.lock:
                    active_id = self.workbench.active_job_id
                if active_id is None:
                    raise PipelineError(ErrorCode.E_NOT_FOUND, "no active job to stop")
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, **self.workbench.stop_job(active_id)})
                return
            if parsed.path == "/api/resume":
                stopped = [
                    job
                    for job in self.workbench.jobs.values()
                    if job.status == "stopped"
                ]
                if not stopped:
                    raise PipelineError(ErrorCode.E_NOT_FOUND, "no stopped job to resume")
                job = self.workbench.resume_job(max(stopped, key=lambda item: item.id).id)
                self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "job": job.public()})
                return
            match = JOB_PATH.match(parsed.path)
            if match:
                job_id = int(match.group(1))
                action = match.group(2)
                if action == "stop":
                    self._send_json(HTTPStatus.ACCEPTED, {"ok": True, **self.workbench.stop_job(job_id)})
                    return
                if action == "resume":
                    job = self.workbench.resume_job(job_id)
                    self._send_json(HTTPStatus.ACCEPTED, {"ok": True, "job": job.public()})
                    return
            raise PipelineError(ErrorCode.E_NOT_FOUND, "HTTP route not found", details={"path": parsed.path})
        except PipelineError as exc:
            status = HTTPStatus.CONFLICT if exc.code == ErrorCode.E_JOB_ACTIVE else HTTPStatus.BAD_REQUEST
            if exc.code == ErrorCode.E_NOT_FOUND:
                status = HTTPStatus.NOT_FOUND
            self._error(exc, status)
        except Exception as exc:  # pragma: no cover - boundary
            self._error(PipelineError(ErrorCode.E_IO, str(exc)), HTTPStatus.INTERNAL_SERVER_ERROR)


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], workbench: Workbench):
        super().__init__(address, WorkbenchHandler)
        self.workbench = workbench


def _query_one(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


def serve(
    base_dir: str | Path,
    *,
    config_path: str = "configs/default.yaml",
    host: str | None = None,
    port: int | None = None,
) -> None:
    base = Path(base_dir).resolve()
    config = Config.load(config_path, base)
    bind_host = host or config.server_host
    bind_port = port or config.server_port
    if bind_host != "127.0.0.1":
        raise PipelineError(
            ErrorCode.E_INVALID_PARAM,
            "HTTP workbench may bind only to 127.0.0.1",
            details={"host": bind_host},
        )
    workbench = Workbench(base, config_path)
    server = WorkbenchHTTPServer((bind_host, bind_port), workbench)
    print(f"CTR/CVR workbench listening on http://{bind_host}:{bind_port} (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
