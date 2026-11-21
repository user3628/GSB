from __future__ import annotations

import json
import platform
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import numpy as np

from . import PIPELINE_VERSION
from .checkpoints import request_stop
from .config import list_versions
from .errors import InvalidParam, Interrupted, QPipeError, ToleranceExceeded
from .pipeline import Pipeline
from .util import canonical_json_bytes
from .verifier import verify_run


class JobManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}
        self._active_job: str | None = None
        self._counter = 0

    def start(self, name: str, target: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        with self._lock:
            if self._active_job is not None:
                active = self._jobs[self._active_job]
                if active["status"] == "running":
                    raise InvalidParam(
                        "another pipeline job is already running",
                        parameter="job",
                        details={"active_job_id": self._active_job},
                    )
            self._counter += 1
            job_id = f"job-{self._counter:06d}"
            job: dict[str, Any] = {
                "job_id": job_id,
                "name": name,
                "status": "running",
                "code": None,
                "result": None,
                "error": None,
            }
            self._jobs[job_id] = job
            self._active_job = job_id
        thread = threading.Thread(
            target=self._run_job, args=(job_id, target), daemon=True
        )
        thread.start()
        return dict(job)

    def _run_job(self, job_id: str, target: Callable[[], dict[str, Any]]) -> None:
        try:
            result = target()
            with self._lock:
                self._jobs[job_id]["status"] = result.get("status", "pass")
                self._jobs[job_id]["code"] = result.get("code")
                self._jobs[job_id]["result"] = result
        except ToleranceExceeded as exc:
            with self._lock:
                self._jobs[job_id]["status"] = "tolerance_failed"
                self._jobs[job_id]["code"] = exc.code
                self._jobs[job_id]["error"] = exc.to_dict()
        except Interrupted as exc:
            with self._lock:
                self._jobs[job_id]["status"] = "interrupted"
                self._jobs[job_id]["code"] = exc.code
                self._jobs[job_id]["error"] = exc.to_dict()
        except QPipeError as exc:
            with self._lock:
                self._jobs[job_id]["status"] = "error"
                self._jobs[job_id]["code"] = exc.code
                self._jobs[job_id]["error"] = exc.to_dict()
        except Exception as exc:
            with self._lock:
                self._jobs[job_id]["status"] = "error"
                self._jobs[job_id]["code"] = "E_INTERNAL"
                self._jobs[job_id]["error"] = {
                    "code": "E_INTERNAL",
                    "message": f"{type(exc).__name__}: {exc}",
                }
        finally:
            with self._lock:
                if self._active_job == job_id:
                    self._active_job = None

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job is not None else None

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(job) for job in self._jobs.values()]

    @property
    def active_job(self) -> str | None:
        with self._lock:
            return self._active_job


class QPipeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.jobs = JobManager()
        super().__init__(address, QPipeRequestHandler)


class QPipeRequestHandler(BaseHTTPRequestHandler):
    server: QPipeHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def _send(self, status: int, value: Any) -> None:
        body = canonical_json_bytes(value, pretty=True)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, exc: QPipeError) -> None:
        self._send(exc.http_status, exc.to_dict())

    def _read_json(self) -> dict[str, Any]:
        length_text = self.headers.get("Content-Length", "0")
        try:
            length = int(length_text)
        except ValueError as exc:
            raise InvalidParam("invalid Content-Length", parameter="body") from exc
        if length < 0 or length > 4 * 1024 * 1024:
            raise InvalidParam("request body is too large", parameter="body")
        data = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidParam("request body must be JSON", parameter="body") from exc
        if not isinstance(value, dict):
            raise InvalidParam("request JSON must be an object", parameter="body")
        return value

    def _query(self) -> dict[str, str]:
        parsed = urlparse(self.path)
        return {
            key: values[-1]
            for key, values in parse_qs(parsed.query, keep_blank_values=True).items()
        }

    def _path(self) -> str:
        return urlparse(self.path).path

    def do_GET(self) -> None:  # noqa: N802
        try:
            path = self._path()
            query = self._query()
            if path == "/api/health":
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "pipeline_version": PIPELINE_VERSION,
                        "python": platform.python_version(),
                        "numpy": np.__version__,
                        "loopback": self.server.server_address[0]
                        in ("127.0.0.1", "::1", "localhost"),
                        "active_job": self.server.jobs.active_job,
                    },
                )
                return
            if path == "/api/status":
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "status": self.server.pipeline.status(
                            run_id=query.get("run_id"),
                            version=query.get("version"),
                        ),
                        "active_job": self.server.jobs.active_job,
                    },
                )
                return
            if path == "/api/versions":
                self._send(
                    HTTPStatus.OK,
                    {"ok": True, "versions": list_versions(self.server.pipeline.root)},
                )
                return
            if path == "/api/layers":
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "layers": self.server.pipeline.layers(
                            run_id=query.get("run_id")
                        ),
                    },
                )
                return
            if path == "/api/errors":
                self._send(
                    HTTPStatus.OK,
                    {
                        "ok": True,
                        "error_report": self.server.pipeline.errors(
                            run_id=query.get("run_id")
                        ),
                    },
                )
                return
            if path == "/api/jobs":
                self._send(
                    HTTPStatus.OK,
                    {"ok": True, "jobs": self.server.jobs.list()},
                )
                return
            if path.startswith("/api/jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                job = self.server.jobs.get(job_id)
                if job is None:
                    raise InvalidParam(
                        "job not found", parameter="job_id", details={"job_id": job_id}
                    )
                self._send(HTTPStatus.OK, {"ok": True, "job": job})
                return
            raise InvalidParam("unknown route", parameter="path", details={"path": path})
        except QPipeError as exc:
            self._error(exc)
        except Exception as exc:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"code": "E_INTERNAL", "message": f"{type(exc).__name__}: {exc}"},
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = self._path()
            body = self._read_json()
            if path == "/api/import":
                manifest = Path(str(body.get("manifest", "")))
                shapes = Path(str(body.get("shapes", "")))
                if not manifest.is_absolute():
                    manifest = self.server.pipeline.workspace / manifest
                if not shapes.is_absolute():
                    shapes = self.server.pipeline.workspace / shapes
                result = self.server.pipeline.import_config(
                    manifest_path=manifest,
                    shapes_path=shapes,
                    version=str(body.get("version", "v1")),
                )
                self._send(
                    HTTPStatus.CREATED,
                    {
                        "ok": True,
                        "config_id": result["config_id"],
                        "version": body.get("version", "v1"),
                        "input_hashes": result["input_hashes"],
                    },
                )
                return
            if path in ("/api/run", "/api/resume"):
                run_id = str(body.get("run_id", "http"))
                version = body.get("version")

                def target() -> dict[str, Any]:
                    outcome = self.server.pipeline.run(
                        run_id=run_id,
                        version=version,
                        resume=path == "/api/resume",
                        stop_after_units=body.get("stop_after_units"),
                    )
                    return {
                        "status": outcome.status,
                        "code": outcome.code,
                        "run_id": outcome.run_id,
                        "final_digest": outcome.final_digest,
                        "output_dir": outcome.output_dir,
                        "invalidation": outcome.invalidation,
                        "end_to_end": outcome.result["end_to_end"],
                    }

                job = self.server.jobs.start("resume" if path.endswith("resume") else "run", target)
                self._send(HTTPStatus.ACCEPTED, {"ok": True, "job": job})
                return
            if path == "/api/stage" or (
                path.startswith("/api/stages/") and path.endswith("/run")
            ):
                stage = (
                    str(body.get("stage", ""))
                    if path == "/api/stage"
                    else path.split("/")[3]
                )
                run_id = str(body.get("run_id", "http"))
                version = body.get("version")

                def stage_target() -> dict[str, Any]:
                    return self.server.pipeline.run_stage(
                        stage=stage,
                        run_id=run_id,
                        version=version,
                        resume=bool(body.get("resume", False)),
                        stop_after_units=body.get("stop_after_units"),
                    )

                job = self.server.jobs.start(f"stage:{stage}", stage_target)
                self._send(HTTPStatus.ACCEPTED, {"ok": True, "job": job})
                return
            if path == "/api/stop":
                request_stop(self.server.pipeline.root, reason="http_request")
                self._send(
                    HTTPStatus.OK,
                    {"ok": True, "code": "E_INTERRUPTED", "stop_requested": True},
                )
                return
            if path == "/api/verify":
                summary = verify_run(
                    root=self.server.pipeline.root,
                    run_id=str(body.get("run_id", "default")),
                    compare_run_id=body.get("compare_run_id"),
                )
                self._send(HTTPStatus.OK, {"ok": True, "verification": summary})
                return
            if path == "/api/publish-layer":
                record = self.server.pipeline.publish_layer_probe(
                    run_id=str(body["run_id"]),
                    version=body.get("version"),
                    stage=str(body["stage"]),
                    layer_seq=int(body["layer_seq"]),
                    layer_id=str(body["layer_id"]),
                    input_fingerprint=str(body["input_fingerprint"]),
                    artifact_digest=str(body["artifact_digest"]),
                )
                self._send(HTTPStatus.CREATED, {"ok": True, "record": record})
                return
            raise InvalidParam("unknown route", parameter="path", details={"path": path})
        except KeyError as exc:
            self._send(
                HTTPStatus.BAD_REQUEST,
                {
                    "code": "E_INVALID_PARAM",
                    "message": f"missing JSON field: {exc.args[0]}",
                    "parameter": exc.args[0],
                },
            )
        except QPipeError as exc:
            self._error(exc)
        except Exception as exc:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"code": "E_INTERNAL", "message": f"{type(exc).__name__}: {exc}"},
            )


def make_server(
    pipeline: Pipeline, *, host: str = "127.0.0.1", port: int = 8787
) -> QPipeHTTPServer:
    if host not in ("127.0.0.1", "localhost", "::1"):
        raise InvalidParam("HTTP interface must bind to loopback", parameter="host")
    return QPipeHTTPServer((host, port), pipeline)


def serve(pipeline: Pipeline, *, host: str = "127.0.0.1", port: int = 8787) -> None:
    server = make_server(pipeline, host=host, port=port)
    actual_host, actual_port = server.server_address[:2]
    print(
        json.dumps(
            {
                "status": "listening",
                "host": actual_host,
                "port": actual_port,
                "url": f"http://{actual_host}:{actual_port}",
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
