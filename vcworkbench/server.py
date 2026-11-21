from __future__ import annotations

import copy
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import numpy as np

from .canonical import ensure_within
from .demo import generate_demo
from .errors import ERROR_HTTP_STATUS, OperationStopped, WorkbenchError, fail
from .manifest import load_manifest
from .pipeline import PipelineRunner
from .storage import STAGES, CheckpointStore
from .version import CONFIG_VERSION, PACKAGE_VERSION
from .workspace import Workspace


class JobManager:
    def __init__(self, workspace: Workspace) -> None:
        self.workspace = workspace
        self._lock = threading.Lock()
        self._counter = 0
        self._jobs: dict[str, dict[str, Any]] = {}

    def create(self, mode: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._counter += 1
            job_id = f"job-{self._counter:04d}"
            job = {
                "id": job_id,
                "mode": mode,
                "status": "queued",
                "created_at_ms": int(time.time() * 1000),
                "payload": copy.deepcopy(payload),
                "progress": None,
                "result": None,
                "error": None,
            }
            self._jobs[job_id] = job
        thread = threading.Thread(target=self._run, args=(job_id, mode, payload), daemon=True)
        thread.start()
        return self.get(job_id)

    def _update(self, job_id: str, **values: Any) -> None:
        with self._lock:
            self._jobs[job_id].update(values)

    def _run(self, job_id: str, mode: str, payload: dict[str, Any]) -> None:
        self._update(job_id, status="running", started_at_ms=int(time.time() * 1000))
        try:
            if mode in {"convert", "resume", "inject", "stream"}:
                shard_ids = payload.get("shard_ids")
                block_frames = int(payload.get("block_frames", 8))
                verify_stream = bool(payload.get("verify_stream", True))
                injections = payload.get("injections") or None
                stop_after_blocks = payload.get("stop_after_blocks")
                clear_stop = mode == "resume" or bool(payload.get("resume", False))
                runner = PipelineRunner(
                    self.workspace,
                    stop_after_blocks=None
                    if stop_after_blocks in (None, 0)
                    else int(stop_after_blocks),
                )

                def progress(event: dict[str, Any]) -> None:
                    # Full per-shard results are exposed at completion; progress stays small.
                    public = {key: value for key, value in event.items() if key != "result"}
                    if "result" in event:
                        public["shard_result"] = {
                            "shard_id": event["result"].get("shard_id"),
                            "pcm_sha256": event["result"].get("output", {}).get("pcm_sha256"),
                        }
                    self._update(job_id, progress=public)

                conversion = runner.convert(
                    shard_ids=shard_ids,
                    block_frames=block_frames,
                    verify_stream=verify_stream,
                    injections=injections,
                    clear_stop=clear_stop,
                    progress=progress,
                )
                if mode == "stream":
                    streaming = []
                    for result in conversion["results"]:
                        stream_result = runner.stream_check(
                            result["shard_id"], block_frames=block_frames
                        )
                        streaming.append(
                            {
                                key: value
                                for key, value in stream_result.items()
                                if key not in {"chunks"}
                            }
                            | {
                                "chunks": [
                                    {
                                        key: value
                                        for key, value in chunk.items()
                                        if key != "pcm_base64"
                                    }
                                    for chunk in stream_result["chunks"]
                                ]
                            }
                        )
                    conversion["streaming"] = streaming
                self._update(
                    job_id,
                    status="completed",
                    result=conversion,
                    finished_at_ms=int(time.time() * 1000),
                )
            else:
                fail("E_INVALID_PARAM", f"unsupported job mode: {mode}", stage="job")
        except OperationStopped as exc:
            self._update(
                job_id,
                status="stopped",
                result=exc.to_dict(),
                finished_at_ms=int(time.time() * 1000),
            )
        except WorkbenchError as exc:
            self._update(
                job_id,
                status="failed",
                error=exc.to_dict()["error"],
                finished_at_ms=int(time.time() * 1000),
            )
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._update(
                job_id,
                status="failed",
                error={
                    "code": "E_INTERNAL",
                    "message": f"{type(exc).__name__}: {exc}",
                    "shard_id": None,
                    "stage": "job",
                    "details": {},
                },
                finished_at_ms=int(time.time() * 1000),
            )

    def get(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                fail("E_JOB_NOT_FOUND", f"unknown job: {job_id}", stage="job", job_id=job_id)
            return copy.deepcopy(job)

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [copy.deepcopy(job) for job in reversed(self._jobs.values())]


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], workspace: Workspace) -> None:
        self.workspace = workspace
        self.jobs = JobManager(workspace)
        self.static_root = Path(__file__).resolve().parent / "static"
        super().__init__(address, WorkbenchHandler)


class WorkbenchHandler(BaseHTTPRequestHandler):
    server: WorkbenchHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        print(f"[http] {self.address_string()} {fmt % args}", file=sys.stderr)

    def _send_bytes(
        self,
        status: int,
        data: bytes,
        content_type: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _send_json(self, status: int, payload: Any) -> None:
        data = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        self._send_bytes(status, data, "application/json; charset=utf-8")

    def _error(self, exc: WorkbenchError) -> None:
        status = ERROR_HTTP_STATUS.get(exc.code, 500)
        self._send_json(status, exc.to_dict())

    def _body_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            fail("E_BAD_REQUEST", "invalid Content-Length", stage="http")
        if length > 16 * 1024 * 1024:
            fail("E_BAD_REQUEST", "request body is too large", stage="http")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            value = json.loads(raw.decode("utf-8")) if raw else {}
        except Exception as exc:
            raise WorkbenchError("E_BAD_REQUEST", f"invalid JSON body: {exc}", stage="http") from exc
        if not isinstance(value, dict):
            fail("E_BAD_REQUEST", "JSON body must be an object", stage="http")
        return value

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_bytes(204, b"", "text/plain", extra_headers={"Allow": "GET,POST,OPTIONS"})

    def do_HEAD(self) -> None:  # noqa: N802
        self.do_GET()

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._route_get()
        except WorkbenchError as exc:
            self._error(exc)
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._error(
                WorkbenchError(
                    "E_INTERNAL",
                    f"{type(exc).__name__}: {exc}",
                    stage="http",
                )
            )

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._route_post()
        except WorkbenchError as exc:
            self._error(exc)
        except OperationStopped as exc:
            self._send_json(202, exc.to_dict())
        except Exception as exc:  # pragma: no cover - defensive boundary
            self._error(
                WorkbenchError(
                    "E_INTERNAL",
                    f"{type(exc).__name__}: {exc}",
                    stage="http",
                )
            )

    def _route_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)
        if path == "/":
            self._serve_static("index.html")
            return
        if path.startswith("/static/"):
            self._serve_static(path[len("/static/") :])
            return
        if path == "/api/health":
            self._send_json(
                200,
                {
                    "ok": True,
                    "service": "vc-streaming-workbench",
                    "version": PACKAGE_VERSION,
                    "config_version": CONFIG_VERSION,
                    "workspace": str(self.server.workspace.root),
                    "state_dir": str(self.server.workspace.state_dir),
                    "numpy": np.__version__,
                    "deterministic": True,
                    "stop": self.server.workspace.stop_status(),
                },
            )
            return
        if path == "/api/state":
            self._send_json(200, self._state_payload())
            return
        if path == "/api/jobs":
            self._send_json(200, {"ok": True, "jobs": self.server.jobs.list()})
            return
        if path.startswith("/api/jobs/"):
            job_id = path.split("/")[-1]
            self._send_json(200, {"ok": True, "job": self.server.jobs.get(job_id)})
            return
        if path.startswith("/api/diagnostics/") and path.endswith("/frame"):
            shard_id = path[len("/api/diagnostics/") : -len("/frame")].strip("/")
            self._frame_diagnostic(shard_id, query)
            return
        if path.startswith("/api/diagnostics/"):
            shard_id = path[len("/api/diagnostics/") :].strip("/")
            runner = PipelineRunner(self.server.workspace)
            self._send_json(200, runner.diagnostics(shard_id))
            return
        if path.startswith("/api/report/"):
            shard_id = path[len("/api/report/") :].strip("/")
            runner = PipelineRunner(self.server.workspace)
            self._send_json(200, runner.report(shard_id))
            return
        if path.startswith("/api/fingerprints/"):
            shard_id = path[len("/api/fingerprints/") :].strip("/")
            store = CheckpointStore(self.server.workspace.state_dir)
            status = store.status(shard_id, verify_artifacts=True)
            self._send_json(
                200,
                {
                    "ok": "error" not in status,
                    "shard_id": shard_id,
                    "fingerprints": {
                        stage: (status.get("current", {}).get(stage) or {}).get(
                            "input_fingerprint"
                        )
                        for stage in STAGES
                    },
                    "checkpoints": status,
                },
            )
            return
        if path.startswith("/api/stream/"):
            shard_id = path[len("/api/stream/") :].strip("/")
            block_frames = int((query.get("block_frames") or ["8"])[0])
            runner = PipelineRunner(self.server.workspace)
            self._send_json(
                200, runner.stream_check(shard_id, block_frames=block_frames)
            )
            return
        if path.startswith("/api/export/") and path.endswith(".wav"):
            shard_id = path[len("/api/export/") : -len(".wav")]
            artifact = CheckpointStore(
                self.server.workspace.state_dir
            ).load_current_artifact(shard_id, "reconstruction")
            if artifact is None:
                fail(
                    "E_INVALID_STAGE",
                    "reconstruction artifact is not available",
                    shard_id=shard_id,
                    stage="reconstruction",
                )
            data = artifact.path("output.wav").read_bytes()
            self._send_bytes(
                200,
                data,
                "audio/wav",
                extra_headers={"Content-Disposition": f'attachment; filename="{shard_id}.wav"'},
            )
            return
        fail("E_NOT_FOUND", f"unknown route: {path}", stage="http")

    def _route_post(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        body = self._body_json()
        if path == "/api/demo":
            self._send_json(
                200,
                generate_demo(self.server.workspace, force=bool(body.get("force", False))),
            )
            return
        if path == "/api/import":
            manifest_path = body.get("manifest_path")
            if not isinstance(manifest_path, str) or not manifest_path:
                fail("E_INVALID_PARAM", "manifest_path is required", stage="http")
            manifest = self.server.workspace.import_manifest(Path(manifest_path))
            self._send_json(
                200,
                {
                    "ok": True,
                    "status": "imported",
                    "manifest_digest": manifest.digest(),
                    "shard_ids": [spec.shard_id for spec in manifest.shards],
                },
            )
            return
        if path == "/api/convert":
            runner = self._runner_from_payload(body)
            result = runner.convert(
                shard_ids=body.get("shard_ids"),
                block_frames=int(body.get("block_frames", 8)),
                verify_stream=bool(body.get("verify_stream", True)),
                injections=self._injection_map(body),
                clear_stop=bool(body.get("resume", False)),
            )
            self._send_json(200, result)
            return
        if path == "/api/inject":
            shard_ids = body.get("shard_ids") or ([body["shard_id"]] if body.get("shard_id") else None)
            injections = self._injection_map(body)
            runner = self._runner_from_payload(body)
            result = runner.convert(
                shard_ids=shard_ids,
                block_frames=int(body.get("block_frames", 8)),
                verify_stream=bool(body.get("verify_stream", False)),
                injections=injections,
            )
            self._send_json(200, result)
            return
        if path == "/api/jobs":
            mode = body.pop("mode", "convert")
            if mode == "inject" and "injections" not in body and "injection" in body:
                body["injections"] = self._injection_map(body)
            job = self.server.jobs.create(str(mode), body)
            self._send_json(202, {"ok": True, "job": job})
            return
        if path.startswith("/api/jobs/") and path.endswith("/stop"):
            job_id = path[len("/api/jobs/") : -len("/stop")].strip("/")
            job = self.server.jobs.get(job_id)
            stopped = self.server.workspace.request_stop(reason=f"stop requested for {job_id}")
            self._send_json(202, {"ok": True, "job_id": job["id"], "stop": stopped})
            return
        if path == "/api/stop":
            stop = self.server.workspace.request_stop(
                reason=str(body.get("reason", "operator request")),
                scope=body.get("scope"),
            )
            self._send_json(202, {"ok": True, "stop": stop})
            return
        if path == "/api/resume":
            body["mode"] = "resume"
            body.setdefault("verify_stream", True)
            job = self.server.jobs.create("resume", body)
            self._send_json(202, {"ok": True, "job": job})
            return
        fail("E_NOT_FOUND", f"unknown route: {path}", stage="http")

    def _runner_from_payload(self, body: dict[str, Any]) -> PipelineRunner:
        stop_after = body.get("stop_after_blocks")
        if stop_after in (None, 0, "0"):
            stop_after = None
        else:
            stop_after = int(stop_after)
        return PipelineRunner(self.server.workspace, stop_after_blocks=stop_after)

    def _injection_map(self, body: dict[str, Any]) -> dict[str, dict[str, Any]] | None:
        if isinstance(body.get("injections"), dict):
            return body["injections"]
        injection = body.get("injection")
        if not isinstance(injection, dict):
            return None
        selected = body.get("shard_ids")
        if not selected:
            if body.get("shard_id"):
                selected = [body["shard_id"]]
            else:
                selected = [
                    spec.shard_id
                    for spec in self.server.workspace.require_manifest().shards
                ]
        return {shard_id: injection for shard_id in selected}

    def _frame_diagnostic(self, shard_id: str, query: dict[str, list[str]]) -> None:
        start = time.perf_counter()
        frame_value = (query.get("frame_index") or query.get("frame") or [None])[0]
        if frame_value is None:
            fail(
                "E_INVALID_PARAM",
                "frame_index query parameter is required",
                shard_id=shard_id,
                stage="diagnostics",
            )
        try:
            frame_index = int(frame_value)
        except ValueError:
            fail(
                "E_INVALID_PARAM",
                "frame_index must be an integer",
                shard_id=shard_id,
                stage="diagnostics",
                frame_index=frame_value,
            )
        runner = PipelineRunner(self.server.workspace)
        payload = runner.diagnose_frame(shard_id, frame_index)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        budget_value = (query.get("latency_budget_ms") or [None])[0]
        if budget_value is not None:
            try:
                budget_ms = float(budget_value)
            except ValueError:
                fail(
                    "E_INVALID_PARAM",
                    "latency_budget_ms must be numeric",
                    shard_id=shard_id,
                    stage="diagnostics",
                )
            if elapsed_ms > budget_ms:
                fail(
                    "E_LATENCY_BUDGET",
                    "frame diagnostic exceeded the latency budget",
                    shard_id=shard_id,
                    stage="diagnostics",
                    frame_index=frame_index,
                    latency_budget_ms=budget_ms,
                    elapsed_ms=round(elapsed_ms, 3),
                )
            payload["latency_budget_ms"] = budget_ms
        payload["latency_ms"] = round(elapsed_ms, 3)
        self._send_json(200, payload)

    def _state_payload(self) -> dict[str, Any]:
        workspace = self.server.workspace
        payload: dict[str, Any] = {
            "ok": True,
            "workspace": str(workspace.root),
            "state_dir": str(workspace.state_dir),
            "ready": workspace.has_manifest(),
            "stop": workspace.stop_status(),
            "version": PACKAGE_VERSION,
            "config_version": CONFIG_VERSION,
        }
        if not workspace.has_manifest():
            payload["shards"] = []
            return payload
        manifest = load_manifest(workspace.manifest_path)
        store = CheckpointStore(workspace.state_dir)
        runner = PipelineRunner(workspace)
        shards: list[dict[str, Any]] = []
        for spec in manifest.shards:
            status = store.status(spec.shard_id, verify_artifacts=True)
            item: dict[str, Any] = {
                "spec": spec.to_dict(),
                "checkpoints": status,
            }
            if status.get("stage_status", {}).get("validation") == "complete":
                try:
                    item["diagnostics"] = runner.diagnostics(spec.shard_id)["summary"]
                except WorkbenchError as exc:
                    item["diagnostics_error"] = exc.to_dict()["error"]
            shards.append(item)
        payload.update(
            {
                "manifest_digest": manifest.digest(),
                "shards": shards,
                "jobs": self.server.jobs.list()[:10],
            }
        )
        return payload

    def _serve_static(self, relative: str) -> None:
        try:
            path = ensure_within(self.server.static_root, self.server.static_root / relative)
        except ValueError:
            fail("E_NOT_FOUND", "static path is outside the asset root", stage="http")
        if not path.is_file():
            fail("E_NOT_FOUND", f"static asset not found: {relative}", stage="http")
        content_types = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".svg": "image/svg+xml",
            ".png": "image/png",
            ".ico": "image/x-icon",
        }
        self._send_bytes(
            200,
            path.read_bytes(),
            content_types.get(path.suffix.lower(), "application/octet-stream"),
        )


def run_server(
    workspace: Workspace,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    auto_demo: bool = True,
) -> WorkbenchHTTPServer:
    workspace.ensure()
    if auto_demo and not workspace.has_manifest():
        generate_demo(workspace)
    try:
        server = WorkbenchHTTPServer((host, port), workspace)
    except OSError as exc:
        fail(
            "E_PORT_UNAVAILABLE",
            f"cannot bind HTTP server on {host}:{port}: {exc}",
            stage="http",
            host=host,
            port=port,
        )
    return server
