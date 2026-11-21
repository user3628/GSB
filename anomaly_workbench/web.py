"""Local HTTP diagnostic workbench backed by ThreadingHTTPServer."""

from __future__ import annotations

import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import WorkbenchError, error_payload
from .pipeline import Pipeline
from .reference import verify_reference
from .util import canonical_json_bytes


ERROR_STATUS = {
    "E_NOT_FOUND": HTTPStatus.NOT_FOUND,
    "E_BUSY": HTTPStatus.CONFLICT,
    "E_STALE_CHECKPOINT": HTTPStatus.CONFLICT,
    "E_INCREMENTAL_MISMATCH": HTTPStatus.CONFLICT,
    "E_NUMERIC": HTTPStatus.UNPROCESSABLE_ENTITY,
    "E_INVALID_CONFIG": HTTPStatus.BAD_REQUEST,
    "E_INVALID_STAGE": HTTPStatus.CONFLICT,
    "E_DATA": HTTPStatus.BAD_REQUEST,
    "E_REFERENCE_MISMATCH": HTTPStatus.CONFLICT,
}


INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><title>Anomaly Diagnostics Workbench</title>
<style>body{font-family:ui-monospace,monospace;max-width:1100px;margin:2rem auto;padding:0 1rem;background:#f6f7f9;color:#111}button{margin:.2rem;padding:.45rem .7rem}pre{background:#111;color:#d8f3dc;padding:1rem;overflow:auto;max-height:70vh}h1{font-size:1.3rem}</style>
</head><body><h1>Deterministic Anomaly Diagnostics Workbench</h1>
<p>Local API: status, staged execution, stop/resume, incremental invalidation, sensitivity and export.</p>
<div><button onclick="call('/api/status')">status</button><button onclick="call('/api/execute',{tiers:['small','medium']})">execute</button><button onclick="call('/api/stop',{})">stop</button><button onclick="call('/api/resume',{tiers:['small','medium']})">resume</button><button onclick="call('/api/sensitivity?tier=small')">small sensitivity</button><button onclick="call('/api/invalidations')">invalidations</button></div>
<pre id="out">ready</pre><script>
async function call(url,body){const opt=body===undefined?{}:{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)};const r=await fetch(url,opt);const j=await r.json();document.getElementById('out').textContent=JSON.stringify(j,null,2)}
</script></body></html>"""


class ApiRuntime:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)
        self.pipeline = Pipeline(self.root)
        self._thread_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.last_result: dict[str, Any] | None = None
        self.last_error: dict[str, Any] | None = None

    def running(self) -> bool:
        with self._thread_lock:
            return self._thread is not None and self._thread.is_alive()

    def start(
        self,
        *,
        tiers: list[str] | None,
        resume: bool,
        verify_reference_mode: bool,
        through_stage: str | None = None,
    ) -> dict[str, Any]:
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                raise WorkbenchError("E_BUSY", "an API execution is already running")
            if resume:
                self.pipeline.clear_stop()
            self.last_error = None
            self.last_result = None

            def worker() -> None:
                try:
                    if resume:
                        result = self.pipeline.resume(
                            tiers=tiers,
                            verify_reference=verify_reference_mode,
                            through_stage=through_stage,
                        )
                    else:
                        result = self.pipeline.run(
                            tiers=tiers,
                            verify_reference=verify_reference_mode,
                            through_stage=through_stage,
                        )
                    self.last_result = result
                except WorkbenchError as exc:
                    self.last_error = exc.to_dict()
                except Exception as exc:  # defensive boundary for a local service
                    self.last_error = {
                        "code": "E_INTERNAL",
                        "message": str(exc),
                        "stage": None,
                        "sample_id": None,
                        "details": {"exception_type": type(exc).__name__},
                    }

            thread = threading.Thread(target=worker, name="awb-executor", daemon=True)
            self._thread = thread
            thread.start()
        return {
            "started": True,
            "resume": resume,
            "tiers": tiers,
            "through_stage": through_stage,
            "state": self.execution_snapshot(),
        }

    def wait(self) -> dict[str, Any]:
        with self._thread_lock:
            thread = self._thread
        if thread is not None:
            thread.join()
        return self.execution_snapshot()

    def execution_snapshot(self) -> dict[str, Any]:
        return {
            "running": self.running(),
            "result": self.last_result,
            "error": self.last_error,
        }


def _tier_parameter(query: dict[str, list[str]], default: str | None = "small") -> str:
    values = query.get("tier")
    if values and values[0]:
        return values[0]
    if default is None:
        raise WorkbenchError("E_DATA", "tier query parameter is required")
    return default


def create_server(
    root: str | os.PathLike[str], host: str = "127.0.0.1", port: int = 8765
) -> tuple[ThreadingHTTPServer, ApiRuntime]:
    runtime = ApiRuntime(root)

    class Handler(BaseHTTPRequestHandler):
        server_version = "AnomalyWorkbench/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _send(self, status: int, payload: Any) -> None:
            if isinstance(payload, bytes):
                body = payload
                content_type = "application/json; charset=utf-8"
            elif isinstance(payload, str):
                body = payload.encode("utf-8")
                content_type = "text/html; charset=utf-8"
            else:
                body = canonical_json_bytes(payload)
                content_type = "application/json; charset=utf-8"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _ok(self, payload: dict[str, Any], status: int = 200) -> None:
            self._send(status, {"ok": True, **payload})

        def _error(self, exc: WorkbenchError) -> None:
            self._send(ERROR_STATUS.get(exc.code, HTTPStatus.INTERNAL_SERVER_ERROR), error_payload(exc))

        def _json_body(self) -> dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise WorkbenchError("E_DATA", "invalid Content-Length") from exc
            if length > 2 * 1024 * 1024:
                raise WorkbenchError("E_DATA", "request body is too large")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                value = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                raise WorkbenchError("E_DATA", f"request body is not valid JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise WorkbenchError("E_DATA", "request body must be a JSON object")
            return value

        def _dispatch_get(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path in {"/", "/index.html"}:
                self._send(HTTPStatus.OK, INDEX_HTML)
                return
            if parsed.path == "/api/health":
                self._ok({"status": "ok", "root": str(runtime.root), "running": runtime.running()})
                return
            if parsed.path == "/api/status":
                status = runtime.pipeline.status(include_records=False)
                self._ok({"status": status, "execution": runtime.execution_snapshot()})
                return
            if parsed.path == "/api/summary":
                self._ok({"summary": runtime.pipeline.summary()})
                return
            if parsed.path == "/api/fingerprints":
                status = runtime.pipeline.status(include_records=False)
                tier = query.get("tier", [None])[0]
                if tier:
                    if tier not in status["tiers"]:
                        raise WorkbenchError("E_NOT_FOUND", f"tier {tier!r} is not imported")
                    self._ok({"tier": tier, "fingerprints": status["tiers"][tier]["fingerprints"]})
                else:
                    self._ok(
                        {
                            "fingerprints": {
                                name: value.get("fingerprints")
                                for name, value in status["tiers"].items()
                            }
                        }
                    )
                return
            if parsed.path == "/api/checkpoints":
                self._ok({"checkpoints": runtime.pipeline.store.status(include_records=True)})
                return
            if parsed.path == "/api/sensitivity":
                tier = _tier_parameter(query)
                self._ok({"sensitivity": runtime.pipeline.sensitivity(tier)})
                return
            if parsed.path == "/api/invalidations":
                tier = query.get("tier", [None])[0]
                self._ok({"invalidations": runtime.pipeline.invalidations(tier)})
                return
            if parsed.path == "/api/scores":
                tier = _tier_parameter(query)
                artifacts = runtime.pipeline.current_artifacts(tier)
                payload = artifacts["score"]["payload"]
                self._ok(
                    {
                        "tier": tier,
                        "artifact_digest": artifacts["score"]["artifact_digest"],
                        "scores": payload,
                    }
                )
                return
            if parsed.path == "/api/diagnostics":
                tier = _tier_parameter(query)
                artifacts = runtime.pipeline.current_artifacts(tier)
                self._ok(
                    {
                        "tier": tier,
                        "artifact_digest": artifacts["diagnostics"]["artifact_digest"],
                        "diagnostics": artifacts["diagnostics"]["payload"],
                    }
                )
                return
            if parsed.path == "/api/reference":
                self._ok({"reference": verify_reference(runtime.root)})
                return
            if parsed.path == "/api/execution":
                self._ok({"execution": runtime.execution_snapshot()})
                return
            raise WorkbenchError("E_NOT_FOUND", f"unknown endpoint: {parsed.path}")

        def _dispatch_post(self) -> None:
            parsed = urlparse(self.path)
            body = self._json_body()
            if parsed.path == "/api/execute":
                tiers = body.get("tiers")
                if tiers is not None and not isinstance(tiers, list):
                    raise WorkbenchError("E_DATA", "tiers must be a list")
                started = runtime.start(
                    tiers=tiers,
                    resume=False,
                    verify_reference_mode=bool(body.get("verify_reference", False)),
                    through_stage=body.get("through_stage"),
                )
                if bool(body.get("wait", False)):
                    self._ok({"execution": runtime.wait(), "started": started}, 200)
                else:
                    self._ok({"execution": started}, 202)
                return
            if parsed.path == "/api/resume":
                tiers = body.get("tiers")
                if tiers is not None and not isinstance(tiers, list):
                    raise WorkbenchError("E_DATA", "tiers must be a list")
                started = runtime.start(
                    tiers=tiers,
                    resume=True,
                    verify_reference_mode=bool(body.get("verify_reference", False)),
                    through_stage=body.get("through_stage"),
                )
                if bool(body.get("wait", False)):
                    self._ok({"execution": runtime.wait(), "started": started}, 200)
                else:
                    self._ok({"execution": started}, 202)
                return
            if parsed.path == "/api/stop":
                self._ok({"stop": runtime.pipeline.request_stop()})
                return
            if parsed.path == "/api/config":
                patch = body.get("patch")
                if not isinstance(patch, dict):
                    raise WorkbenchError("E_INVALID_CONFIG", "patch must be a JSON object")
                result = runtime.pipeline.update_config(patch)
                execution = None
                if bool(body.get("execute", False)):
                    tiers = body.get("tiers")
                    runtime.start(
                        tiers=tiers,
                        resume=False,
                        verify_reference_mode=False,
                    )
                    execution = runtime.wait() if bool(body.get("wait", True)) else runtime.execution_snapshot()
                self._ok({"config_change": result, "execution": execution})
                return
            if parsed.path == "/api/import":
                tier = body.get("tier")
                declaration_path = body.get("declaration_path")
                shard_paths = body.get("shard_paths")
                if not isinstance(tier, str) or not isinstance(declaration_path, str):
                    raise WorkbenchError("E_DATA", "tier and declaration_path are required")
                if not isinstance(shard_paths, list) or not all(
                    isinstance(value, str) for value in shard_paths
                ):
                    raise WorkbenchError("E_DATA", "shard_paths must be a list of local paths")
                result = runtime.pipeline.import_dataset(
                    tier,
                    declaration_path,
                    shard_paths,
                    replace=bool(body.get("replace", True)),
                )
                execution = None
                verification = None
                if bool(body.get("execute", False)):
                    runtime.start(tiers=[tier], resume=False, verify_reference_mode=False)
                    execution = runtime.wait()
                    if bool(body.get("verify_full", False)) and not runtime.last_error:
                        verification = runtime.pipeline.verify_full_equivalence(tier)
                self._ok({"import": result, "execution": execution, "verification": verification})
                return
            if parsed.path == "/api/replace-shard":
                tier = body.get("tier")
                shard_path = body.get("shard_path")
                if not isinstance(tier, str) or not isinstance(shard_path, str):
                    raise WorkbenchError("E_DATA", "tier and shard_path are required")
                result = runtime.pipeline.replace_shard(
                    tier, shard_path, declaration_source=body.get("declaration_path")
                )
                execution = None
                verification = None
                if bool(body.get("execute", True)):
                    runtime.start(tiers=[tier], resume=False, verify_reference_mode=False)
                    execution = runtime.wait()
                    if bool(body.get("verify_full", True)) and not runtime.last_error:
                        verification = runtime.pipeline.verify_full_equivalence(tier)
                self._ok({"replacement": result, "execution": execution, "verification": verification})
                return
            if parsed.path == "/api/export":
                if bool(body.get("all", False)):
                    tiers = list(runtime.pipeline._select_tiers(None))
                    exports = {
                        tier: runtime.pipeline.export_tier(tier, body.get("output_dir"))
                        for tier in tiers
                    }
                else:
                    tier = body.get("tier", "small")
                    if not isinstance(tier, str):
                        raise WorkbenchError("E_DATA", "tier must be a string")
                    exports = {
                        tier: runtime.pipeline.export_tier(tier, body.get("output_dir"))
                    }
                self._ok({"exports": exports})
                return
            if parsed.path == "/api/verify-full":
                tier = body.get("tier")
                if not isinstance(tier, str):
                    raise WorkbenchError("E_DATA", "tier is required")
                self._ok({"verification": runtime.pipeline.verify_full_equivalence(tier)})
                return
            raise WorkbenchError("E_NOT_FOUND", f"unknown endpoint: {parsed.path}")

        def do_GET(self) -> None:  # noqa: N802 - stdlib handler contract
            try:
                self._dispatch_get()
            except WorkbenchError as exc:
                self._error(exc)
            except Exception as exc:
                self._error(
                    WorkbenchError(
                        "E_INTERNAL",
                        str(exc),
                        details={"exception_type": type(exc).__name__},
                    )
                )

        def do_POST(self) -> None:  # noqa: N802 - stdlib handler contract
            try:
                self._dispatch_post()
            except WorkbenchError as exc:
                self._error(exc)
            except Exception as exc:
                self._error(
                    WorkbenchError(
                        "E_INTERNAL",
                        str(exc),
                        details={"exception_type": type(exc).__name__},
                    )
                )

    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    return server, runtime


def serve(root: str | os.PathLike[str], host: str = "127.0.0.1", port: int = 8765) -> None:
    server, _ = create_server(root, host, port)
    host_value, port_value = server.server_address[:2]
    print(f"Anomaly workbench listening on http://{host_value}:{port_value}", flush=True)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
