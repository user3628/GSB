from __future__ import annotations

import copy
import json
import socket
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import __version__
from .config import DEFAULT_CONFIG, config_fingerprint, validate_config
from .errors import WorkbenchError
from .pipeline import Workbench
from .util import apply_overrides, canonical_json_text, deep_merge


HTML_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DimCluster Diagnostics Workbench</title>
<style>
:root{color-scheme:dark;--bg:#0f141b;--panel:#171f29;--line:#2c3948;--fg:#dce6ef;--muted:#8ea1b4;--accent:#65d3b3;--warn:#ffc857;--bad:#ff7b72}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace}
header{padding:20px 24px;border-bottom:1px solid var(--line);display:flex;gap:16px;align-items:baseline}
h1{font-size:18px;margin:0}.muted{color:var(--muted)}main{padding:20px 24px;display:grid;grid-template-columns:360px 1fr;gap:18px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:14px}
button{background:#203142;color:var(--fg);border:1px solid #38506a;border-radius:7px;padding:8px 11px;cursor:pointer;margin:0 6px 6px 0}
button:hover{border-color:var(--accent)}button.primary{background:#164a3f;border-color:#2d8b73}
textarea{width:100%;height:300px;background:#0b1016;color:var(--fg);border:1px solid var(--line);border-radius:8px;padding:10px;box-sizing:border-box}
pre{white-space:pre-wrap;overflow:auto;max-height:68vh;background:#0b1016;border:1px solid var(--line);border-radius:8px;padding:12px}
.badge{display:inline-block;padding:2px 7px;border:1px solid var(--line);border-radius:20px;color:var(--muted)}
</style>
</head>
<body>
<header><h1>DimCluster Workbench</h1><span class="badge" id="health">checking</span><span class="muted">local-only deterministic diagnostics</span></header>
<main>
<section class="panel">
  <div class="muted">Config override (JSON)</div>
  <textarea id="config"></textarea>
  <div style="margin-top:10px">
    <button class="primary" onclick="run('auto',false)">Run incremental</button>
    <button onclick="run('full',false)">Run full</button>
    <button onclick="run('auto',true)">Resume + verify</button>
    <button onclick="post('/api/stop',{})">Stop after tier</button>
    <button onclick="show('/api/status')">Status</button>
    <button onclick="show('/api/fingerprints')">Fingerprints</button>
    <button onclick="show('/api/report')">Report</button>
    <button onclick="rationale()">Rationale</button>
  </div>
  <div class="muted">Rationale tier</div>
  <input id="tier" value="small" style="width:100%;background:#0b1016;color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:7px;box-sizing:border-box">
</section>
<section class="panel"><div class="muted" id="title">Response</div><pre id="out">Ready.</pre></section>
</main>
<script>
const cfg = document.getElementById('config');
const out = document.getElementById('out');
const configText = __CONFIG__;
cfg.value = JSON.stringify(configText,null,2);
async function req(url, body){
  const opts = body === undefined ? {} : {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)};
  const r = await fetch(url,opts); const text = await r.text();
  try{return {status:r.status,data:JSON.parse(text)}}catch(e){return {status:r.status,data:text}}
}
function render(title,obj){document.getElementById('title').textContent=title;out.textContent=JSON.stringify(obj,null,2)}
async function show(url){const r=await req(url);render(url,r.data)}
async function post(url,body,title){const r=await req(url,body);render(title||url,r.data)}
async function run(mode,verify){let c;try{c=JSON.parse(cfg.value)}catch(e){render('config error',{error:String(e)});return}render('run request',{mode,verify_full:verify});const r=await req('/api/run',{config:c,mode,verify_full:verify,background:false});render('run result',r.data)}
async function rationale(){const t=encodeURIComponent(document.getElementById('tier').value);show('/api/rationale?tier='+t)}
fetch('/api/health').then(r=>r.json()).then(x=>{document.getElementById('health').textContent=x.ok?'healthy':'error';document.getElementById('health').style.color=x.ok?'var(--accent)':'var(--bad)'});
</script>
</body>
</html>
"""


class ServerContext:
    def __init__(self, root: Path, config: dict[str, Any]) -> None:
        self.root = root.resolve()
        self.config = copy.deepcopy(config)
        self.config_lock = threading.Lock()
        self.run_lock = threading.Lock()
        self.result_lock = threading.Lock()
        self.running = False
        self.last_result: dict[str, Any] | None = None
        self.last_error: dict[str, Any] | None = None

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    def workbench(self, config: dict[str, Any] | None = None) -> Workbench:
        with self.config_lock:
            active = copy.deepcopy(self.config if config is None else config)
        return Workbench(self.root, active)

    def update_config(self, config: dict[str, Any]) -> None:
        validate_config(config)
        with self.config_lock:
            self.config = copy.deepcopy(config)

    def _store_result(self, result: dict[str, Any]) -> None:
        with self.result_lock:
            self.last_result = result
            self.last_error = None

    def _store_error(self, error: WorkbenchError) -> None:
        with self.result_lock:
            self.last_error = error.to_dict()

    def execute_run(self, request: dict[str, Any]) -> dict[str, Any]:
        with self.config_lock:
            current = copy.deepcopy(self.config)
        config = _merge_request_config(request, current)
        self.update_config(config)
        workbench = self.workbench(config)
        try:
            result = workbench.run(
                mode=str(request.get("mode", "auto")),
                reset=bool(request.get("reset", False)),
                verify_full=bool(request.get("verify_full", False)),
                stop_after_tier=request.get("stop_after_tier"),
            )
            self._store_result(result)
            return result
        except WorkbenchError as exc:
            self._store_error(exc)
            raise


def _merge_request_config(
    request: dict[str, Any],
    base_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    override = request.get("config") or {}
    if not isinstance(override, dict):
        raise WorkbenchError("E_INVALID_CONFIG", "request.config must be an object")
    base = base_config if base_config is not None else DEFAULT_CONFIG
    config = deep_merge(copy.deepcopy(DEFAULT_CONFIG), base)
    config = deep_merge(config, override)
    overrides = request.get("overrides") or []
    if not isinstance(overrides, list):
        raise WorkbenchError("E_INVALID_CONFIG", "request.overrides must be a list")
    config = apply_overrides(config, [str(value) for value in overrides])
    validate_config(config)
    return config


class Handler(BaseHTTPRequestHandler):
    server: "WorkbenchHTTPServer"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep the local terminal deterministic; requests are visible in API state.
        return

    @property
    def context(self) -> ServerContext:
        return self.server.context

    def _read_json(self) -> dict[str, Any]:
        length_text = self.headers.get("Content-Length", "0")
        try:
            length = int(length_text)
        except ValueError as exc:
            raise WorkbenchError("E_INVALID_CONFIG", "Invalid Content-Length") from exc
        if length > 2_000_000:
            raise WorkbenchError("E_INVALID_CONFIG", "Request body is too large")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WorkbenchError("E_INVALID_CONFIG", "Request body must be valid JSON") from exc
        if not isinstance(value, dict):
            raise WorkbenchError("E_INVALID_CONFIG", "Request JSON must be an object")
        return value

    def _send(self, status: int, payload: Any, content_type: str = "application/json") -> None:
        if content_type == "application/json":
            body = canonical_json_text(payload, pretty=True).encode("utf-8")
        else:
            body = str(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _ok(self, payload: Any, status: int = 200) -> None:
        self._send(status, {"ok": True, **payload} if isinstance(payload, dict) else payload)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path in {"/", "/index.html"}:
                config_json = canonical_json_text(self.context.config)
                page = HTML_PAGE.replace("__CONFIG__", config_json)
                self._send(200, page, "text/html")
                return
            if parsed.path == "/api/health":
                self._ok(
                    {
                        "service": "dimcluster-workbench",
                        "version": __version__,
                        "config_sha256": config_fingerprint(self.context.config),
                        "running": self.context.running,
                    }
                )
                return
            if parsed.path == "/api/status":
                workbench = self.context.workbench()
                status = workbench.state.status()
                status["running"] = self.context.running
                with self.context.result_lock:
                    status["last_result"] = self.context.last_result
                    status["last_error"] = self.context.last_error
                self._ok(status)
                return
            if parsed.path == "/api/fingerprints":
                self._ok(self.context.workbench().fingerprints())
                return
            if parsed.path == "/api/report":
                self._ok(self.context.workbench().report())
                return
            if parsed.path == "/api/rationale":
                tier = (query.get("tier") or [""])[0]
                if not tier:
                    raise WorkbenchError("E_INVALID_CONFIG", "tier query parameter is required")
                self._ok(self.context.workbench().rationale(tier))
                return
            if parsed.path == "/api/runs/latest":
                with self.context.result_lock:
                    self._ok(
                        {
                            "running": self.context.running,
                            "last_result": self.context.last_result,
                            "last_error": self.context.last_error,
                        }
                    )
                return
            if parsed.path == "/api/config":
                self._ok({"config": self.context.config})
                return
            raise WorkbenchError("E_INVALID_STAGE", f"Unknown endpoint: {parsed.path}")
        except WorkbenchError as exc:
            self._send(exc.http_status, exc.to_dict())
        except Exception as exc:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            request = self._read_json()
            if parsed.path == "/api/import":
                manifest_path = request.get("manifest_path")
                workbench = self.context.workbench()
                audit = workbench.import_samples(
                    Path(manifest_path) if manifest_path else None
                )
                self._ok(audit)
                return
            if parsed.path == "/api/config/validate":
                with self.context.config_lock:
                    current = copy.deepcopy(self.context.config)
                config = _merge_request_config(
                    {"config": request.get("config", request)}, current
                )
                self._ok({"valid": True, "config_sha256": config_fingerprint(config)})
                return
            if parsed.path == "/api/stop":
                workbench = self.context.workbench()
                result = workbench.state.request_stop(str(request.get("reason", "http_requested")))
                self._ok(result)
                return
            if parsed.path in {"/api/run", "/api/resume"}:
                if parsed.path == "/api/resume":
                    request = {**request, "mode": "auto"}
                background = bool(request.get("background", False))
                if not self.context.run_lock.acquire(blocking=False):
                    raise WorkbenchError(
                        "E_INVALID_STAGE",
                        "Another run is already active in this workbench process",
                        {"running": True},
                    )
                if background:
                    self.context.running = True

                    def worker() -> None:
                        try:
                            self.context.execute_run(request)
                        except WorkbenchError:
                            pass
                        except Exception as exc:
                            self.context._store_error(
                                WorkbenchError("E_INTERNAL", str(exc), {})
                            )
                        finally:
                            self.context.running = False
                            self.context.run_lock.release()

                    threading.Thread(target=worker, name="dimcluster-run", daemon=True).start()
                    self._ok(
                        {
                            "accepted": True,
                            "running": True,
                            "status_endpoint": "/api/status",
                            "run_endpoint": "/api/runs/latest",
                        },
                        status=202,
                    )
                    return
                self.context.running = True
                try:
                    result = self.context.execute_run(request)
                finally:
                    self.context.running = False
                    self.context.run_lock.release()
                self._ok(result)
                return
            raise WorkbenchError("E_INVALID_STAGE", f"Unknown endpoint: {parsed.path}")
        except WorkbenchError as exc:
            self._send(exc.http_status, exc.to_dict())
        except Exception as exc:
            self._send(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"ok": False, "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            )


class WorkbenchHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        context: ServerContext,
    ) -> None:
        self.context = context
        super().__init__(address, Handler)


def create_server(
    root: Path,
    config: dict[str, Any],
    host: str = "127.0.0.1",
    port: int = 0,
) -> tuple[WorkbenchHTTPServer, ServerContext]:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise WorkbenchError(
            "E_INVALID_CONFIG",
            "HTTP interface must bind to a loopback address",
            {"host": host},
        )
    validate_config(config)
    context = ServerContext(root, config)
    server = WorkbenchHTTPServer((host, int(port)), context)
    server.context = context
    return server, context


def serve_forever(
    *,
    root: Path,
    config: dict[str, Any],
    host: str = "127.0.0.1",
    port: int = 0,
    pretty: bool = False,
) -> int:
    server, _ = create_server(root, config, host, port)
    actual_host, actual_port = server.server_address[:2]
    ready = {
        "ok": True,
        "event": "http_ready",
        "host": actual_host,
        "port": actual_port,
        "url": f"http://{actual_host}:{actual_port}/",
        "bridge": request_loopback_probe(actual_host, actual_port),
    }
    print(canonical_json_text(ready, pretty=pretty), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def request_loopback_probe(host: str, port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.settimeout(0.3)
    try:
        probe.connect((host, int(port)))
        return True
    except OSError:
        return False
    finally:
        probe.close()
