from __future__ import annotations

import base64
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .errors import BusinessError, E_INPUT_INVALID, E_INTERNAL, E_NOT_FOUND
from .service import Service

MAX_BODY_BYTES = 16 * 1024 * 1024

DASHBOARD = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>JoinCost Diagnostic Workbench</title>
<style>
:root{color-scheme:light dark;font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
body{max-width:1180px;margin:28px auto;padding:0 18px;line-height:1.45}
h1{font-size:24px;margin-bottom:4px}h2{font-size:16px;margin-top:24px}
button{margin:3px 6px 3px 0;padding:7px 12px;cursor:pointer}
pre{white-space:pre-wrap;overflow-wrap:anywhere;border:1px solid #8886;padding:12px;border-radius:7px;min-height:80px}
.row{display:grid;grid-template-columns:1fr 1fr;gap:16px}@media(max-width:800px){.row{grid-template-columns:1fr}}
.muted{opacity:.68}
</style>
</head>
<body>
<h1>JoinCost Diagnostic Workbench</h1>
<div class="muted">Loopback-only deterministic join ordering, statistics interchange, and crash recovery.</div>
<h2>Actions</h2>
<button onclick="importPath('samples/stats_v2.bin')">Import binary snapshot</button>
<button onclick="importPath('samples/stats_v2.jsonl')">Import JSONL snapshot</button>
<button onclick="runPlan()">Build plan from current snapshot</button>
<button onclick="call('/v1/recovery','outRecovery')">Show recovery report</button>
<button onclick="call('/v1/snapshot','outSnapshot')">Show current snapshot</button>
<div class="row">
  <div><h2>Plan / API result</h2><pre id="outPlan">No request yet.</pre></div>
  <div><h2>Snapshot / recovery</h2><pre id="outSnapshot">No request yet.</pre><pre id="outRecovery"></pre></div>
</div>
<script>
async function call(url, id, body){
  const r = await fetch(url, body ? {method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)} : {});
  const text = await r.text();
  let data; try{data=JSON.parse(text)}catch(e){data=text}
  document.getElementById(id).textContent = JSON.stringify(data,null,2);
  return data;
}
async function importPath(path){
  const data = await call('/v1/snapshots/import','outPlan',{path});
  await call('/v1/snapshot','outSnapshot');
  return data;
}
async function runPlan(){
  const snap = await call('/v1/snapshot','outSnapshot');
  if(!snap.current_snapshot){document.getElementById('outPlan').textContent='Import a snapshot first.';return}
  const preds = await (await fetch('/samples/predicates.json')).json();
  const request = {
    request_id:'ui-'+Date.now(),
    snapshot_version:snap.current_snapshot.snapshot_version,
    predicates:preds,
    options:{max_relations:8,tolerance_ppm:1000}
  };
  await call('/v1/plans','outPlan',request);
}
call('/v1/snapshot','outSnapshot');
</script>
</body>
</html>"""


class JoinCostHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], service: Service):
        self.service = service
        super().__init__(server_address, JoinCostHandler)


class JoinCostHandler(BaseHTTPRequestHandler):
    server: JoinCostHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, status: int, payload: dict[str, Any] | list[Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _send_html(self, status: int, html: str) -> None:
        encoded = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _send_error(self, error: BusinessError) -> None:
        self._send_json(error.http_status, error.to_dict())

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise BusinessError(E_INPUT_INVALID, "invalid Content-Length") from exc
        if length <= 0:
            raise BusinessError(E_INPUT_INVALID, "request body is required")
        if length > MAX_BODY_BYTES:
            raise BusinessError(
                E_INPUT_INVALID,
                "request body exceeds the local size limit",
                {"limit_bytes": MAX_BODY_BYTES},
            )
        raw = self.rfile.read(length)
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BusinessError(E_INPUT_INVALID, "request body is not valid JSON") from exc
        if not isinstance(parsed, dict):
            raise BusinessError(E_INPUT_INVALID, "request body must be a JSON object")
        return parsed

    def _loopback_check(self) -> None:
        client = self.client_address[0]
        if client not in {"127.0.0.1", "::1"}:
            raise BusinessError(
                E_INPUT_INVALID, "this service only accepts loopback clients", {"client": client}
            )

    def do_GET(self) -> None:
        try:
            self._loopback_check()
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path == "/":
                self._send_html(HTTPStatus.OK, DASHBOARD)
                return
            if path == "/health":
                self._send_json(HTTPStatus.OK, self.server.service.health())
                return
            if path == "/v1/snapshot":
                self._send_json(HTTPStatus.OK, self.server.service.current_snapshot())
                return
            if path == "/v1/snapshots":
                self._send_json(HTTPStatus.OK, self.server.service.current_snapshot())
                return
            if path.startswith("/v1/snapshots/"):
                version_text = path.rsplit("/", 1)[-1]
                if not version_text.isdigit():
                    raise BusinessError(E_INPUT_INVALID, "snapshot version must be an integer")
                self._send_json(
                    HTTPStatus.OK, self.server.service.snapshot(int(version_text))
                )
                return
            if path.startswith("/v1/plans/"):
                request_id = path.rsplit("/", 1)[-1]
                self._send_json(HTTPStatus.OK, self.server.service.get_plan(request_id))
                return
            if path.startswith("/v1/requests/"):
                request_id = path.rsplit("/", 1)[-1]
                self._send_json(HTTPStatus.OK, self.server.service.request_status(request_id))
                return
            if path == "/v1/imports":
                values = parse_qs(parsed.query)
                try:
                    limit = int(values.get("limit", ["50"])[0])
                except ValueError as exc:
                    raise BusinessError(E_INPUT_INVALID, "limit must be an integer") from exc
                if limit < 1 or limit > 500:
                    raise BusinessError(E_INPUT_INVALID, "limit must be in [1, 500]")
                self._send_json(HTTPStatus.OK, self.server.service.imports(limit))
                return
            if path == "/v1/recovery":
                self._send_json(HTTPStatus.OK, self.server.service.recovery())
                return
            if path == "/samples/predicates.json":
                sample = self.server.service.workspace_root / "samples" / "predicates.json"
                if not sample.is_file():
                    raise BusinessError(E_NOT_FOUND, "sample predicates are not available")
                self._send_json(HTTPStatus.OK, json.loads(sample.read_text(encoding="utf-8")))
                return
            raise BusinessError(E_NOT_FOUND, "endpoint was not found", {"path": path})
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(
                BusinessError(E_INTERNAL, "unexpected server error", {"reason": str(exc)})
            )

    def do_POST(self) -> None:
        try:
            self._loopback_check()
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            body = self._read_json()
            if path == "/v1/snapshots/import":
                if "content_base64" in body:
                    try:
                        data = base64.b64decode(body["content_base64"], validate=True)
                    except Exception as exc:
                        raise BusinessError(
                            E_INPUT_INVALID, "content_base64 is not valid base64"
                        ) from exc
                    result = self.server.service.import_snapshot_bytes(
                        data,
                        source=body.get("source", "inline-base64"),
                        format_hint=body.get("format_hint"),
                        expected_version=body.get("expected_version"),
                        expected_digest=body.get("expected_digest"),
                    )
                elif "path" in body:
                    result = self.server.service.import_snapshot_path(
                        body["path"],
                        format_hint=body.get("format_hint"),
                        expected_version=body.get("expected_version"),
                        expected_digest=body.get("expected_digest"),
                    )
                else:
                    raise BusinessError(
                        E_INPUT_INVALID,
                        "snapshot import requires path or content_base64",
                    )
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/plans":
                result = self.server.service.submit_plan(body)
                self._send_json(HTTPStatus.OK, result)
                return
            if path == "/v1/recovery":
                result = self.server.service.recover_when_idle()
                self._send_json(HTTPStatus.OK, result)
                return
            raise BusinessError(E_NOT_FOUND, "endpoint was not found", {"path": path})
        except BusinessError as exc:
            self._send_error(exc)
        except Exception as exc:
            self._send_error(
                BusinessError(E_INTERNAL, "unexpected server error", {"reason": str(exc)})
            )

    def _method_not_allowed(self) -> None:
        try:
            self._loopback_check()
            raise BusinessError(
                E_INPUT_INVALID,
                "HTTP method is not supported by this endpoint",
                {"method": self.command, "path": self.path},
            )
        except BusinessError as exc:
            self._send_error(exc)

    do_PUT = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_DELETE = _method_not_allowed


def create_server(
    service: Service, host: str = "127.0.0.1", port: int = 0
) -> JoinCostHTTPServer:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError("HTTP server must bind to a loopback address")
    server = JoinCostHTTPServer((host, port), service)
    return server


def serve_forever(server: JoinCostHTTPServer) -> None:
    server.serve_forever(poll_interval=0.2)

