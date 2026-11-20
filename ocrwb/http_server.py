"""Local-only HTTP diagnostic workbench built on the standard library."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .checkpoint import CheckpointStore
from .config import WorkbenchConfig
from .errors import WorkbenchError
from .hashing import canonical_json
from .pipeline import Pipeline

DASHBOARD_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>OCR 诊断工作台</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 24px; color: #1f2937; }
    h1 { font-size: 20px; }
    button { margin: 4px 6px 4px 0; padding: 6px 12px; cursor: pointer; }
    pre { background: #0f172a; color: #e2e8f0; padding: 14px; border-radius: 8px;
          max-height: 70vh; overflow: auto; font-size: 12px; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .card { border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; min-width: 180px; }
  </style>
</head>
<body>
  <h1>OCR 文字检测与识别可视化诊断工作台</h1>
  <div>
    <button onclick="call('/api/run','POST')">执行流水线</button>
    <button onclick="call('/api/stop','POST')">请求停止</button>
    <button onclick="call('/api/resume','POST')">恢复执行</button>
    <button onclick="load('/api/status')">阶段状态</button>
    <button onclick="load('/api/fingerprints')">版本指纹</button>
    <button onclick="load('/api/report')">诊断报告</button>
    <button onclick="load('/api/export')">导出结果</button>
  </div>
  <div class="row" id="cards"></div>
  <pre id="output">点击按钮调用本地接口…</pre>
<script>
async function call(url, method) {
  const resp = await fetch(url, { method: method || 'GET' });
  show(resp);
}
async function load(url) { const resp = await fetch(url); show(resp); }
async function show(resp) {
  const data = await resp.json();
  document.getElementById('output').textContent = JSON.stringify(data, null, 2);
  if (data.global) {
    const g = data.global;
    document.getElementById('cards').innerHTML =
      ['检测召回 ' + g.detection_recall, '整行准确率 ' + g.exact_match_accuracy,
       '字符准确率 ' + g.character_accuracy, '平均距离 ' + g.mean_match_distance,
       '检测成本 ' + g.detect_cost, '识别成本 ' + g.recognize_cost]
      .map(t => '<div class="card">' + t + '</div>').join('');
  }
}
load('/api/status');
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "OcrWorkbench/1.0"

    def log_message(self, format, *args):  # quiet deterministic logging
        return

    def _send(self, payload, status: int = 200, content_type: str = "application/json") -> None:
        body = (
            canonical_json(payload).encode("utf-8")
            if content_type == "application/json"
            else payload.encode("utf-8")
        )
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, exc: WorkbenchError) -> None:
        self._send(exc.to_dict(), status=409 if exc.code in {"E_STALE_CHECKPOINT", "E_INVALID_STAGE"} else 422)

    def _pipeline(self) -> Pipeline:
        return self.server.pipeline  # type: ignore[attr-defined]

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path, query = parsed.path, parse_qs(parsed.query)
        try:
            if path == "/":
                self._send(DASHBOARD_HTML, content_type="text/html")
            elif path == "/api/status":
                self._send(self._pipeline().store.status())
            elif path == "/api/fingerprints":
                store = self._pipeline().store
                self._send(
                    {
                        "config_version": store.load_config().version(),
                        "input_fingerprints": store.load_input_fingerprints(),
                        "journals": {
                            shard["image_id"]: store.read_journal(shard["image_id"])
                            for shard in store.status()["shards"]
                        },
                    }
                )
            elif path == "/api/report":
                self._send(self._pipeline().load_report())
            elif path == "/api/decision":
                image_id = query.get("image_id", [""])[0]
                self._send(self._pipeline().store.load_artifact(image_id, "decision"))
            elif path == "/api/export":
                self._send(self._export())
            else:
                self._send({"code": "E_INVALID_STAGE", "message": f"unknown route {path}"}, status=404)
        except WorkbenchError as exc:
            self._send_error(exc)
        except FileNotFoundError as exc:
            self._send({"code": "E_INVALID_STAGE", "message": str(exc)}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path, query = parsed.path, parse_qs(parsed.query)
        pipeline = self._pipeline()
        try:
            if path == "/api/run":
                if query.get("allow_stop") != ["true"]:
                    pipeline.clear_stop()
                self._send(pipeline.run().to_dict())
            elif path == "/api/stop":
                pipeline.request_stop()
                self._send({"stop_requested": True})
            elif path == "/api/resume":
                pipeline.clear_stop()
                self._send(pipeline.run().to_dict())
            else:
                self._send({"code": "E_INVALID_STAGE", "message": f"unknown route {path}"}, status=404)
        except WorkbenchError as exc:
            self._send_error(exc)

    def _export(self) -> list[dict]:
        store = self._pipeline().store
        manifest = store.load_manifest()
        export = []
        for shard in manifest["shards"]:
            decision = store.load_artifact(shard["image_id"], "decision")
            recognition = store.load_artifact(shard["image_id"], "recognize")
            export.append(
                {
                    "image_id": shard["image_id"],
                    "group": shard["group"],
                    "detection": decision["refined_detection"],
                    "decision": decision["decision"],
                    "recognition": recognition["recognition"],
                }
            )
        return export


def build_server(workspace: str, sample_dir: str, config: WorkbenchConfig | None = None, port: int = 8088) -> ThreadingHTTPServer:
    pipeline = Pipeline(workspace, sample_dir, config)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.pipeline = pipeline  # type: ignore[attr-defined]
    return server


def serve(workspace: str, sample_dir: str, port: int = 8088) -> None:
    server = build_server(workspace, sample_dir, port=port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"OCR workbench listening on http://127.0.0.1:{port}")
    try:
        thread.join()
    except KeyboardInterrupt:
        server.shutdown()
