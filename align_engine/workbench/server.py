"""Local HTTP diagnostic workbench (stdlib only, no network required).

Serves a JSON API plus a minimal single-page dashboard on 127.0.0.1.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .. import service
from ..errors import EngineError

PAGE = """<!doctype html>
<html lang="zh"><head><meta charset="utf-8">
<title>对齐引擎诊断工作台</title>
<style>
body{font-family:system-ui,monospace;margin:2rem;background:#0f1420;color:#dde}
button{margin:.2rem;padding:.4rem .8rem}pre{background:#1a2233;padding:1rem;
overflow:auto;max-height:60vh}input{width:22rem}
</style></head><body>
<h2>多模态特征对齐 · 诊断工作台</h2>
<div>
<button onclick="call('GET','/api/status')">状态</button>
<button onclick="call('GET','/api/fingerprints')">版本指纹</button>
<button onclick="call('GET','/api/summary')">对齐摘要</button>
<button onclick="call('GET','/api/uncovered')">未覆盖/降级</button>
<button onclick="call('GET','/api/checkpoints')">检查点</button>
<button onclick="call('GET','/api/invalidations')">失效事件</button>
</div>
<div>
<button onclick="call('POST','/api/run',{})">运行全部阶段</button>
<button onclick="call('POST','/api/run',{stage:'validate'})">仅维度校验</button>
<button onclick="call('POST','/api/stop',{})">请求停止</button>
<button onclick="call('POST','/api/resume',{})">恢复运行</button>
<button onclick="call('POST','/api/export',{})">导出对齐工程</button>
<button onclick="call('POST','/api/reset',{})">重置工作区</button>
</div>
<div>
<input id="manifest" placeholder="清单路径 manifest_v2.yaml">
<input id="shards" placeholder="分片目录 samples/shards">
<button onclick="call('POST','/api/import',{manifest_path:val('manifest'),shards_dir:val('shards')})">导入</button>
</div>
<pre id="out">就绪。</pre>
<script>
function val(id){return document.getElementById(id).value}
async function call(method,url,body){
  const r=await fetch(url,{method,headers:{'Content-Type':'application/json'},
    body:method==='POST'?JSON.stringify(body||{}):undefined});
  const t=await r.text();
  document.getElementById('out').textContent=url+' -> '+r.status+'\\n'+t;
}
</script></body></html>"""


class WorkbenchHandler(BaseHTTPRequestHandler):
    workspace = "."

    def log_message(self, *args):  # quiet
        pass

    # ------------------------------------------------------------- helpers
    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False, indent=2,
                          sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_page(self):
        body = PAGE.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _dispatch(self, method, path, params):
        ws = self.workspace
        if method == "GET" and path == "/":
            return ("page", None)
        if method == "GET" and path == "/api/health":
            return ("json", {"ok": True})
        if method == "GET" and path == "/api/status":
            return ("json", service.do_status(ws))
        if method == "GET" and path == "/api/fingerprints":
            return ("json", service.do_fingerprints(ws))
        if method == "GET" and path == "/api/summary":
            return ("json", service.do_summary(ws))
        if method == "GET" and path == "/api/uncovered":
            return ("json", service.do_uncovered(ws))
        if method == "GET" and path == "/api/checkpoints":
            return ("json", service.do_checkpoints(ws))
        if method == "GET" and path == "/api/invalidations":
            return ("json", service.do_invalidations(ws))
        if method == "POST" and path == "/api/import":
            return ("json", service.do_import(
                ws, params["manifest_path"], params["shards_dir"],
                params.get("extra_shards_dir")))
        if method == "POST" and path == "/api/run":
            return ("json", service.do_run(
                ws, stage=params.get("stage"),
                stop_after_units=params.get("stop_after_units")))
        if method == "POST" and path == "/api/stop":
            return ("json", service.do_stop(ws))
        if method == "POST" and path == "/api/resume":
            return ("json", service.do_run(ws))
        if method == "POST" and path == "/api/reset":
            return ("json", service.do_reset(ws))
        if method == "POST" and path == "/api/export":
            return ("json", service.do_export(ws))
        if method == "POST" and path == "/api/convert":
            return ("json", service.do_convert(
                params["src"], params["src_format"],
                params["dst"], params["dst_format"],
                verify=params.get("verify", False)))
        if method == "POST" and path == "/api/upgrade":
            return ("json", service.do_upgrade(
                params["v1_path"], params["v2_path"], params["shards_dir"],
                params["work_dir"], out_path=params.get("out_path")))
        if method == "POST" and path == "/api/config":
            return ("json", service.do_config_set(
                ws, params["key"], params["value"]))
        raise EngineError("E_NOT_FOUND", "unknown endpoint",
                          {"method": method, "path": path})

    def _handle(self, method):
        try:
            path = urlparse(self.path).path
            params = self._body() if method == "POST" else {}
            kind, payload = self._dispatch(method, path, params)
            if kind == "page":
                self._send_page()
            else:
                self._send_json(payload)
        except EngineError as exc:
            self._send_json({"error": exc.to_dict()}, status=400)
        except Exception as exc:  # pragma: no cover - defensive
            self._send_json({"error": {"code": "E_INTERNAL",
                                       "message": str(exc)}}, status=500)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


def make_server(workspace, host, port):
    handler = type("BoundHandler", (WorkbenchHandler,),
                   {"workspace": workspace})
    return ThreadingHTTPServer((host, port), handler)


def serve(workspace, host, port):
    server = make_server(workspace, host, port)
    actual_host, actual_port = server.server_address
    print("诊断工作台已启动: http://{}:{}/ (工作区: {})".format(
        actual_host, actual_port, workspace))
    print("按 Ctrl+C 停止。")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def serve_in_thread(workspace, host="127.0.0.1", port=0):
    """Start the workbench on an ephemeral port (used by tests)."""
    server = make_server(workspace, host, port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, server.server_address[1]
