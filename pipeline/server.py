"""本地 HTTP 接口（仅监听 127.0.0.1，无外部服务依赖）。

路由：
  POST /generate                 生成确定性演示数据
  POST /import?kind=train|infer|config  上传/导入本地文件
  POST /run                      body: {"through_stage": "..."} 分阶段执行
  GET  /status                   检查点、指纹、失效事件
  POST /validate                 body: {"scope": "..."} 增量一致性校验
  POST /edit-sample              body: {"sample_id","text","label"}
  POST /stop                     请求优雅停止（演示）
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from pipeline.atomicio import canonical_json_bytes
from pipeline.data_ops import edit_training_sample
from pipeline.errors import PipelineError
from pipeline.generator import generate_workspace
from pipeline.pipeline import Pipeline


class StopController:
    def __init__(self):
        self.event = threading.Event()

    def reset(self):
        self.event.clear()


def make_handler(workspace_root, stop_controller):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # 静默默认访问日志
            return

        def _send(self, payload, status=200):
            body = canonical_json_bytes(payload)
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_error(self, exc, status=409):
            self._send(exc.to_dict(), status)

        def _body(self):
            length = int(self.headers.get("Content-Length", 0) or 0)
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path == "/status":
                try:
                    self._send(Pipeline(workspace_root).status())
                except PipelineError as exc:
                    self._send_error(exc)
                return
            self._send({"error": "E_NOT_FOUND", "message": parsed.path}, 404)

        def do_POST(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            try:
                if parsed.path == "/generate":
                    body = self._body()
                    result = generate_workspace(workspace_root, seed=int(body.get("seed", 20261122)))
                    self._send(
                        {
                            "workspace": result["workspace"].root,
                            "train_shards": result["train_paths"],
                            "infer_shards": result["infer_paths"],
                            "expected_model_version": result["expected_model_version"],
                        }
                    )
                elif parsed.path == "/import":
                    body = self._body()
                    kind = query.get("kind", [body.get("kind")])[0]
                    destination = Pipeline(workspace_root).import_file(body["path"], kind)
                    self._send({"imported": destination, "kind": kind})
                elif parsed.path == "/run":
                    body = self._body()
                    pipe = Pipeline(workspace_root)
                    report = pipe.run(
                        through_stage=body.get("through_stage", "stage4_summary"),
                        stop_event=stop_controller.event,
                    )
                    self._send(report)
                elif parsed.path == "/validate":
                    body = self._body()
                    self._send(
                        Pipeline(workspace_root).validate_incremental(
                            scope=body.get("scope", "stage4_summary")
                        )
                    )
                elif parsed.path == "/edit-sample":
                    body = self._body()
                    result = edit_training_sample(
                        workspace_root,
                        body["sample_id"],
                        body.get("text"),
                        body.get("label"),
                    )
                    self._send(result)
                elif parsed.path == "/stop":
                    stop_controller.event.set()
                    self._send({"stop_requested": True})
                else:
                    self._send({"error": "E_NOT_FOUND", "message": parsed.path}, 404)
            except PipelineError as exc:
                self._send_error(exc)
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                self._send({"error": "E_INVALID_REQUEST", "message": str(exc)}, 400)

    return Handler


def run_server(workspace_root, host="127.0.0.1", port=8765):
    controller = StopController()
    server = ThreadingHTTPServer((host, port), make_handler(workspace_root, controller))
    print(f"pipeline HTTP 服务已启动: http://{host}:{port} (工作区 {workspace_root})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
