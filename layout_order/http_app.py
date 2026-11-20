"""本地 HTTP 应用（仅标准库 http.server，无外部依赖）。

端点：
    POST /api/samples/import        导入样本
    POST /api/analyze               触发分析 {"doc_id"?: ...}
    POST /api/stop                  请求优雅停止
    POST /api/resume                恢复分析
    GET  /api/status[?doc_id=...]   阶段状态与版本指纹
    GET  /api/documents/<id>/sequence
    GET  /api/documents/<id>/spanning
    GET  /api/documents/<id>/result 导出结果
    GET  /api/invalidations
    POST /api/documents/<id>/update 增量更新与全量比对
    GET  /api/health
"""

from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .config import DEFAULT_CONFIG
from .engine import PipelineEngine
from .errors import HTTP_STATUS, LayoutError
from .generator import generate_documents
from .hashing import canonical_json
from .stages import normalize as normalize_stage


class HttpState:
    def __init__(self, workspace: str, sample_path: str | None = None):
        self.engine = PipelineEngine(workspace, DEFAULT_CONFIG)
        self.workspace = os.path.abspath(workspace)
        self.sample_path = sample_path or os.path.join(
            self.workspace, "samples", "documents.json"
        )
        self.stop_file = os.path.join(self.workspace, "STOP_REQUESTED")
        self.run_lock = threading.Lock()

    def load_samples(self) -> list[dict[str, Any]]:
        with open(self.sample_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data["documents"] if isinstance(data, dict) else data


class LayoutHandler(BaseHTTPRequestHandler):
    state: HttpState  # 由工厂函数注入

    def log_message(self, fmt: str, *args: Any) -> None:  # 精简日志
        sys_log = f"[http] {self.address_string()} {fmt % args}"
        print(sys_log, flush=True)

    def _send(self, status: int, payload: Any) -> None:
        body = (canonical_json(payload) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _error(self, exc: LayoutError) -> None:
        self._send(HTTP_STATUS.get(exc.code, 500), exc.to_dict())

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            state = self.state

            if path == "/api/health":
                self._send(200, {"status": "ok", "workspace": state.workspace})
                return
            if path == "/api/status":
                doc_id = query.get("doc_id", [None])[0]
                if doc_id:
                    if doc_id not in state.engine.store.list_documents():
                        raise LayoutError("E_DOC_NOT_FOUND",
                                          f"工作区不存在文档 {doc_id}", {"doc_id": doc_id})
                    self._send(200, state.engine.document_status(doc_id))
                else:
                    self._send(200, {"documents": [
                        state.engine.document_status(doc)
                        for doc in state.engine.store.list_documents()
                    ]})
                return
            if path == "/api/invalidations":
                self._send(200, {"events": state.engine.store.read_invalidation_log()})
                return

            prefix_models = {
                "/sequence": ("order", lambda p: p["product"]),
                "/spanning": ("summary", lambda p: p["product"]["spanning_stats"]),
                "/result": ("summary", lambda p: p["product"]),
            }
            for suffix, (stage, selector) in prefix_models.items():
                marker = "/api/documents/"
                if path.startswith(marker) and path.endswith(suffix):
                    doc_id = path[len(marker):-len(suffix)]
                    record = state.engine.store.load_stage(doc_id, stage)
                    if record is None:
                        raise LayoutError("E_NOT_FOUND", f"文档 {doc_id} 尚无 {stage} 产物",
                                          {"doc_id": doc_id, "stage": stage})
                    self._send(200, selector(record))
                    return
            self._send(404, {"code": "E_NOT_FOUND", "message": f"未知路径 {path}",
                             "details": {}})
        except LayoutError as exc:
            self._error(exc)
        except (FileNotFoundError, KeyError, json.JSONDecodeError) as exc:
            self._send(404, {"code": "E_NOT_FOUND", "message": str(exc), "details": {}})

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path.rstrip("/") or "/"
            state = self.state

            if path == "/api/samples/import":
                payload = self._body()
                documents = payload.get("documents")
                if documents is None and os.path.exists(state.sample_path):
                    documents = state.load_samples()
                if documents is None:
                    documents = generate_documents()
                for doc in documents:
                    normalize_stage.validate_raw_document(doc)
                    state.engine.store.save_input(doc)
                self._send(200, {"imported": [doc["doc_id"] for doc in documents]})
                return

            if path == "/api/analyze":
                if not state.run_lock.acquire(blocking=False):
                    raise LayoutError("E_STALE_CHECKPOINT",
                                      "已有分析任务在运行", {"reason": "busy"})
                try:
                    payload = self._body()
                    docs = state.load_samples()
                    doc_id = payload.get("doc_id")
                    if doc_id:
                        docs = [doc for doc in docs if doc["doc_id"] == doc_id]
                        if not docs:
                            raise LayoutError("E_DOC_NOT_FOUND",
                                              f"样本中不存在文档 {doc_id}", {"doc_id": doc_id})

                    def stop_check() -> bool:
                        return os.path.exists(state.stop_file)

                    reports = state.engine.run(docs, stop_check)
                finally:
                    state.run_lock.release()
                self._send(200, {"reports": reports})
                return

            if path == "/api/stop":
                with open(state.stop_file, "w", encoding="utf-8") as handle:
                    handle.write("stop\n")
                self._send(200, {"stop_requested": True})
                return

            if path == "/api/resume":
                if os.path.exists(state.stop_file):
                    os.remove(state.stop_file)
                docs = state.load_samples()
                reports = state.engine.run(docs, lambda: False)
                self._send(200, {"reports": reports, "resumed": True})
                return

            if path.startswith("/api/documents/") and path.endswith("/update"):
                doc_id = path[len("/api/documents/"):-len("/update")]
                target = next((doc for doc in state.load_samples()
                               if doc["doc_id"] == doc_id), None)
                if target is None:
                    raise LayoutError("E_DOC_NOT_FOUND", f"样本中不存在文档 {doc_id}",
                                      {"doc_id": doc_id})
                self._send(200, state.engine.update_document(target))
                return

            self._send(404, {"code": "E_NOT_FOUND", "message": f"未知路径 {path}",
                             "details": {}})
        except LayoutError as exc:
            self._error(exc)
        except (FileNotFoundError, KeyError, json.JSONDecodeError) as exc:
            self._send(400, {"code": "E_INVALID_STAGE", "message": str(exc), "details": {}})


def create_server(host: str, port: int, workspace: str,
                  sample_path: str | None = None) -> ThreadingHTTPServer:
    state = HttpState(workspace, sample_path)
    handler = type("BoundLayoutHandler", (LayoutHandler,), {"state": state})
    server = ThreadingHTTPServer((host, port), handler)
    return server


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="版面阅读顺序分析本地 HTTP 服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--workspace", default="workspace")
    parser.add_argument("--sample-path", default=None)
    args = parser.parse_args(argv)

    server = create_server(args.host, args.port, args.workspace, args.sample_path)
    print(f"[http] layout_order serving on http://{args.host}:{args.port} "
          f"(workspace={os.path.abspath(args.workspace)})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
