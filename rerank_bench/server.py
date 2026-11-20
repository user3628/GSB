"""Local HTTP API bound to 127.0.0.1 on a non-privileged port."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import errors
from .errors import PipelineError
from .pipeline import Workspace
from .runner import export_summary, rerank_trace, run_pipeline, status

DEFAULT_PORT = 8791


def _json_bytes(obj) -> bytes:
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2)
            + "\n").encode("utf-8")


class Api:
    """Business operations behind the HTTP layer (also used by tests)."""

    def __init__(self, workspace_root, repo_root=None):
        self.ws = Workspace(workspace_root)
        self.repo_root = repo_root

    def handle(self, method: str, path: str, body: dict):
        parts = [p for p in path.split("/") if p]
        if method == "POST" and parts == ["import"]:
            manifest = self.ws.import_data(body["data_dir"])
            return {"imported": manifest}
        if method == "POST" and parts in (["run"], ["stages", "run"]):
            return run_pipeline(
                self.ws, serial=bool(body.get("serial", False)),
                stage_limit=body.get("stage"), repo_root=self.repo_root,
                stop_after=body.get("stop_after"))
        if method == "POST" and parts == ["stop"]:
            self.ws.request_stop()
            return {"stop_requested": True}
        if method == "POST" and parts == ["resume"]:
            self.ws.clear_stop()
            return run_pipeline(self.ws, serial=bool(body.get("serial", False)),
                                repo_root=self.repo_root)
        if method == "GET" and parts == ["status"]:
            return status(self.ws)
        if method == "GET" and parts == ["budget"]:
            return self._budget()
        if method == "GET" and parts == ["export"]:
            return export_summary(self.ws)
        if method == "GET" and len(parts) == 2 and parts[0] == "fingerprints":
            return self._fingerprints(parts[1])
        if method == "GET" and len(parts) == 3 and parts[0] == "shards" \
                and parts[2] == "trace":
            return rerank_trace(self.ws, parts[1])
        if method == "GET" and len(parts) == 4 and parts[0] == "shards" \
                and parts[2] == "artifacts":
            return self._artifact(parts[1], parts[3])
        if method == "GET" and parts == ["errors"]:
            return {"errors": self._errors()}
        raise PipelineError(
            errors.E_INVALID_STAGE, f"no route for {method} {path}",
            method=method, path=path)

    def _budget(self):
        st = status(self.ws)
        plan = st["plan"]
        last = st["last_run"]
        try:
            consumed = export_summary(self.ws)["budget"]["consumed_evaluations"]
        except PipelineError:
            consumed = 0
        return {
            "global_eval_budget": plan["global_eval_budget"],
            "reserved_evaluations": plan["reserved_evaluations"],
            "consumed_evaluations": consumed,
            "per_worker_quota": plan["per_worker_quota"],
            "max_workers": plan["max_workers"],
            "concurrency": plan["concurrency"],
            "admitted": plan["admitted"],
            "unprocessed": plan["rejected"],
            "pending_after_stop": last.get("pending") or [],
        }

    def _fingerprints(self, shard_id):
        st = status(self.ws)
        for shard in st["shards"]:
            if shard["shard_id"] == shard_id:
                return shard
        raise PipelineError(
            "E_CONFIG", f"unknown shard {shard_id}", shard_id=shard_id)

    def _errors(self):
        st = status(self.ws)
        out = []
        for shard_id, result in st["last_run"].get("shard_results", {}).items():
            if result.get("status") == "error":
                out.append(result["error"])
        return out

    def _artifact(self, shard_id, stage):
        if stage not in ("assemble", "score", "rerank", "aggregate"):
            raise PipelineError(
                errors.E_INVALID_STAGE, f"unknown stage {stage}", stage=stage)
        artifact = self.ws.store.get_artifact(shard_id, stage)
        if artifact is None:
            raise PipelineError(
                errors.E_INVALID_STAGE,
                f"artifact {shard_id}/{stage} not published yet",
                shard_id=shard_id, stage=stage)
        return artifact


def make_handler(api: Api):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, obj, code=200):
            blob = _json_bytes(obj)
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(blob)))
            self.end_headers()
            self.wfile.write(blob)

        def _dispatch(self, method):
            try:
                body = {}
                length = int(self.headers.get("Content-Length") or 0)
                if length:
                    body = json.loads(self.rfile.read(length).decode("utf-8"))
                result = api.handle(method, self.path.split("?")[0], body)
                self._send({"ok": True, "result": result})
            except PipelineError as exc:
                self._send({"ok": False, "error": exc.to_dict()}, code=409)
            except Exception as exc:  # API boundary
                self._send({"ok": False, "error": {
                    "code": "E_INTERNAL",
                    "message": f"{type(exc).__name__}: {exc}",
                    "details": {},
                }}, code=500)

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def log_message(self, *args):  # keep demo output clean
            pass

    return Handler


def serve(workspace_root, port=DEFAULT_PORT, repo_root=None):
    api = Api(workspace_root, repo_root=repo_root)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(api))
    print(f"serving on http://127.0.0.1:{port} (workspace={workspace_root})")
    server.serve_forever()
