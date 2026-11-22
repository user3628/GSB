"""Local deterministic HTTP interface backed by :mod:`http.server`.

The server holds a single bound workspace (data directory + run directory).
All responses are canonical JSON and all failures are returned with the stable
business error codes from :mod:`ragwb.errors`; no external service is used.
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from .errors import WorkbenchError
from .incremental import detect_change, rebuild_incremental
from .io_utils import read_json
from .pipeline import Pipeline
from . import query as query_mod


class ServerState:
    def __init__(self) -> None:
        self.data_dir: Optional[str] = None
        self.run_dir: Optional[str] = None
        self.pipeline: Optional[Pipeline] = None

    def bind(self, data_dir: str, run_dir: str) -> Pipeline:
        self.data_dir = data_dir
        self.run_dir = run_dir
        self.pipeline = Pipeline(data_dir, run_dir)
        return self.pipeline

    def require(self) -> Pipeline:
        if self.pipeline is None:
            raise WorkbenchError(
                "E_IMPORT_INVALID",
                "no workspace imported; POST /import first",
                stage="http")
        return self.pipeline


STATE = ServerState()


def _json_response(handler: BaseHTTPRequestHandler, status: int,
                   payload: Dict[str, Any]) -> None:
    body = (json.dumps(payload, ensure_ascii=False, sort_keys=True,
                       allow_nan=False) + "\n").encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class WorkbenchHandler(BaseHTTPRequestHandler):
    server_version = "ragwb/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet logs
        return

    def _read_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send(self, payload: Dict[str, Any], status: int = 200) -> None:
        _json_response(self, status, payload)

    def _send_error(self, exc: WorkbenchError) -> None:
        self._send({"ok": False, "error": exc.to_dict()}, status=409)

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        parsed = urlparse(self.path)
        parts = [segment for segment in parsed.path.split("/") if segment]
        query = parse_qs(parsed.query)
        try:
            if parts == ["status"]:
                pipeline = STATE.require()
                self._send({"ok": True,
                            "summary": self._load_summary(pipeline)})
                return
            if parts == ["sessions"]:
                pipeline = STATE.require()
                self._send({"ok": True,
                            "sessions": sorted(pipeline.sessions)})
                return
            if len(parts) == 3 and parts[0] == "sessions" and \
                    parts[2] == "fingerprints":
                self._send({"ok": True,
                            "fingerprints": self._fingerprints(
                                parts[1])})
                return
            if len(parts) == 3 and parts[0] == "sessions" and \
                    parts[2] == "evidence":
                turn = int(query.get("turn", [0])[0])
                self._send({"ok": True,
                            "evidence": self._evidence(parts[1], turn)})
                return
            if len(parts) == 3 and parts[0] == "sessions" and \
                    parts[2] == "plan":
                turn = int(query.get("turn", [0])[0])
                self._send({"ok": True,
                            "plan": self._plan(parts[1], turn)})
                return
            if len(parts) == 2 and parts[0] == "answers":
                pipeline = STATE.require()
                self._send({"ok": True,
                            "answer": read_json(
                                pipeline._answer_path(parts[1]))})
                return
            if len(parts) == 2 and parts[0] == "export":
                pipeline = STATE.require()
                answer = read_json(pipeline._answer_path(parts[1]))
                self._send({"ok": True, "export": answer})
                return
            if parts == ["invalidation"]:
                pipeline = STATE.require()
                path = f"{pipeline.run_dir}/invalidation.json"
                self._send({"ok": True,
                            "invalidation": read_json(path)})
                return
            self._send({"ok": False,
                        "error": {"code": "E_IMPORT_INVALID",
                                  "message": f"unknown route: {self.path}"}},
                       status=404)
        except WorkbenchError as exc:
            self._send_error(exc)
        except FileNotFoundError as exc:
            self._send({"ok": False,
                        "error": {"code": "E_INVALID_STAGE",
                                  "message": str(exc)}}, status=404)

    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        parts = [segment for segment in
                 urlparse(self.path).path.split("/") if segment]
        body = self._read_body()
        try:
            if parts == ["import"]:
                pipeline = STATE.bind(body["data_dir"], body["run_dir"])
                self._send({"ok": True, "data_dir": pipeline.data_dir,
                            "run_dir": pipeline.run_dir,
                            "config_fingerprint":
                                pipeline.config_fingerprint})
                return
            if parts == ["run"]:
                pipeline = STATE.require()
                summary = pipeline.run(
                    session_ids=body.get("session_ids"),
                    max_turns=body.get("max_turns"))
                self._send({"ok": True, "summary": summary})
                return
            if parts == ["stop"]:
                pipeline = STATE.require()
                pipeline.stop.request_stop()
                self._send({"ok": True, "stop_requested": True})
                return
            if parts == ["recover"]:
                pipeline = STATE.require()
                summary = pipeline.run(
                    session_ids=body.get("session_ids"))
                self._send({"ok": True, "summary": summary})
                return
            if parts == ["incremental", "verify"]:
                result = rebuild_incremental(
                    STATE.require(),
                    data_dir_baseline=STATE.data_dir or "",
                    run_dir_clean=body["clean_run_dir"],
                    changed_data_dir=body["changed_data_dir"],
                    changed_run_dir=body["changed_run_dir"])
                self._send({"ok": True, "verification": result})
                return
            if parts == ["invalidation", "detect"]:
                reports = self._detect_all()
                self._send({"ok": True,
                            "reports": [report.to_dict()
                                        for report in reports]})
                return
            self._send({"ok": False,
                        "error": {"code": "E_IMPORT_INVALID",
                                  "message": f"unknown route: {self.path}"}},
                       status=404)
        except WorkbenchError as exc:
            self._send_error(exc)

    def _load_summary(self, pipeline: Pipeline) -> Dict[str, Any]:
        path = f"{pipeline.run_dir}/summary.json"
        return read_json(path)

    def _fingerprints(self, session_id: str) -> Dict[str, Any]:
        pipeline = STATE.require()
        diagnostics = read_json(
            pipeline._diagnostics_path(session_id))
        return {
            "session_id": session_id,
            "turns": [
                {"turn_index": turn["turn_index"],
                 "input_fingerprints": turn["input_fingerprints"],
                 "stage_fingerprints": turn["stage_fingerprints"]}
                for turn in diagnostics["turns"]
            ],
        }

    def _evidence(self, session_id: str, turn_index: int) -> Dict[str, Any]:
        pipeline = STATE.require()
        diagnostics = read_json(
            pipeline._diagnostics_path(session_id))
        for turn in diagnostics["turns"]:
            if turn["turn_index"] == turn_index:
                return {
                    "session_id": session_id,
                    "turn_index": turn_index,
                    "evidence_chain": turn.get("evidence_chain", []),
                    "error": turn.get("error"),
                }
        raise WorkbenchError(
            "E_INVALID_STAGE", f"turn {turn_index} not found",
            session_id=session_id, turn_index=turn_index)

    def _plan(self, session_id: str, turn_index: int) -> Dict[str, Any]:
        pipeline = STATE.require()
        diagnostics = read_json(
            pipeline._diagnostics_path(session_id))
        for turn in diagnostics["turns"]:
            if turn["turn_index"] == turn_index:
                return turn.get("strategy", {})
        raise WorkbenchError(
            "E_INVALID_STAGE", f"turn {turn_index} not found",
            session_id=session_id, turn_index=turn_index)

    def _detect_all(self):
        pipeline = STATE.require()
        reports = []
        for session_id, turns in sorted(pipeline.sessions.items()):
            focus = query_mod.Focus()
            for turn in sorted(turns, key=lambda row: row["turn_index"]):
                reports.append(detect_change(
                    pipeline, session_id, turn, focus.to_dict()))
                if turn.get("focus_hint"):
                    focus = query_mod.Focus(entity=turn["focus_hint"])
                else:
                    focus = query_mod.Focus(
                        entity=focus.entity)
        return reports


def build_server(host: str, port: int,
                 data_dir: Optional[str] = None,
                 run_dir: Optional[str] = None
                 ) -> Tuple[ThreadingHTTPServer, ServerState]:
    if data_dir and run_dir:
        STATE.bind(data_dir, run_dir)
    httpd = ThreadingHTTPServer((host, port), WorkbenchHandler)
    return httpd, STATE


def serve(host: str, port: int, data_dir: Optional[str] = None,
          run_dir: Optional[str] = None) -> None:
    httpd, _ = build_server(host, port, data_dir, run_dir)
    print(f"ragwb HTTP server listening on http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - manual shutdown
        pass
    finally:
        httpd.server_close()
