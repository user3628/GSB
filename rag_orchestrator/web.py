"""Local Flask diagnostics API and visualization workbench."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, request

from . import __version__
from .checkpoints import STAGES, CheckpointStore
from .config import config_section_hashes, load_config
from .engine import Orchestrator
from .errors import E_REQUEST_NOT_FOUND, EngineError
from .util import read_jsonl


class WorkspaceService:
    def __init__(
        self,
        *,
        config_path: str | Path = "data/config.yaml",
        docs_path: str | Path = "data/docs.jsonl",
        requests_path: str | Path = "data/requests.jsonl",
        workspace: str | Path = "runtime",
    ) -> None:
        self.config_path = Path(config_path)
        self.docs_path = Path(docs_path)
        self.requests_path = Path(requests_path)
        self.workspace = Path(workspace)
        self.stop_event = threading.Event()
        self.results: dict[str, dict[str, Any]] = {}
        self.run_all_results: dict[str, Any] | None = None
        self._lock = threading.Lock()
        self.reload()

    def reload(self) -> None:
        self.config = load_config(self.config_path)
        self.documents = read_jsonl(self.docs_path)
        self.requests = sorted(read_jsonl(self.requests_path), key=lambda item: str(item.get("query_id", "")))
        self.request_map = {str(item.get("query_id")): item for item in self.requests}
        self.store = CheckpointStore(self.workspace)
        self.config_hashes = config_section_hashes(self.config)

    def _orchestrator(self, mode: str, stop_after_stage: str | None) -> Orchestrator:
        return Orchestrator(
            self.config,
            self.documents,
            str(self.workspace),
            mode=mode,
            stop_event=self.stop_event,
            stop_after_stage=stop_after_stage,
        )

    def run(self, query_id: str, *, mode: str = "parallel", stop_after_stage: str | None = None) -> dict[str, Any]:
        with self._lock:
            self.reload()
            request_record = self.request_map.get(query_id)
            if request_record is None:
                error = EngineError(
                    E_REQUEST_NOT_FOUND,
                    "requested query_id is not present in the local request sample",
                    query_id=query_id,
                    details={"available_query_ids": sorted(self.request_map)},
                )
                return {
                    "query_id": query_id,
                    "status": "error",
                    "error": error.to_dict(),
                    "partial": {},
                    "diagnostics": {},
                }
            result = self._orchestrator(mode, stop_after_stage).run(request_record)
            self.results[query_id] = result
            if self.stop_event.is_set() and result.get("status") == "stopped":
                self.stop_event.clear()
            return result

    def run_all(self, *, mode: str = "parallel") -> dict[str, Any]:
        with self._lock:
            self.reload()
            result = self._orchestrator(mode, None).run_all(self.requests)
            for item in result["results"]:
                self.results[str(item.get("query_id"))] = item
            self.run_all_results = result
            if self.stop_event.is_set():
                self.stop_event.clear()
            return result

    def request_stop(self) -> None:
        self.stop_event.set()

    def workspace_summary(self) -> dict[str, Any]:
        unique_documents = {str(doc.get("doc_id")) for doc in self.documents}
        shards = sorted({str(doc.get("shard", doc.get("source"))) for doc in self.documents})
        return {
            "status": "ok",
            "engine_version": __version__,
            "paths": {
                "config": str(self.config_path),
                "documents": str(self.docs_path),
                "requests": str(self.requests_path),
                "workspace": str(self.workspace),
            },
            "counts": {
                "document_records": len(self.documents),
                "unique_doc_ids": len(unique_documents),
                "shards": len(shards),
                "requests": len(self.requests),
            },
            "shards": shards,
            "config_fingerprint": self.config_hashes["config"],
            "budget_config_version": self.config_hashes["budget"][:16],
            "stages": list(STAGES),
            "latest_results": {
                key: {
                    "status": value.get("status"),
                    "stable_sha256": value.get("diagnostics", {}).get("stable_sha256"),
                }
                for key, value in self.results.items()
            },
            "run_all_summary_sha256": (self.run_all_results or {}).get("summary_sha256"),
        }


def create_app(service: WorkspaceService) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["JSON_AS_ASCII"] = False

    @app.get("/")
    def dashboard() -> str:
        return render_template("dashboard.html", engine_version=__version__)

    @app.get("/favicon.ico")
    def favicon() -> tuple[str, int]:
        return "", 204

    @app.get("/api/health")
    def health() -> Any:
        return jsonify({"status": "ok", "engine_version": __version__, "offline": True})

    @app.get("/api/workspace")
    def workspace() -> Any:
        service.reload()
        return jsonify(service.workspace_summary())

    @app.get("/api/config")
    def config_view() -> Any:
        service.reload()
        return jsonify(
            {
                "config": service.config,
                "section_fingerprints": service.config_hashes,
                "budget_config_version": service.config_hashes["budget"][:16],
            }
        )

    @app.get("/api/requests")
    def requests_view() -> Any:
        service.reload()
        return jsonify({"requests": service.requests, "count": len(service.requests)})

    @app.get("/api/documents")
    def documents_view() -> Any:
        service.reload()
        return jsonify({"documents": service.documents, "count": len(service.documents)})

    @app.post("/api/run")
    def run_one() -> Any:
        payload = request.get_json(silent=True) or {}
        query_id = payload.get("query_id")
        if not isinstance(query_id, str) or not query_id:
            return jsonify({"status": "error", "error": {"code": "E_REQUEST_NOT_FOUND", "message": "query_id is required"}}), 400
        mode = payload.get("mode", "parallel")
        stop_after_stage = payload.get("stop_after_stage")
        if mode not in {"parallel", "serial"}:
            return jsonify({"status": "error", "error": {"code": "E_INVALID_STAGE", "message": "mode must be parallel or serial"}}), 400
        result = service.run(query_id, mode=mode, stop_after_stage=stop_after_stage)
        return jsonify(result)

    @app.post("/api/run-all")
    def run_all() -> Any:
        payload = request.get_json(silent=True) or {}
        mode = payload.get("mode", "parallel")
        if mode not in {"parallel", "serial"}:
            return jsonify({"status": "error", "error": {"code": "E_INVALID_STAGE", "message": "mode must be parallel or serial"}}), 400
        return jsonify(service.run_all(mode=mode))

    @app.post("/api/stop")
    def stop() -> Any:
        service.request_stop()
        return jsonify({"status": "ok", "stop_requested": True, "message": "current indivisible stage will finish before exiting"})

    @app.post("/api/reload")
    def reload_view() -> Any:
        service.reload()
        return jsonify(service.workspace_summary())

    @app.get("/api/results")
    def results_view() -> Any:
        with service._lock:
            return jsonify(
                {
                    "results": service.results,
                    "run_all_summary_sha256": (service.run_all_results or {}).get("summary_sha256"),
                }
            )

    @app.get("/api/results/<query_id>")
    def result_view(query_id: str) -> Any:
        with service._lock:
            if query_id not in service.results:
                return jsonify(
                    {
                        "status": "error",
                        "error": {
                            "code": E_REQUEST_NOT_FOUND,
                            "message": "no in-memory result for query_id",
                            "query_id": query_id,
                        },
                    }
                ), 404
            return jsonify(service.results[query_id])

    @app.get("/api/checkpoints/<query_id>")
    def checkpoints_view(query_id: str) -> Any:
        summary = service.store.summary(query_id)
        version_path = service.store.workspace / "fingerprints" / f"{query_id}.json"
        summary["version_snapshot_exists"] = version_path.exists()
        return jsonify(summary)

    @app.get("/api/diagnostics/<query_id>")
    def diagnostics_view(query_id: str) -> Any:
        with service._lock:
            result = service.results.get(query_id)
        request_record = service.request_map.get(query_id)
        return jsonify(
            {
                "query_id": query_id,
                "request": request_record,
                "result": result,
                "checkpoints": service.store.summary(query_id),
                "workspace": service.workspace_summary(),
            }
        )

    return app
