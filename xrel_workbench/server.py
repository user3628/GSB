from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

from .bridge import load_bridge
from .checkpoints import CheckpointStore
from .config import load_config
from .errors import (
    BusinessError,
    E_CONFIG,
    E_INCREMENTAL_MISMATCH,
    E_INVALID_STAGE,
    E_NUMERIC,
    E_STALE_CHECKPOINT,
)
from .pipeline import export_scores, run_pipeline, verify_against_full, workspace_status
from .util import read_json, read_jsonl
from .workspace import (
    WorkspacePaths,
    clear_stop,
    edit_pair,
    import_bridge,
    import_shard,
    list_shards,
    request_stop,
    update_bridge_entry,
    update_config,
)


def _status_code(error: BusinessError) -> int:
    if error.code in {E_CONFIG, E_NUMERIC}:
        return 422
    if error.code in {E_STALE_CHECKPOINT, E_INVALID_STAGE, E_INCREMENTAL_MISMATCH}:
        return 409
    return 400


def create_app(workspace_root: Path) -> Flask:
    app = Flask(__name__)
    app.json.ensure_ascii = False
    app.json.sort_keys = True
    app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024
    paths = WorkspacePaths(Path(workspace_root).resolve())
    webui = Path(__file__).with_name("webui.html")

    @app.errorhandler(BusinessError)
    def handle_business_error(error: BusinessError) -> tuple[Response, int]:
        return jsonify(error.to_dict()), _status_code(error)

    @app.errorhandler(400)
    def handle_bad_request(error: Any) -> tuple[Response, int]:
        return jsonify({"ok": False, "error": {"code": "E_INPUT", "message": str(error)}}), 400

    @app.get("/")
    def index() -> Response:
        return Response(webui.read_text(encoding="utf-8"), mimetype="text/html; charset=utf-8")

    @app.get("/api/health")
    def health() -> Response:
        return jsonify({"ok": True, "service": "xrel-workbench", "workspace": str(paths.root)})

    @app.get("/api/status")
    def status() -> Response:
        return jsonify({"ok": True, "data": workspace_status(paths)})

    @app.get("/api/fingerprints")
    def fingerprints() -> Response:
        manifest = read_json(paths.manifest) if paths.manifest.exists() else {}
        return jsonify(
            {
                "ok": True,
                "data": {
                    "config_version": manifest.get("config_version"),
                    "input_digests": manifest.get("input_digests", {}),
                    "fingerprints": manifest.get("fingerprints", {}),
                },
            }
        )

    @app.get("/api/inputs")
    def inputs() -> Response:
        return jsonify(
            {
                "ok": True,
                "data": {
                    "shards": list_shards(paths),
                    "bridge": load_bridge(paths.bridge),
                    "config": load_config(paths.scoring),
                },
            }
        )

    @app.get("/api/shards/<shard>")
    def shard_pairs(shard: str) -> Response:
        path = paths.shards / f"{shard}.jsonl"
        if not path.exists():
            raise BusinessError("E_INPUT", f"shard not found: {shard}")
        return jsonify({"ok": True, "data": read_jsonl(path)})

    @app.get("/api/grades")
    def grades() -> Response:
        shard = request.args.get("shard")
        records: list[dict[str, Any]] = []
        selected = [shard] if shard else list_shards(paths)
        for name in selected:
            path = paths.artifacts / "stage4" / f"{name}.jsonl"
            if path.exists():
                records.extend(read_jsonl(path))
        pair_id = request.args.get("pair_id")
        if pair_id:
            records = [item for item in records if item["pair_id"] == pair_id]
        return jsonify({"ok": True, "count": len(records), "data": records})

    @app.get("/api/errors")
    def errors() -> Response:
        path = paths.reports / "error_report.json"
        if not path.exists():
            raise BusinessError("E_INPUT", "error report is not available; run the build first")
        report = read_json(path)
        pair_id = request.args.get("pair_id")
        if pair_id:
            report["pairs"] = [item for item in report["pairs"] if item["pair_id"] == pair_id]
        return jsonify({"ok": True, "data": report})

    @app.get("/api/validation")
    def validation() -> Response:
        path = paths.reports / "validation.json"
        if not path.exists():
            raise BusinessError("E_INPUT", "validation report is not available; run the build first")
        return jsonify({"ok": True, "data": read_json(path)})

    @app.get("/api/summary")
    def summary() -> Response:
        path = paths.reports / "summary.json"
        if not path.exists():
            raise BusinessError("E_INPUT", "summary is not available; run the build first")
        return jsonify({"ok": True, "data": read_json(path)})

    @app.get("/api/checkpoints")
    def checkpoints() -> Response:
        return jsonify({"ok": True, "data": CheckpointStore(paths.root).status()})

    @app.get("/api/export/scores")
    def download_scores() -> Response:
        return Response(
            export_scores(paths),
            mimetype="application/x-ndjson; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=scores.jsonl"},
        )

    @app.post("/api/run")
    def run() -> Response:
        body = request.get_json(silent=True) or {}
        result = run_pipeline(
            paths,
            full=bool(body.get("full", False)),
            verify_incremental=bool(body.get("verify", True)),
        )
        return jsonify({"ok": True, "data": result})

    @app.post("/api/stop")
    def stop() -> Response:
        body = request.get_json(silent=True) or {}
        request_stop(paths, reason=str(body.get("reason", "HTTP stop request")))
        return jsonify({"ok": True, "data": {"status": "stop_requested"}})

    @app.post("/api/resume")
    def resume() -> Response:
        body = request.get_json(silent=True) or {}
        clear_stop(paths)
        result = run_pipeline(paths, verify_incremental=bool(body.get("verify", True)))
        return jsonify({"ok": True, "data": result})

    @app.post("/api/verify")
    def verify() -> Response:
        return jsonify({"ok": True, "data": verify_against_full(paths)})

    @app.post("/api/import/shards")
    def import_shards_http() -> Response:
        if "file" in request.files:
            upload = request.files["file"]
            shard = request.form.get("shard") or (upload.filename or "").rsplit(".", 1)[0]
            content: str | bytes = upload.read()
        else:
            body = request.get_json(silent=True) or {}
            shard = str(body.get("shard", ""))
            content = body.get("content", "")
        changed = import_shard(paths, shard, content)
        result = None
        body = request.get_json(silent=True) or {}
        if body.get("recalculate", False):
            result = run_pipeline(paths, verify_incremental=bool(body.get("verify", True)))
        return jsonify({"ok": True, "data": {"import": changed, "build": result}})

    @app.post("/api/import/bridge")
    def import_bridge_http() -> Response:
        recalculate = False
        verify = True
        if "file" in request.files:
            content: Any = request.files["file"].read()
            recalculate = request.form.get("recalculate", "false").lower() == "true"
        else:
            body = request.get_json(silent=True) or {}
            content = body.get("bridge", body.get("content", {}))
            recalculate = bool(body.get("recalculate", False))
            verify = bool(body.get("verify", True))
        changed = import_bridge(paths, content)
        result = run_pipeline(paths, verify_incremental=verify) if recalculate else None
        return jsonify({"ok": True, "data": {"import": changed, "build": result}})

    @app.post("/api/edit/bridge")
    def edit_bridge_http() -> Response:
        body = request.get_json(silent=True) or {}
        changed = update_bridge_entry(
            paths,
            str(body["entry_id"]),
            weight=float(body["weight"]) if body.get("weight") is not None else None,
            en=body.get("en"),
            zh=body.get("zh"),
        )
        result = (
            run_pipeline(paths, verify_incremental=bool(body.get("verify", True)))
            if body.get("recalculate", True)
            else None
        )
        return jsonify({"ok": True, "data": {"edit": changed, "build": result}})

    @app.post("/api/edit/config")
    def edit_config_http() -> Response:
        body = request.get_json(silent=True) or {}
        value = body.get("value")
        if isinstance(value, str):
            value = json.loads(value)
        changed = update_config(paths, str(body["path"]), value)
        result = (
            run_pipeline(paths, verify_incremental=bool(body.get("verify", True)))
            if body.get("recalculate", True)
            else None
        )
        return jsonify({"ok": True, "data": {"edit": changed, "build": result}})

    @app.post("/api/edit/pair")
    def edit_pair_http() -> Response:
        body = request.get_json(silent=True) or {}
        changed = edit_pair(
            paths,
            str(body["shard"]),
            str(body["pair_id"]),
            query=body.get("query"),
            doc=body.get("doc"),
            human_relevance=body.get("human_relevance"),
            clear_human_relevance=bool(body.get("clear_human_relevance", False)),
        )
        result = (
            run_pipeline(paths, verify_incremental=bool(body.get("verify", True)))
            if body.get("recalculate", True)
            else None
        )
        return jsonify({"ok": True, "data": {"edit": changed, "build": result}})

    @app.post("/api/checkpoint")
    def publish_checkpoint() -> Response:
        body = request.get_json(silent=True) or {}
        record = CheckpointStore(paths.root).publish(
            stage=str(body["stage"]),
            shard=str(body["shard"]),
            input_digest=str(body["input_digest"]),
            config_version=str(body["config_version"]),
            artifact_path=str(body["artifact_path"]),
            fingerprint=str(body["fingerprint"]),
            expected_seq=body.get("expected_seq"),
            expected_shards=body.get("expected_shards"),
        )
        return jsonify({"ok": True, "data": record})

    return app
