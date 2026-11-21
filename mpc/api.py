from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request

from .errors import BusinessError
from .service import CheckpointService


def _ok(data: Any) -> tuple[Any, int]:
    return jsonify({"ok": True, "data": data}), 200


def _json_body() -> dict[str, Any]:
    if not request.is_json:
        raise BusinessError("E_INVALID_JSON", "Content-Type must be application/json")
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        raise BusinessError("E_INVALID_JSON", "request body must be a JSON object")
    return payload


def create_app(
    database_path: str | Path | None = None,
    samples_dir: str | Path | None = None,
    *,
    recover_on_start: bool = True,
) -> Flask:
    root = Path(__file__).resolve().parents[1]
    db_path = Path(
        database_path
        or os.environ.get("MPC_DB_PATH", str(root / "var" / "checkpoints.sqlite3"))
    )
    sample_path = Path(
        samples_dir
        or os.environ.get("MPC_SAMPLES_DIR", str(root / "data" / "seed_20261122"))
    )
    service = CheckpointService(
        db_path=db_path,
        samples_dir=sample_path,
        recover_on_start=recover_on_start,
    )
    app = Flask(__name__)
    app.config["MPC_SERVICE"] = service
    app.config["JSON_SORT_KEYS"] = False

    @app.errorhandler(BusinessError)
    def handle_business_error(error: BusinessError):
        body = error.to_dict()
        return (
            jsonify(
                {
                    "ok": False,
                    "code": error.code,
                    "error": body,
                }
            ),
            error.status,
        )

    @app.errorhandler(404)
    def handle_not_found(_error):
        error = BusinessError("E_NOT_FOUND", "HTTP route not found")
        return jsonify({"ok": False, "code": error.code, "error": error.to_dict()}), 404

    @app.errorhandler(Exception)
    def handle_unexpected(error: Exception):
        # Flask routes intentionally expose only a stable error envelope. The
        # traceback is kept out of the response because it may contain local paths.
        business = BusinessError(
            "E_INTERNAL",
            "internal server error",
            details={"type": type(error).__name__},
        )
        return (
            jsonify({"ok": False, "code": business.code, "error": business.to_dict()}),
            500,
        )

    @app.get("/api/health")
    @app.get("/health")
    @app.get("/api/status")
    def health():
        return _ok(service.health())

    @app.get("/api/policies/current")
    def current_policy():
        return _ok(service.get_current_policy())

    @app.post("/api/policies/snapshots")
    @app.post("/api/policies/full")
    @app.post("/api/policy/snapshot")
    def submit_policy_snapshot():
        return _ok(service.submit_policy_snapshot(_json_body()))

    @app.post("/api/policies/increments")
    @app.post("/api/policy/increment")
    def submit_policy_increment():
        return _ok(service.submit_policy_increment(_json_body()))

    @app.post("/api/policies/load-snapshot")
    @app.post("/api/policy/snapshot/load")
    def load_policy_snapshot():
        body = _json_body()
        path = body.get("path")
        if not isinstance(path, str) or not path.strip():
            raise BusinessError("E_INVALID_REQUEST", "snapshot path is required")
        commit_id = body.get("commit_id")
        if commit_id is not None and not isinstance(commit_id, str):
            raise BusinessError("E_INVALID_REQUEST", "commit_id must be a string")
        return _ok(service.load_policy_snapshot_file(path, commit_id=commit_id))

    @app.get("/api/policies/<int:version>/derived")
    def policy_derived(version: int):
        return _ok(service.get_policy_derived(version))

    @app.post("/api/checkpoints")
    @app.post("/api/checkpoint")
    def submit_checkpoint():
        return _ok(service.submit_checkpoint(_json_body()))

    @app.get("/api/checkpoints/latest")
    def latest_checkpoint_manifest():
        return _ok(service.get_checkpoint_manifest("latest"))

    @app.get("/api/checkpoints/latest/derived")
    def latest_checkpoint_derived():
        return _ok(service.get_checkpoint_derived("latest"))

    @app.get("/api/checkpoints/<ref>")
    def checkpoint_manifest(ref: str):
        return _ok(service.get_checkpoint_manifest(ref))

    @app.get("/api/checkpoints/<ref>/derived")
    @app.get("/api/checkpoint/<ref>/derived")
    def checkpoint_derived(ref: str):
        raw_policy_version = request.args.get("policy_version")
        policy_version = None
        if raw_policy_version is not None:
            try:
                policy_version = int(raw_policy_version)
            except ValueError as exc:
                raise BusinessError("E_INVALID_REQUEST", "policy_version must be an integer") from exc
        return _ok(service.get_checkpoint_derived(ref, policy_version=policy_version))

    @app.get("/api/checkpoints/<ref>/shards/<shard_id>/derived")
    def checkpoint_shard_derived(ref: str, shard_id: str):
        raw_policy_version = request.args.get("policy_version")
        policy_version = None
        if raw_policy_version is not None:
            try:
                policy_version = int(raw_policy_version)
            except ValueError as exc:
                raise BusinessError("E_INVALID_REQUEST", "policy_version must be an integer") from exc
        data = service.get_checkpoint_derived(ref, policy_version=policy_version)
        for shard in data["shards"]:
            if shard["shard_id"] == shard_id:
                return _ok(
                    {
                        "checkpoint": data["checkpoint"],
                        "policy": data["policy"],
                        "is_current_policy": data["is_current_policy"],
                        "aggregate": data["aggregate"],
                        "shard": shard,
                    }
                )
        raise BusinessError(
            "E_NOT_FOUND", "shard not found in checkpoint", details={"shard_id": shard_id}
        )

    @app.get("/api/versions/<int:version>/derived")
    def checkpoint_derived_by_version(version: int):
        raw_policy_version = request.args.get("policy_version")
        policy_version = None
        if raw_policy_version is not None:
            try:
                policy_version = int(raw_policy_version)
            except ValueError as exc:
                raise BusinessError("E_INVALID_REQUEST", "policy_version must be an integer") from exc
        return _ok(service.get_checkpoint_derived(version, policy_version=policy_version))

    @app.get("/api/derived")
    def checkpoint_derived_query():
        raw_version = request.args.get("version", request.args.get("checkpoint_version"))
        if raw_version is None:
            raise BusinessError("E_INVALID_REQUEST", "version query parameter is required")
        try:
            version = int(raw_version)
        except ValueError as exc:
            raise BusinessError("E_INVALID_REQUEST", "version must be an integer") from exc
        raw_policy = request.args.get("policy_version")
        policy_version = None
        if raw_policy is not None:
            try:
                policy_version = int(raw_policy)
            except ValueError as exc:
                raise BusinessError("E_INVALID_REQUEST", "policy_version must be an integer") from exc
        return _ok(service.get_checkpoint_derived(version, policy_version=policy_version))

    @app.get("/api/recovery/report")
    @app.get("/api/recovery")
    def recovery_report():
        report_id = request.args.get("report_id")
        return _ok(service.get_recovery_report(report_id))

    @app.post("/api/recovery/scan")
    def recovery_scan():
        return _ok(service.recover_pending())

    @app.get("/api/recovery/intents")
    def recovery_intents():
        return _ok({"intents": service.list_intents()})

    @app.get("/api/commits")
    def commits():
        return _ok({"operations": service.list_commit_operations()})

    return app
