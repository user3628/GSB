"""Local Flask diagnostic workbench and JSON API."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Mapping

from flask import Flask, jsonify, render_template, request

from .canonical import atomic_write_json, read_json
from .errors import AudioQError
from .models import decode_config, encode_config
from .pipeline import EvaluationWorkbench


HTTP_STATUS = {
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 409,
    "E_INVALID_WEIGHT": 422,
    "E_NUMERIC": 422,
    "E_EMPTY_AUDIO": 422,
    "E_CLIPPED_AUDIO": 422,
    "E_LENGTH_MISMATCH": 422,
    "E_SAMPLE_RATE": 422,
    "E_AUDIO_FORMAT": 422,
    "E_MANIFEST": 400,
    "E_CONFIG": 400,
    "E_IO": 500,
}


def create_app(
    root: Path | str,
    manifest_path: Path | str,
    config_path: Path | str,
    state_dir: Path | str = "work/state",
    *,
    normalize_policy: str = "normalize",
    snr_threshold_rule: str = "fixed",
    default_snr_threshold_db: float = 8.0,
) -> Flask:
    root_path = Path(root).resolve()
    session_lock = threading.Lock()
    session: dict[str, str] = {
        "manifest": str(manifest_path),
        "config": str(config_path),
        "state_dir": str(state_dir),
    }
    policies = {
        "normalize_policy": normalize_policy,
        "snr_threshold_rule": snr_threshold_rule,
        "default_snr_threshold_db": default_snr_threshold_db,
    }

    app = Flask(__name__, template_folder="templates")
    app.json.ensure_ascii = False
    app.json.sort_keys = True

    def workbench() -> EvaluationWorkbench:
        with session_lock:
            manifest = session["manifest"]
            config = session["config"]
            state = session["state_dir"]
        return EvaluationWorkbench(root_path, manifest, config, state_dir=state, **policies)

    def run_payload(result: Mapping[str, Any], include_report: bool = False) -> dict[str, Any]:
        payload = {key: value for key, value in result.items() if key not in {"report", "status_snapshot"}}
        if result.get("report") is not None:
            payload["report_summary"] = result["report"]["summary"]
            payload["report_fingerprint"] = result["report"]["report_fingerprint"]
            if include_report:
                payload["report"] = result["report"]
        return dict(payload)

    @app.errorhandler(AudioQError)
    def handle_business_error(exc: AudioQError):
        return jsonify(exc.to_dict()), HTTP_STATUS.get(exc.code, 400)

    @app.errorhandler(FileNotFoundError)
    def handle_missing_file(exc: FileNotFoundError):
        error = AudioQError("E_IO", str(exc), observed={"path": exc.filename})
        return jsonify(error.to_dict()), 404

    @app.errorhandler(OSError)
    def handle_os_error(exc: OSError):
        error = AudioQError(
            "E_IO",
            str(exc) or exc.__class__.__name__,
            observed={"path": getattr(exc, "filename", None), "reason": exc.__class__.__name__},
        )
        return jsonify(error.to_dict()), 500

    @app.get("/")
    def dashboard():
        return render_template("dashboard.html")

    @app.get("/api/health")
    def health():
        wb = workbench()
        return jsonify(
            {
                "ok": True,
                "service": "audioq",
                "evaluation_id": wb.evaluation_id,
                "config_fingerprint": wb.config.version_fingerprint,
            }
        )

    @app.get("/api/status")
    def status():
        return jsonify(workbench().status())

    @app.post("/api/run")
    def run_stages():
        body = request.get_json(silent=True) or {}
        stop_after = body.get("stop_after_shards")
        if stop_after is not None and (isinstance(stop_after, bool) or not isinstance(stop_after, int)):
            raise AudioQError("E_CONFIG", "stop_after_shards must be an integer")
        wb = workbench()
        result = wb.run(
            stop_after_shards=stop_after,
            clear_stop=bool(body.get("clear_stop", False)),
            write_report=True,
        )
        return jsonify({"ok": True, **run_payload(result, bool(body.get("include_report", False)))})

    @app.post("/api/resume")
    def resume():
        body = request.get_json(silent=True) or {}
        result = workbench().resume(write_report=True)
        return jsonify({"ok": True, **run_payload(result, bool(body.get("include_report", False)))})

    @app.post("/api/stop")
    def request_stop():
        body = request.get_json(silent=True) or {}
        payload = workbench().request_stop(reason=str(body.get("reason", "http_request")))
        return jsonify({"ok": True, **payload, "note": "the worker stops after the current atomic shard"})

    @app.get("/api/report")
    def report():
        return jsonify(workbench().build_report())

    @app.post("/api/report/export")
    def export_report():
        body = request.get_json(silent=True) or {}
        path = body.get("path")
        report_value, data, target = workbench().write_report(path)
        return jsonify(
            {
                "ok": True,
                "path": target.relative_to(root_path).as_posix() if target.is_relative_to(root_path) else str(target),
                "report_fingerprint": report_value["report_fingerprint"],
                "summary": report_value["summary"],
            }
        )

    @app.get("/api/shards/<shard_id>")
    def shard_details(shard_id: str):
        wb = workbench()
        current = wb.status()
        shard_status = next((item for item in current["shards"] if item["shard_id"] == shard_id), None)
        if shard_status is None:
            raise AudioQError("E_MANIFEST", "unknown shard id", shard_id=shard_id, observed={"shard_id": shard_id})
        artifact = wb.checkpoints.read_artifact(shard_id, "diagnostics", repair=False)
        return jsonify(
            {
                "ok": True,
                "shard_id": shard_id,
                "checkpoint_status": shard_status,
                "diagnostics": artifact,
                "latest_invalidation": next(
                    (
                        item
                        for item in (current.get("last_persisted_invalidation") or {}).get("events", [])
                        if item.get("shard_id") == shard_id
                    ),
                    None,
                ),
            }
        )

    @app.get("/api/config")
    def config_details():
        wb = workbench()
        return jsonify(
            {
                "ok": True,
                "path": wb._relative(wb.config_path),
                "schema_version": wb.config.schema_version,
                "semantic": wb.config.semantic_dict(),
                "version_fingerprint": wb.config.version_fingerprint,
                "compatibility": dict(wb.config.compatibility),
            }
        )

    @app.post("/api/convert")
    def convert_config():
        body = request.get_json(silent=True) or {}
        target_version = body.get("target_version")
        if target_version not in (1, 2):
            raise AudioQError("E_CONFIG", "target_version must be 1 or 2", observed={"target_version": target_version})
        if isinstance(body.get("config"), Mapping):
            raw = body["config"]
        elif body.get("input_path"):
            raw = read_json(root_path / Path(str(body["input_path"])))
        else:
            raw = read_json(workbench().config_path)
        config = decode_config(
            raw,
            normalize_policy=str(body.get("normalize_policy", policies["normalize_policy"])),
            snr_threshold_rule=str(body.get("snr_threshold_rule", policies["snr_threshold_rule"])),
            default_snr_threshold_db=float(
                body.get("default_snr_threshold_db", policies["default_snr_threshold_db"])
            ),
        )
        exported = encode_config(config, target_version)
        output_path = body.get("output_path")
        written = None
        if output_path:
            target = root_path / Path(str(output_path))
            atomic_write_json(target, exported, pretty=True)
            written = target.relative_to(root_path).as_posix()
        return jsonify(
            {
                "ok": True,
                "source_schema_version": config.schema_version,
                "target_schema_version": target_version,
                "version_fingerprint": config.version_fingerprint,
                "derived_snr_threshold_db": config.metrics.snr_threshold_db,
                "downgrade_items": exported.get("downgrade_items", []),
                "downgrade_restore": exported.get("downgrade_restore", {}),
                "internal_model": config.semantic_dict(),
                "exported": exported,
                "written_to": written,
            }
        )

    @app.post("/api/import")
    def import_inputs():
        body = request.get_json(silent=True) or {}
        imported_dir = root_path / "work" / "imported"
        imported_dir.mkdir(parents=True, exist_ok=True)
        next_manifest = session["manifest"]
        next_config = session["config"]
        if isinstance(body.get("manifest"), Mapping) or body.get("manifest_path"):
            if isinstance(body.get("manifest"), Mapping):
                path = imported_dir / "manifest.json"
                atomic_write_json(path, body["manifest"], pretty=True)
                next_manifest = path.relative_to(root_path).as_posix()
            else:
                next_manifest = str(body["manifest_path"])
        if isinstance(body.get("config"), Mapping) or body.get("config_path"):
            if isinstance(body.get("config"), Mapping):
                raw_config = body["config"]
                config = decode_config(
                    raw_config,
                    normalize_policy=str(body.get("normalize_policy", policies["normalize_policy"])),
                    snr_threshold_rule=str(body.get("snr_threshold_rule", policies["snr_threshold_rule"])),
                    default_snr_threshold_db=float(
                        body.get("default_snr_threshold_db", policies["default_snr_threshold_db"])
                    ),
                )
                path = imported_dir / f"config_v{config.schema_version}.json"
                atomic_write_json(path, raw_config, pretty=True)
                next_config = path.relative_to(root_path).as_posix()
            else:
                next_config = str(body["config_path"])
                decode_config(read_json(root_path / Path(next_config)))
        next_state = str(body.get("state_dir", session["state_dir"]))
        candidate = EvaluationWorkbench(
            root_path,
            next_manifest,
            next_config,
            state_dir=next_state,
            **policies,
        )
        with session_lock:
            session.update({"manifest": next_manifest, "config": next_config, "state_dir": next_state})
        return jsonify(
            {
                "ok": True,
                "manifest": next_manifest,
                "config": next_config,
                "config_fingerprint": candidate.config.version_fingerprint,
                "shard_count": len(candidate.shards),
            }
        )

    @app.get("/api/checkpoints")
    def checkpoints():
        wb = workbench()
        return jsonify({"ok": True, **wb.checkpoints.summary([shard.shard_id for shard in wb.shards])})

    @app.post("/api/checkpoint")
    def publish_checkpoint():
        body = request.get_json(silent=True) or {}
        wb = workbench()
        record = wb.checkpoints.publish_checkpoint(
            shard_id=str(body.get("shard_id", "")),
            stage=str(body.get("stage", "")),
            input_fingerprint=str(body.get("input_fingerprint", "")),
            config_fingerprint=str(body.get("config_fingerprint", wb.config.version_fingerprint)),
            artifact=body.get("artifact", {}),
            expected_seq=body.get("expected_seq"),
        )
        return jsonify({"ok": True, "record": record}), 201

    return app
