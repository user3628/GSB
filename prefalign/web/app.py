"""Local Flask HTTP interface for the preference-alignment editor."""
from __future__ import annotations

import json
from pathlib import Path

from flask import Flask, jsonify, request

from ..core.errors import PrefAlignError
from ..core.hashing import canonical_json
from ..core.workspace import (
    build_workspace,
    edit_sample,
    full_rebuild_digests,
    import_demo_shards,
    import_shards,
    replace_source_shard,
)


def create_app(project_root: str | Path, config_path: str | Path | None = None) -> Flask:
    root = Path(project_root).resolve()
    app = Flask(__name__)
    app.config["PROJECT_ROOT"] = root

    def components():
        return build_workspace(root, Path(config_path).resolve() if config_path else None)

    @app.errorhandler(PrefAlignError)
    def handle_business_error(exc: PrefAlignError):
        return jsonify(exc.to_dict()), 409

    @app.errorhandler(FileNotFoundError)
    def handle_missing(exc):
        return jsonify({"code": "E_IO", "message": str(exc)}), 404

    @app.get("/health")
    def health():
        return jsonify({"status": "ok"})

    @app.post("/shards/import")
    def import_endpoint():
        cfg, store, _ = components()
        payload = request.get_json(force=True)
        if payload.get("demo"):
            result = import_demo_shards(store, root)
        else:
            paths = [root / name for name in payload["shards"]]
            result = import_shards(store, paths)
        return jsonify({"imported": result})

    @app.post("/pipeline/run")
    def run_endpoint():
        _, _, pipeline = components()
        reason = (request.get_json(silent=True) or {}).get("reason", "http_run")
        return jsonify(pipeline.run(reason=reason))

    @app.get("/pipeline/status")
    def status_endpoint():
        _, _, pipeline = components()
        return jsonify(pipeline.status())

    @app.get("/pipeline/artifacts/<stage>")
    def artifact_endpoint(stage: str):
        _, _, pipeline = components()
        if stage not in ("normalize", "quality", "features", "sampling"):
            return jsonify({"code": "E_INVALID_STAGE", "message": stage}), 400
        return jsonify(pipeline.load_artifact(stage))

    @app.post("/samples/edit")
    def edit_endpoint():
        _, store, pipeline = components()
        payload = request.get_json(force=True)
        sample = edit_sample(store, payload)
        result = pipeline.run(reason=f"edit:{sample.pair_id}")
        return jsonify({"edited": sample.pair_id, "run": result})

    @app.post("/sources/<source>/replace")
    def replace_endpoint(source: str):
        _, store, pipeline = components()
        payload = request.get_json(force=True)
        entry = replace_source_shard(store, source, root / payload["file"], payload["alias"])
        result = pipeline.run(reason=f"replace_source:{source}")
        return jsonify({"replaced": source, "entry": entry, "run": result})

    @app.post("/stop")
    def stop_endpoint():
        _, _, pipeline = components()
        pipeline.request_stop()
        return jsonify({"stop_requested": True})

    @app.post("/resume")
    def resume_endpoint():
        _, _, pipeline = components()
        return jsonify(pipeline.resume())

    @app.post("/verify")
    def verify_endpoint():
        _, _, pipeline = components()
        payload = request.get_json(silent=True) or {}
        baseline_root = Path(payload["baseline_root"]).resolve() if payload.get(
            "baseline_root") else root / "workspace" / "verify_full"
        digests, count = full_rebuild_digests(root, baseline_root)
        return jsonify(pipeline.verify_incremental(digests, count))

    @app.post("/mixture/export")
    def export_endpoint():
        _, _, pipeline = components()
        target = pipeline.export_mixture()
        return jsonify({"exported": str(target)})

    @app.get("/mixture")
    def mixture_endpoint():
        _, _, pipeline = components()
        return jsonify(pipeline.load_artifact("sampling"))

    return app


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run the prefalign HTTP server")
    parser.add_argument("--root", default=".")
    parser.add_argument("--config", default=None)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5050)
    args = parser.parse_args()
    create_app(args.root, args.config).run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
