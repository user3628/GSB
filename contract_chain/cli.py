"""Command-line interface for the local contract-chain pipeline."""
from __future__ import annotations

import argparse
import json
import sys

from .codec import read_json
from .config import PipelineConfig
from .errors import ChainError
from .evidence import export_manifest, import_manifest, reconstruct_decisions
from .httpapi import serve
from .pipeline import Pipeline
from .samplegen import write_workspace_bundle


def _print(obj):
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))


def _load_optional_config(args):
    path = getattr(args, "config", None)
    if path:
        return PipelineConfig.from_dict(read_json(path))
    return None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="contract-chain", description=__doc__)
    parser.add_argument("--workspace", default="./cc_workspace", help="checkpoint workspace directory")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="write deterministic sample, config and evidence template")
    p_import = sub.add_parser("import-sample", help="import a layout sample JSON file")
    p_import.add_argument("path")
    p_config = sub.add_parser("config", help="replace pipeline configuration")
    p_config.add_argument("path")
    sub.add_parser("run", help="execute or resume all pending stages")
    sub.add_parser("full", help="discard checkpoints and run from scratch")
    sub.add_parser("resume", help="clear stop marker and continue")
    sub.add_parser("stop", help="request cooperative graceful stop")
    sub.add_parser("status", help="show stage status, fingerprints and checkpoints")
    sub.add_parser("demotions", help="show demoted citations and reasons")
    sub.add_parser("evidence", help="show the exported evidence manifest")
    sub.add_parser("reconstruct", help="rebuild verdicts from the imported evidence list")
    p_inc = sub.add_parser("incremental", help="incremental run with optional new sample/config")
    p_inc.add_argument("--sample")
    p_inc.add_argument("--config")
    p_exp = sub.add_parser("export", help="export verified evidence manifest")
    p_exp.add_argument("path")
    p_imp = sub.add_parser("import-evidence", help="verify and import an evidence manifest")
    p_imp.add_argument("path")
    p_http = sub.add_parser("http", help="start local HTTP demo server")
    p_http.add_argument("--host", default="127.0.0.1")
    p_http.add_argument("--port", type=int, default=8765)

    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            _print(write_workspace_bundle(args.workspace))
            return 0
        pipeline = Pipeline(args.workspace, _load_optional_config(args))
        if args.command == "import-sample":
            sample = read_json(args.path)
            pipeline.load_sample(sample)
            _print({"imported": sample["sample_id"]})
        elif args.command == "config":
            pipeline.update_config(PipelineConfig.from_dict(read_json(args.path)))
            _print({"config_digest": pipeline.config.full_digest()})
        elif args.command == "run":
            _print(pipeline.run())
        elif args.command == "full":
            _print(pipeline.run_full())
        elif args.command == "resume":
            _print(pipeline.resume())
        elif args.command == "stop":
            pipeline.store.request_stop()
            _print({"stop": True})
        elif args.command == "status":
            _print(pipeline.status())
        elif args.command == "demotions":
            _print(pipeline.store.get_stage("resolve")["demotions"])
        elif args.command == "evidence":
            _print(import_manifest("%s/evidence_manifest.json" % args.workspace))
        elif args.command == "reconstruct":
            manifest = import_manifest("%s/evidence_manifest.json" % args.workspace)
            _print(reconstruct_decisions(manifest))
        elif args.command == "incremental":
            sample = read_json(args.sample) if args.sample else None
            config = PipelineConfig.from_dict(read_json(args.config)) if args.config else None
            _print(pipeline.run_incremental(sample, config))
        elif args.command == "export":
            manifest = import_manifest("%s/evidence_manifest.json" % args.workspace)
            _print({"exported": export_manifest(manifest, args.path)})
        elif args.command == "import-evidence":
            manifest = import_manifest(args.path)
            _print({"records": manifest["record_count"], "digest": manifest["manifest_digest"]})
        elif args.command == "http":
            server, _ = serve(args.workspace, args.host, args.port)
            print("serving on http://%s:%d (Ctrl+C to stop)" % (args.host, args.port))
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                server.shutdown()
        return 0
    except ChainError as exc:
        _print(exc.to_dict())
        return 2


if __name__ == "__main__":
    sys.exit(main())
