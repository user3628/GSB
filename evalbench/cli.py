"""Command line interface. Business error codes are printed as JSON and
mirrored in the process exit code (2 = business error, 1 = internal)."""
from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path

from .config import load_config
from .errors import EvalBenchError
from .pipeline import Pipeline, run_pipeline


def _emit_error(exc: EvalBenchError) -> int:
    print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True), file=sys.stderr)
    return 2


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2))


def cmd_gen_data(args) -> int:
    import subprocess
    script = Path(__file__).resolve().parent.parent / "scripts" / "gen_data.py"
    return subprocess.call([sys.executable, str(script), "--out", args.out])


def cmd_run(args) -> int:
    stop_event = threading.Event()

    def _handle(_sig, _frame):
        stop_event.set()
    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    try:
        report = run_pipeline(
            args.workspace, args.data, args.config,
            stop_event=stop_event, unit_delay_ms=args.unit_delay_ms,
            shard_ids=args.shards,
        )
    except EvalBenchError as exc:
        return _emit_error(exc)
    _print(report)
    return 0


def cmd_status(args) -> int:
    try:
        cfg = load_config(args.config)
        pipe = Pipeline(args.workspace, cfg)
        _print(pipe.status(data_dir=args.data))
    except EvalBenchError as exc:
        return _emit_error(exc)
    return 0


def cmd_upgrade_config(args) -> int:
    try:
        cfg = load_config(args.config)
    except EvalBenchError as exc:
        return _emit_error(exc)
    out = {
        "upgraded_config": cfg.to_v2_dict(),
        "upgrade_notes": cfg.upgrade_notes_dicts(),
    }
    payload = json.dumps(out, ensure_ascii=False, sort_keys=True, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0


def cmd_roundtrip(args) -> int:
    from . import codec
    try:
        cfg = load_config(args.config)
        shard_id = Path(args.shard).stem
        results = []
        with open(args.shard, "r", encoding="utf-8") as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        for line_no, line in enumerate(lines, start=1):
            obj = json.loads(line)
            version = codec.detect_declared_version(obj, shard_id=shard_id, line_no=line_no)
            if version == codec.SCHEMA_V2:
                results.append(codec.roundtrip_chain_from_v2(
                    obj, shard_id=shard_id, line_no=line_no, n_bins=cfg.n_bins,
                    rounding=cfg.rounding, id_delimiter=cfg.id_delimiter))
            else:
                results.append(codec.roundtrip_chain_from_v1(
                    obj, shard_id=shard_id, line_no=line_no, n_bins=cfg.n_bins,
                    rounding=cfg.rounding, id_delimiter=cfg.id_delimiter))
        _print({"shard_id": shard_id, "chains_verified": len(results), "ok": True})
    except EvalBenchError as exc:
        return _emit_error(exc)
    return 0


def cmd_serve(args) -> int:
    import uvicorn
    from .api import create_app
    app = create_app(default_data_dir=args.data, default_config=args.config)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="evalbench",
                                     description="evaluation & calibration interop workbench")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("gen-data", help="generate fixed-seed shards and configs")
    p.add_argument("--out", default="data")
    p.set_defaults(func=cmd_gen_data)

    p = sub.add_parser("run", help="run the 3-stage pipeline over shards")
    p.add_argument("--data", required=True, help="directory with *.jsonl shards")
    p.add_argument("--config", required=True)
    p.add_argument("--workspace", required=True)
    p.add_argument("--shards", nargs="*", default=None, help="restrict to shard ids")
    p.add_argument("--unit-delay-ms", type=int, default=0,
                   help="artificial per-unit delay (graceful-stop demos)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("status", help="show per-stage fingerprints and invalidation state")
    p.add_argument("--data", default=None)
    p.add_argument("--config", required=True)
    p.add_argument("--workspace", required=True)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("upgrade-config", help="upgrade a v1 config to v2 with audit notes")
    p.add_argument("--config", required=True)
    p.add_argument("--out", default=None)
    p.set_defaults(func=cmd_upgrade_config)

    p = sub.add_parser("roundtrip", help="verify round-trip chains for one shard file")
    p.add_argument("--shard", required=True)
    p.add_argument("--config", required=True)
    p.set_defaults(func=cmd_roundtrip)

    p = sub.add_parser("serve", help="start the local HTTP API")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8077)
    p.add_argument("--data", default="data/shards")
    p.add_argument("--config", default="data/configs/config_v2.json")
    p.set_defaults(func=cmd_serve)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
