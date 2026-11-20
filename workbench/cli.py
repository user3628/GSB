"""Command line interface.

Exit codes: 0 ok / graceful stop, 2 business error (E_xxx JSON on stderr),
1 unexpected error.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import signal
import sys
import threading

from . import __version__
from .config import SECTIONS, bump_config, default_config_store, validate_config
from .engine import Engine
from .errors import ConfigError, SampleError, WorkbenchError
from .sample import DEFAULT_ROWS, DEFAULT_SEED, generate_csv
from .store import Store
from .util import read_csv_columns, sha256_file


def _print_json(obj, stream=None):
    stream = stream or sys.stdout
    stream.write(json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True) + "\n")


def _parse_value(raw: str):
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def _set_dotted(cfg: dict, dotted: str, value):
    parts = dotted.split(".")
    node = cfg
    for part in parts[:-1]:
        if part not in node or not isinstance(node[part], dict):
            node[part] = {}
        node = node[part]
    node[parts[-1]] = value


def _cmd_gen_sample(args):
    meta = generate_csv(args.out, seed=args.seed, rows=args.rows)
    return {"generated": meta}


def _cmd_gen_config(args):
    from .config import DEFAULT_CONFIG

    parent = os.path.dirname(os.path.abspath(args.out))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(DEFAULT_CONFIG, f, indent=2, ensure_ascii=False)
        f.write("\n")
    return {"written": args.out}


def _cmd_config_init(args):
    store = Store(args.state)
    store.ensure()
    if store.load_config_store() is not None and not args.force:
        raise ConfigError("config already initialized; use config-set or --force")
    if args.from_file:
        with open(args.from_file, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        validate_config(cfg)
        obj = {
            "version": 1,
            "section_versions": {s: 1 for s in SECTIONS},
            "config": cfg,
        }
    else:
        obj = default_config_store()
    store.write_config_store(obj)
    return {"config_version": obj["version"], "section_versions": obj["section_versions"]}


def _cmd_config_show(args):
    store = Store(args.state)
    obj = store.load_config_store()
    if obj is None:
        raise ConfigError("config not initialized; run 'config-init' first")
    return obj


def _cmd_config_set(args):
    store = Store(args.state)
    obj = store.load_config_store()
    if obj is None:
        raise ConfigError("config not initialized; run 'config-init' first")
    if args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            new_cfg = json.load(f)
    else:
        new_cfg = copy.deepcopy(obj["config"])
        for pair in args.set or []:
            key, _, raw = pair.partition("=")
            if not key or not _:
                raise ConfigError("--set expects key=value", {"got": pair})
            _set_dotted(new_cfg, key, _parse_value(raw))
    validate_config(new_cfg)
    new_obj = bump_config(obj, new_cfg)
    store.write_config_store(new_obj)
    return {
        "config_version": new_obj["version"],
        "section_versions": new_obj["section_versions"],
        "changed_sections": sorted(
            s for s, v in new_obj["section_versions"].items()
            if v != obj["section_versions"][s]
        ),
    }


def _cmd_import_sample(args):
    store = Store(args.state)
    store.ensure()
    if not os.path.exists(args.path):
        raise SampleError("sample file not found", {"path": args.path})
    header, columns = read_csv_columns(args.path)
    rows = len(columns[header[0]]) if header else 0
    reg = {
        "path": os.path.abspath(args.path),
        "sha256": sha256_file(args.path),
        "rows": rows,
        "columns": header,
    }
    store.write_sample_registry(reg)
    return {"imported": reg}


def _run_with_signals(args):
    engine = Engine(args.state)
    stop = threading.Event()
    previous = {}

    def handler(signum, frame):
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        previous[sig] = signal.signal(sig, handler)
    try:
        report = engine.run(
            budget_ms=args.budget_ms,
            stage_sleep_ms=args.stage_sleep_ms,
            stop_flag=stop.is_set,
        )
    finally:
        for sig, prev in previous.items():
            signal.signal(sig, prev)
    if report["status"] == "stopped":
        sys.stderr.write(
            "graceful stop: completed stages %s; journal head preserved\n"
            % report.get("completed_stages", [])
        )
    return report


def _cmd_stop(args):
    store = Store(args.state)
    store.ensure()
    store.request_stop()
    return {"stop_requested": True}


def _cmd_status(args):
    return Engine(args.state).status()


def _cmd_stage(args):
    return Engine(args.state).stage_artifact(args.stage)


def _cmd_selection(args):
    return Engine(args.state).selection()


def _cmd_verify(args):
    return Engine(args.state).verify()


def _cmd_serve(args):
    from .server import serve

    return serve(args.state, args.host, args.port)


def build_parser():
    p = argparse.ArgumentParser(prog="workbench", description=__doc__)
    p.add_argument("--state", default=".workbench_state", help="state directory")
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("gen-sample", help="generate deterministic sample CSV")
    s.add_argument("--out", required=True)
    s.add_argument("--seed", type=int, default=DEFAULT_SEED)
    s.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    s.set_defaults(func=_cmd_gen_sample)

    s = sub.add_parser("gen-config", help="write the default JSON config")
    s.add_argument("--out", required=True)
    s.set_defaults(func=_cmd_gen_config)

    s = sub.add_parser("config-init", help="initialize config in the state dir")
    s.add_argument("--from", dest="from_file", default=None)
    s.add_argument("--force", action="store_true")
    s.set_defaults(func=_cmd_config_init)

    s = sub.add_parser("config-show", help="print current config and versions")
    s.set_defaults(func=_cmd_config_show)

    s = sub.add_parser("config-set", help="update config (bumps version)")
    s.add_argument("--set", action="append", metavar="KEY=VALUE",
                   help="dotted key assignment, repeatable")
    s.add_argument("--file", help="replace the whole config from a JSON file")
    s.set_defaults(func=_cmd_config_set)

    s = sub.add_parser("import-sample", help="register a sample CSV")
    s.add_argument("--path", required=True)
    s.set_defaults(func=_cmd_import_sample)

    for name, help_text in (
        ("run", "run the pipeline (incremental)"),
        ("resume", "resume from the last consistent journal record"),
    ):
        s = sub.add_parser(name, help=help_text)
        s.add_argument("--budget-ms", type=float, default=None)
        s.add_argument("--stage-sleep-ms", type=float, default=0,
                       help="demo/testing aid: sleep after each stage")
        s.set_defaults(func=_run_with_signals)

    s = sub.add_parser("stop", help="request a graceful stop")
    s.set_defaults(func=_cmd_stop)

    s = sub.add_parser("status", help="journal head, versions, stage fingerprints")
    s.set_defaults(func=_cmd_status)

    s = sub.add_parser("stage", help="print a stage artifact")
    s.add_argument("stage", choices=["infer", "encode", "select", "summarize"])
    s.set_defaults(func=_cmd_stage)

    s = sub.add_parser("selection", help="print kept/dropped feature lists")
    s.set_defaults(func=_cmd_selection)

    s = sub.add_parser("verify", help="full-rebuild consistency check")
    s.set_defaults(func=_cmd_verify)

    s = sub.add_parser("serve", help="start the local HTTP interface")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8765)
    s.set_defaults(func=_cmd_serve)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.func(args)
    except WorkbenchError as exc:
        _print_json({"ok": False, "error": exc.to_dict()}, stream=sys.stderr)
        return 2
    except BrokenPipeError:
        return 0
    except Exception as exc:  # unexpected
        import traceback

        traceback.print_exc(file=sys.stderr)
        _print_json(
            {"ok": False,
             "error": {"code": "E_INTERNAL", "message": str(exc), "details": {}}},
            stream=sys.stderr,
        )
        return 1
    if result is not None:
        _print_json(result)
    return 0
