"""Command line interface for the workbench."""
from __future__ import annotations

import argparse
import json
import sys

from . import engine
from .errors import BenchError
from .generator import DEFAULT_MODEL_CONFIG, generate_all
from .state import load_state


def _print(payload) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lora-bench")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate deterministic samples/config/model")
    gen.add_argument("--data-dir", required=True)
    gen.add_argument("--global-cap", type=int, default=None)
    gen.add_argument("--quota", type=int, default=None)

    imp = sub.add_parser("import", help="import a config into a workspace")
    imp.add_argument("--workspace", required=True)
    imp.add_argument("--config", required=True)

    run = sub.add_parser("run", help="run admitted tasks")
    run.add_argument("--workspace", required=True)
    run.add_argument("--workers", type=int, default=None)

    stop = sub.add_parser("stop", help="request graceful stop")
    stop.add_argument("--workspace", required=True)

    resume = sub.add_parser("resume", help="resume from latest checkpoints")
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--task-id", default=None)

    state = sub.add_parser("state", help="show persisted state")
    state.add_argument("--workspace", required=True)

    budget = sub.add_parser("budget", help="show budget consumption and rejections")
    budget.add_argument("--workspace", required=True)

    summary = sub.add_parser("summary", help="build and print deterministic summary")
    summary.add_argument("--workspace", required=True)

    export = sub.add_parser("export-loss", help="export loss curves as CSV")
    export.add_argument("--workspace", required=True)

    args = parser.parse_args(argv)
    try:
        if args.command == "generate":
            overrides = {}
            if args.global_cap is not None:
                overrides["global_sample_cap"] = args.global_cap
            if args.quota is not None:
                overrides["per_task_sample_quota"] = args.quota
            _print(generate_all(args.data_dir, budget_override=overrides or None))
            return 0
        if args.command == "import":
            result = engine.import_work(
                args.workspace, args.config, DEFAULT_MODEL_CONFIG, reimport=True
            )
            _print(result.to_dict())
            return 0
        if args.command == "run":
            _print(engine.run(args.workspace, max_workers=args.workers))
            return 0
        if args.command == "stop":
            engine.stop(args.workspace)
            _print({"status": "stop_requested"})
            return 0
        if args.command == "resume":
            _print(engine.resume(args.workspace, task_id=args.task_id))
            return 0
        if args.command == "state":
            current = load_state(args.workspace)
            if current is None:
                raise BenchError("no persisted state", code="E_NOT_FOUND")
            _print(current.to_dict())
            return 0
        if args.command == "budget":
            import os

            path = f"{args.workspace}/admission.json"
            ledger = {}
            if os.path.exists(path):
                with open(path, encoding="utf-8") as handle:
                    ledger = json.load(handle)
            current = load_state(args.workspace)
            _print(
                {
                    "budget": current.budget,
                    "global_cap": current.global_cap,
                    "global_consumed": current.global_consumed,
                    "admission": ledger,
                }
            )
            return 0
        if args.command == "summary":
            _print(engine.build_summary(args.workspace))
            return 0
        if args.command == "export-loss":
            summary = engine.build_summary(args.workspace)
            print("task_id,step,loss")
            for task in summary["tasks"]:
                for step, loss in enumerate(task["loss_history"], start=1):
                    print(f"{task['task_id']},{step},{loss:.12g}")
            return 0
    except BenchError as exc:
        print(json.dumps({"error": exc.to_dict()}, ensure_ascii=False), file=sys.stderr)
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
