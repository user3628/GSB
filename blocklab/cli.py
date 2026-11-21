"""Command-line interface for the local BlockLab workbench."""

from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Any

from .engine import ExecutionEngine
from .errors import BusinessError
from .paths import STAGES
from .util import atomic_write_text, canonical_json, digest_bytes
from .workbench import Workbench


def _print(value: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False))
        return
    print(canonical_json(value))


def _status_text(status: dict[str, Any]) -> str:
    job = status["job"]
    lines = [
        f"Job {job['job_id']}  shard={job['shard_id']}  complete={'yes' if status['complete'] else 'no'}",
        f"source={status['source_shape']}  target={status['actual_target_shape']}  alerts={status['alerts']}  code={status['business_code'] or '-'}",
        f"version={status['version_fingerprint']}",
        "",
        f"{'stage':<12} {'epoch':>5} {'blocks':>10} {'next':>6} {'alerts':>7}  content_digest",
    ]
    for stage in STAGES:
        item = status["stages"][stage]
        lines.append(
            f"{stage:<12} {item['epoch']:>5} {item['valid_blocks']:>4}/{item['expected_blocks']:<5} "
            f"{item['next_sequence']:>6} {item['alerts']:>7}  {item['content_digest'] or '-'}"
        )
    if status.get("run_state"):
        run = status["run_state"]
        lines.extend(["", f"runner={run.get('status')} computed={run.get('computed_blocks', 0)} reused={run.get('reused_blocks', 0)}"])
    if status.get("last_error"):
        lines.extend(["", f"last_error={canonical_json(status['last_error'])}"])
    return "\n".join(lines)


def _run_text(result: dict[str, Any]) -> str:
    lines = [
        f"job={result['job_id']} status={result['status']} code={result.get('business_code') or '-'}",
        f"computed={result['computed_blocks']} reused={result['reused_blocks']} complete={result['complete']}",
    ]
    if result.get("stopped_at"):
        lines.append(f"stopped_at={canonical_json(result['stopped_at'])}")
    if result.get("summary_sha256"):
        lines.append(f"summary_sha256={result['summary_sha256']}")
    return "\n".join(lines)


def _json_dump(value: Any, *, indent: int = 2) -> str:
    return json.dumps(value, ensure_ascii=False, indent=indent, sort_keys=True, allow_nan=False)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="blocklab",
        description="Deterministic block activation/reorder workbench (local only)",
    )
    parser.add_argument("--root", default=".", help="workspace root (default: cwd)")
    parser.add_argument("--workdir", default="work", help="runtime directory inside root")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create runtime directories")
    samples = sub.add_parser("samples", help="regenerate deterministic samples")
    samples.add_argument("--out", default="samples")

    imp = sub.add_parser("import", help="register an NPZ shard and YAML config")
    imp.add_argument("--shard", required=True)
    imp.add_argument("--config", required=True)
    imp.add_argument("--job")
    imp.add_argument("--manifest")
    imp.add_argument("--shard-id")

    run = sub.add_parser("run", help="run or resume stages")
    run.add_argument("--job", required=True)
    run.add_argument("--stage", action="append", choices=list(STAGES) + ["all"])
    run.add_argument("--stop-after-stage", choices=list(STAGES))
    run.add_argument("--stop-after-blocks", type=int)
    run.add_argument("--block-delay-ms", type=int, default=0)
    run.add_argument("--resume", action="store_true", help="clear a pending stop before running")

    for name in ("status", "fingerprints", "invalidations"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--job", required=True)

    diagnostics = sub.add_parser("diagnostics")
    diagnostics.add_argument("--job", required=True)
    diagnostics.add_argument("--stage", choices=list(STAGES))

    blocks = sub.add_parser("blocks")
    blocks.add_argument("--job", required=True)
    blocks.add_argument("--stage", choices=list(STAGES))

    stop = sub.add_parser("stop", help="request graceful stop after the current block")
    stop.add_argument("--job", required=True)

    resume = sub.add_parser("resume", help="clear stop request and continue")
    resume.add_argument("--job", required=True)
    resume.add_argument("--stage", action="append", choices=list(STAGES) + ["all"])
    resume.add_argument("--block-delay-ms", type=int, default=0)

    export = sub.add_parser("export", help="export deterministic result summary")
    export.add_argument("--job", required=True)
    export.add_argument("--out", help="destination file inside workspace root")

    serve = sub.add_parser("serve", help="start local HTTP application")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)

    demo = sub.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo.add_argument("--workdir", default="demo_work")
    demo.add_argument("--report", default="artifacts/demo_report.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    raw_args = list(sys.argv[1:] if argv is None else argv)
    global_args: list[str] = []
    command_args: list[str] = []
    index = 0
    while index < len(raw_args):
        token = raw_args[index]
        if token == "--json":
            global_args.append(token)
            index += 1
        elif token in ("--root", "--workdir"):
            global_args.append(token)
            if index + 1 >= len(raw_args):
                parser.error(f"{token} requires a value")
            global_args.append(raw_args[index + 1])
            index += 2
        else:
            command_args.append(token)
            index += 1
    args = parser.parse_args(global_args + command_args)
    root = Path(args.root).resolve()
    wb = Workbench(root, args.workdir)
    engine = ExecutionEngine(wb)
    try:
        if args.command == "init":
            _print({"ok": True, "workdir": str(wb.paths.workdir)}, args.json)
            return 0
        if args.command == "samples":
            import subprocess

            output_dir = wb.paths.resolve_root_path(args.out)
            result = subprocess.run(
                [sys.executable, str(root / "samples" / "generator.py"), "--out", str(output_dir)],
                cwd=str(root),
                check=False,
                text=True,
                capture_output=True,
            )
            if result.returncode != 0:
                raise BusinessError("E_INPUT", "sample generation failed", stderr=result.stderr)
            _print(json.loads(result.stdout.strip()), args.json)
            return 0
        if args.command == "import":
            value = wb.import_job(
                source_path=args.shard,
                config_path=args.config,
                job_id=args.job,
                manifest_path=args.manifest,
                shard_id=args.shard_id,
            )
            _print(value, args.json)
            return 0
        if args.command in ("run", "resume"):
            stop_flag = [False]
            old_handlers: dict[int, Any] = {}

            def handler(_signum, _frame):
                stop_flag[0] = True

            for sig in (signal.SIGINT, signal.SIGTERM):
                old_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, handler)
            try:
                result = engine.run_job(
                    args.job,
                    stages=args.stage,
                    stop_after_stage=getattr(args, "stop_after_stage", None),
                    stop_after_blocks=getattr(args, "stop_after_blocks", None),
                    block_delay_ms=args.block_delay_ms,
                    clear_stop=bool(getattr(args, "resume", False) or args.command == "resume"),
                    external_stop=lambda: stop_flag[0],
                )
            finally:
                for sig, old in old_handlers.items():
                    signal.signal(sig, old)
            if args.json:
                _print(result, True)
            else:
                print(_run_text(result))
            return 0
        if args.command == "status":
            status = wb.job_status(args.job)
            if args.json:
                _print(status, True)
            else:
                print(_status_text(status))
            return 0
        if args.command == "fingerprints":
            _print(wb.fingerprints(args.job), args.json)
            return 0
        if args.command == "invalidations":
            _print(wb.invalidations(args.job), args.json)
            return 0
        if args.command == "diagnostics":
            _print(wb.diagnostics(args.job, stage=args.stage), args.json)
            return 0
        if args.command == "blocks":
            bundle = wb.scan_all_stages(args.job)
            blocks: list[dict[str, Any]] = []
            for stage in STAGES:
                if args.stage is not None and stage != args.stage:
                    continue
                scan = bundle["scans"][stage]
                for record in scan["records"]:
                    blocks.append(
                        {
                            "stage": stage,
                            "sequence": record["sequence"],
                            "epoch": record["epoch"],
                            "block": record["block"],
                            "status": record["status"],
                            "product_digest": record["product_digest"],
                            "diagnostics": record.get("metadata", {}).get("diagnostics"),
                            "metadata": record.get("metadata", {}),
                        }
                    )
            _print({"ok": True, "job_id": args.job, "blocks": blocks}, args.json)
            return 0
        if args.command == "stop":
            _print(wb.request_stop(args.job), args.json)
            return 0
        if args.command == "export":
            engine.ensure_outputs(args.job)
            summary_path = wb.paths.job(args.job).output_path("summary.json")
            if not summary_path.exists():
                raise BusinessError(
                    "E_INVALID_STAGE",
                    "summary is not available until all stages are complete",
                    job_id=args.job,
                )
            data = summary_path.read_text(encoding="utf-8")
            if args.out:
                destination = wb.paths.resolve_root_path(args.out)
                atomic_write_text(destination, data)
                _print(
                    {
                        "ok": True,
                        "job_id": args.job,
                        "path": destination.relative_to(root).as_posix(),
                        "sha256": digest_bytes(data.encode("utf-8")),
                    },
                    args.json,
                )
            else:
                print(data, end="")
            return 0
        if args.command == "serve":
            from .http_server import run_server

            run_server(root=root, workdir=args.workdir, host=args.host, port=args.port)
            return 0
        if args.command == "demo":
            from .demo import run_demo

            report = run_demo(root=root, workdir=args.workdir, report_path=root / args.report)
            _print(report, args.json)
            return 0
        parser.error("unknown command")
        return 2
    except BusinessError as error:
        value = error.to_dict()
        if args.json:
            print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(f"{error.code}: {error.message}", file=sys.stderr)
            if error.details:
                print(json.dumps(error.details, ensure_ascii=False, indent=2, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

