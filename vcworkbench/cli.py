from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes
from .demo import generate_demo
from .errors import OperationStopped, WorkbenchError
from .manifest import load_manifest
from .pipeline import PipelineRunner
from .server import run_server
from .storage import STAGES, CheckpointStore
from .verification import run_acceptance
from .workspace import Workspace


def _scalar(value: str) -> Any:
    try:
        return json.loads(value)
    except Exception:
        return value


def _key_value(text: str, *, option: str) -> tuple[str, Any]:
    if "=" not in text:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            f"{option} expects key=value",
            stage="cli",
            value=text,
        )
    key, raw = text.split("=", 1)
    if not key:
        raise WorkbenchError(
            "E_INVALID_PARAM",
            f"{option} key must not be empty",
            stage="cli",
            value=text,
        )
    return key, _scalar(raw)


def _injections(args: argparse.Namespace, shard_ids: list[str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if getattr(args, "injections_json", None):
        try:
            parsed = json.loads(args.injections_json)
        except Exception as exc:
            raise WorkbenchError(
                "E_INVALID_PARAM", f"invalid --injections-json: {exc}", stage="cli"
            ) from exc
        if not isinstance(parsed, dict):
            raise WorkbenchError(
                "E_INVALID_PARAM", "--injections-json must be an object", stage="cli"
            )
        result.update(parsed)
    for shard_id in shard_ids:
        result.setdefault(shard_id, {})
    for text in getattr(args, "inject", []) or []:
        key, value = _key_value(text, option="--inject")
        for shard_id in shard_ids:
            result.setdefault(shard_id, {})[key] = value
    for text in getattr(args, "inject_shard", []) or []:
        if ":" not in text:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "--inject-shard expects shard:key=value",
                stage="cli",
                value=text,
            )
        shard_id, pair = text.split(":", 1)
        if shard_id not in shard_ids:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "--inject-shard references an unselected shard",
                shard_id=shard_id,
                stage="cli",
                selected_shard_ids=shard_ids,
            )
        key, value = _key_value(pair, option="--inject-shard")
        result.setdefault(shard_id, {})[key] = value
    return {key: value for key, value in result.items() if value}


def _write_json(value: Any) -> None:
    print(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
    )


def _print_convert(result: dict[str, Any]) -> None:
    for item in result["results"]:
        output = item["output"]
        diag = item["diagnostics"]
        print(
            f"{item['shard_id']}: samples={output['sample_count']} "
            f"clips={output['clip_count']} pcm={output['pcm_sha256'][:16]}... "
            f"in_range={diag['in_range_ratio']:.3f}"
        )
        print(
            "  stages: "
            + ", ".join(f"{stage['stage']}={stage['status']}" for stage in item["stages"])
        )
        stream = item.get("stream_consistency", {})
        print(
            f"  stream={bool(stream.get('verified'))} "
            f"earliest_invalidated={item.get('earliest_invalidated_stage') or 'none'}"
        )


def _add_convert_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--shard", action="append", dest="shards", help="shard id; repeatable")
    parser.add_argument("--block-frames", type=int, default=8, help="frame block size for streaming")
    parser.add_argument("--no-stream-check", action="store_true", help="skip full-vs-stream comparison")
    parser.add_argument("--inject", action="append", default=[], metavar="KEY=VALUE")
    parser.add_argument(
        "--inject-shard", action="append", default=[], metavar="SHARD:KEY=VALUE"
    )
    parser.add_argument("--injections-json", metavar="JSON")
    parser.add_argument(
        "--stop-after-blocks",
        type=int,
        help="simulate an operator stop request after N reconstruction blocks",
    )
    parser.add_argument("--resume", action="store_true", help="clear a saved stop marker first")


def _convert_from_args(workspace: Workspace, args: argparse.Namespace) -> dict[str, Any]:
    manifest = workspace.require_manifest()
    selected = args.shards or [spec.shard_id for spec in manifest.shards]
    injections = _injections(args, selected)
    runner = PipelineRunner(
        workspace,
        stop_after_blocks=args.stop_after_blocks,
    )
    return runner.convert(
        shard_ids=selected,
        block_frames=args.block_frames,
        verify_stream=not args.no_stream_check,
        injections=injections,
        clear_stop=bool(args.resume),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vcworkbench",
        description="Deterministic local streaming voice-conversion workbench",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--workspace",
        default="workbench_data",
        help="workspace containing manifest.json, samples/, state/ and exports/",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="generate deterministic WAV shards and manifest")
    demo.add_argument("--force", action="store_true")
    demo.add_argument("--verify", action="store_true", help="run the complete acceptance suite")

    convert = sub.add_parser("convert", help="run four-stage conversion")
    _add_convert_options(convert)

    resume = sub.add_parser("resume", help="clear stop marker and continue incomplete stages")
    _add_convert_options(resume)
    resume.set_defaults(resume=True)

    stream = sub.add_parser("stream", help="run and inspect frame-block streaming inference")
    stream.add_argument("--shard", action="append", dest="shards")
    stream.add_argument("--block-frames", type=int, default=4)
    stream.add_argument("--chunks-dir", help="write each raw chunk as PCM16LE")

    diagnose = sub.add_parser("diagnose", help="show pitch-alignment diagnostics")
    diagnose.add_argument("--shard", required=True)
    diagnose.add_argument("--frame", type=int)
    diagnose.add_argument("--latency-budget-ms", type=float)

    report = sub.add_parser("report", help="export a conversion report")
    report.add_argument("--shard", required=True)
    report.add_argument("--out")

    export = sub.add_parser("export", help="export converted WAV")
    export.add_argument("--shard", required=True)
    export.add_argument("--out")

    fingerprints = sub.add_parser("fingerprints", help="show stage fingerprints and checkpoints")
    fingerprints.add_argument("--shard", required=True)

    sub.add_parser("state", help="show workspace and checkpoint state")

    stop = sub.add_parser("stop", help="write a graceful stop request")
    stop.add_argument("--reason", default="operator request")
    stop.add_argument("--shard", action="append", dest="scope")

    imp = sub.add_parser("import", help="import an external manifest and local WAV files")
    imp.add_argument("--manifest", required=True)

    serve = sub.add_parser("serve", help="start the local HTTP workbench")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.add_argument("--no-auto-demo", action="store_true")

    verify = sub.add_parser("verify", help="run the full acceptance suite in a temp workspace")
    verify.add_argument("--out", help="write the verification JSON to this path")
    # Global switches are commonly typed after the subcommand as well. Keep a
    # single destination while accepting both positions.
    for child in sub.choices.values():
        child.add_argument("--workspace", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
        child.add_argument("--json", action="store_true", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    return parser


def _state_payload(workspace: Workspace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": True,
        "workspace": str(workspace.root),
        "ready": workspace.has_manifest(),
        "stop": workspace.stop_status(),
    }
    if not workspace.has_manifest():
        return payload
    manifest = load_manifest(workspace.manifest_path)
    store = CheckpointStore(workspace.state_dir)
    payload["manifest_digest"] = manifest.digest()
    payload["shards"] = [
        {"spec": spec.to_dict(), "checkpoints": store.status(spec.shard_id)}
        for spec in manifest.shards
    ]
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    workspace = Workspace(Path(args.workspace))
    try:
        if args.command == "demo":
            result = generate_demo(workspace, force=args.force)
            if args.verify:
                acceptance = run_acceptance()
                result["acceptance"] = acceptance
            _write_json(result) if args.json else _print_json_or_message(result)
            if args.verify and not result.get("acceptance", {}).get("ok", False):
                return 1
            return 0

        if args.command == "convert":
            result = _convert_from_args(workspace, args)
            _write_json(result) if args.json else _print_convert(result)
            return 0

        if args.command == "resume":
            result = _convert_from_args(workspace, args)
            _write_json(result) if args.json else _print_convert(result)
            return 0

        if args.command == "stream":
            manifest = workspace.require_manifest()
            selected = args.shards or [spec.shard_id for spec in manifest.shards]
            runner = PipelineRunner(workspace)
            results = []
            for shard_id in selected:
                result = runner.stream_check(shard_id, block_frames=args.block_frames)
                chunks = result.pop("chunks")
                result["chunks"] = [
                    {key: value for key, value in chunk.items() if key != "pcm_base64"}
                    for chunk in chunks
                ]
                if args.chunks_dir:
                    import base64

                    destination = Path(args.chunks_dir)
                    destination.mkdir(parents=True, exist_ok=True)
                    for index, chunk in enumerate(chunks):
                        (destination / f"{shard_id}.chunk_{index:04d}.pcm").write_bytes(
                            base64.b64decode(chunk["pcm_base64"])
                        )
                results.append(result)
            output = {"ok": True, "status": "verified", "results": results}
            _write_json(output) if args.json else _write_json(output)
            return 0

        if args.command == "diagnose":
            start = time.perf_counter()
            runner = PipelineRunner(workspace)
            if args.frame is None:
                output = runner.diagnostics(args.shard)
            else:
                output = runner.diagnose_frame(args.shard, args.frame)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if args.latency_budget_ms is not None and elapsed_ms > args.latency_budget_ms:
                raise WorkbenchError(
                    "E_LATENCY_BUDGET",
                    "frame diagnostic exceeded the latency budget",
                    shard_id=args.shard,
                    stage="diagnostics",
                    details={
                        "frame_index": args.frame,
                        "latency_budget_ms": args.latency_budget_ms,
                        "elapsed_ms": round(elapsed_ms, 3),
                    },
                )
            output["latency_ms"] = round(elapsed_ms, 3)
            _write_json(output)
            return 0

        if args.command == "report":
            runner = PipelineRunner(workspace)
            output_path = Path(args.out) if args.out else None
            result = runner.export_report(args.shard, output_path)
            if args.json:
                _write_json(result)
            else:
                print(f"report={result['path']} sha256={result['sha256']}")
            return 0

        if args.command == "export":
            destination = Path(args.out) if args.out else workspace.exports_dir / f"{args.shard}.wav"
            result = PipelineRunner(workspace).export_wav(args.shard, destination)
            _write_json(result) if args.json else print(
                f"wav={result['path']} bytes={result['bytes']} sha256={result['sha256']}"
            )
            return 0

        if args.command == "fingerprints":
            store = CheckpointStore(workspace.state_dir)
            status = store.status(args.shard, verify_artifacts=True)
            output = {
                "ok": "error" not in status,
                "shard_id": args.shard,
                "fingerprints": {
                    stage: (status.get("current", {}).get(stage) or {}).get("input_fingerprint")
                    for stage in STAGES
                },
                "checkpoints": status,
            }
            _write_json(output)
            return 0

        if args.command == "state":
            output = _state_payload(workspace)
            _write_json(output)
            return 0

        if args.command == "stop":
            stop = workspace.request_stop(
                reason=args.reason, scope=args.scope or ["all"]
            )
            _write_json({"ok": True, "stop": stop})
            return 0

        if args.command == "import":
            manifest = workspace.import_manifest(Path(args.manifest))
            output = {
                "ok": True,
                "status": "imported",
                "manifest_digest": manifest.digest(),
                "shard_ids": [spec.shard_id for spec in manifest.shards],
            }
            _write_json(output)
            return 0

        if args.command == "serve":
            server = run_server(
                workspace,
                host=args.host,
                port=args.port,
                auto_demo=not args.no_auto_demo,
            )
            url = f"http://{args.host}:{server.server_address[1]}"
            if args.json:
                _write_json({"ok": True, "url": url, "workspace": str(workspace.root)})
            else:
                print(f"VC workbench listening on {url}")
                print(f"workspace: {workspace.root}")
                print("press Ctrl+C to stop")
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0

        if args.command == "verify":
            result = run_acceptance()
            if args.out:
                output_path = Path(args.out)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(canonical_json_bytes(result) + b"\n")
                result["output_path"] = str(output_path)
            _write_json(result)
            return 0 if result["ok"] else 1

        parser.error(f"unknown command: {args.command}")
        return 2
    except OperationStopped as exc:
        payload = exc.to_dict()
        if args.json:
            _write_json(payload)
        else:
            print(
                f"stopped shard={exc.shard_id} stage={exc.stage} "
                f"after block={exc.blocks_completed}",
                file=sys.stderr,
            )
        return 3
    except WorkbenchError as exc:
        payload = exc.to_dict()
        if args.json:
            _write_json(payload)
        else:
            error = payload["error"]
            print(
                f"{error['code']} stage={error['stage']} shard={error['shard_id']}: "
                f"{error['message']}",
                file=sys.stderr,
            )
            if error.get("details"):
                print(json.dumps(error["details"], ensure_ascii=False, indent=2), file=sys.stderr)
        return 2


def _print_json_or_message(result: dict[str, Any]) -> None:
    if result.get("status") == "generated":
        print(
            f"generated workspace={result['workspace']} shards={','.join(result['shard_ids'])} "
            f"seed={result['seed']} samples={result['sample_count']}"
        )
        if "acceptance" in result:
            acceptance = result["acceptance"]
            print(
                f"acceptance={acceptance['status']} checks={acceptance['check_count']} "
                f"baseline={json.dumps(acceptance['baseline'], ensure_ascii=False)}"
            )
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
