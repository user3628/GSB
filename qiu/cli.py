"""Command-line interface for parse, version, experiment, and serve operations."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .engine import QueryEngine
from .errors import QueryError
from .experiment import experiment_bytes, run_experiment
from .io import canonical_json, read_jsonl
from .server import create_server

ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiu", description="Deterministic local query understanding engine")
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--training", type=Path)
    parser.add_argument("--dictionary", type=Path)
    parser.add_argument("--eval", type=Path)
    subparsers = parser.add_subparsers(dest="command", required=True)

    parse_parser = subparsers.add_parser("parse", help="parse one query or a JSONL batch")
    source = parse_parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--query")
    source.add_argument("--input", type=Path)
    parse_parser.add_argument("--query-id", default="cli-query-1")
    parse_parser.add_argument("--locale", default="zh-CN")
    parse_parser.add_argument("--channel", default="web")
    parse_parser.add_argument("--history-json", default="[]")
    parse_parser.add_argument("--faults-json", default="{}")

    experiment_parser = subparsers.add_parser("experiment", help="run the local evaluation set")
    experiment_parser.add_argument("--seed", type=int, default=17)
    experiment_parser.add_argument("--bootstrap-samples", type=int, default=200)
    experiment_parser.add_argument("--output", type=Path)

    subparsers.add_parser("version", help="show stage fingerprints and asset status")

    serve_parser = subparsers.add_parser("serve", help="start the local HTTP API")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=8765)

    demo_parser = subparsers.add_parser("demo", help="run the deterministic end-to-end demonstration")
    demo_parser.add_argument("--output-dir", type=Path, default=ROOT / "artifacts")
    demo_parser.add_argument("--seed", type=int, default=17)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config_path = args.config or args.data_dir / "config.json"
    training_path = args.training or args.data_dir / "training.jsonl"
    dictionary_path = args.dictionary or args.data_dir / "dictionary.json"
    eval_path = args.eval or args.data_dir / "eval.jsonl"

    try:
        if args.command == "demo":
            from .demo import run_demo

            report = run_demo(
                config_path=config_path,
                training_path=training_path,
                dictionary_path=dictionary_path,
                eval_path=eval_path,
                output_dir=args.output_dir,
                seed=args.seed,
            )
            sys.stdout.buffer.write(report)
            return 0

        engine = QueryEngine(
            config_path=config_path,
            training_path=training_path,
            dictionary_path=dictionary_path,
        )
        if args.command == "parse":
            if args.query is not None:
                history = json.loads(args.history_json)
                faults = json.loads(args.faults_json)
                sample = {
                    "channel": args.channel,
                    "history_categories": history,
                    "locale": args.locale,
                    "query_id": args.query_id,
                    "raw_query": args.query,
                }
                _write_stdout(engine.parse(sample, faults))
                return 0

            parsed: list[dict[str, Any]] = []
            for index, item in enumerate(read_jsonl(args.input), start=1):
                if "query" in item:
                    sample = item["query"]
                    faults = item.get("faults", {})
                else:
                    sample = dict(item)
                    faults = sample.pop("faults", {})
                if not isinstance(sample, dict) or not isinstance(faults, dict):
                    raise ValueError(f"input line {index} has invalid query/faults objects")
                parsed.append(engine.parse(sample, faults))
            _write_stdout(parsed[0] if len(parsed) == 1 else parsed)
            return 0

        if args.command == "experiment":
            summary = run_experiment(
                engine,
                eval_path=eval_path,
                seed=args.seed,
                bootstrap_samples=args.bootstrap_samples,
            )
            payload = experiment_bytes(summary)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_bytes(payload)
            sys.stdout.buffer.write(payload)
            return 0

        if args.command == "version":
            _write_stdout(engine.version_status())
            return 0

        if args.command == "serve":
            server = create_server(engine, eval_path=eval_path, host=args.host, port=args.port)
            print(f"QIU listening on http://{args.host}:{args.port}", file=sys.stderr, flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                server.server_close()
            return 0
    except QueryError as exc:
        sys.stdout.buffer.write((canonical_json(exc.to_dict()) + "\n").encode("utf-8"))
        return 2
    except (ValueError, json.JSONDecodeError) as exc:
        error = QueryError("E_BAD_REQUEST", "cli", str(exc)).to_dict()
        sys.stdout.buffer.write((canonical_json(error) + "\n").encode("utf-8"))
        return 2
    return 0


def _write_stdout(value: Any) -> None:
    sys.stdout.buffer.write((canonical_json(value) + "\n").encode("utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
