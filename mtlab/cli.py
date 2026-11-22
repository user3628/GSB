from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from . import ENGINE_VERSION, __version__
from .checkpoints import CheckpointStore
from .config import STAGE_ORDER, TIERS, ExperimentConfig
from .errors import GracefulStop, MtlError
from .generator import DEFAULT_SEED, generate_dataset
from .pipeline import query_status, resolve_dataset, run_experiment
from .policy_stage import load_policy, make_policy_decision


def _print_json(value: Any) -> None:
    sys.stdout.write(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n")


def _config_from_args(args: argparse.Namespace) -> ExperimentConfig:
    tiers = tuple(part.strip() for part in args.tiers.split(",") if part.strip())
    unknown = [tier for tier in tiers if tier not in TIERS]
    if unknown or not tiers:
        raise MtlError(
            code="E_INVALID_STAGE",
            message="invalid tier selection",
            details={"tiers": list(tiers), "allowed": list(TIERS)},
        )
    return ExperimentConfig(
        generator_seed=args.generator_seed,
        bpe_merges=args.bpe_merges,
        bpe_min_count=args.bpe_min_count,
        smoothing=args.smoothing,
        discard_threshold=args.discard_threshold,
        em_iterations=args.em_iterations,
        policy_budget=args.budget,
        beam_width_override=args.beam_width,
        phrase_limit_override=args.phrase_limit,
        phrase_max_len=args.phrase_max_len,
        metric_max_order=args.metric_max_order,
        confidence_level=args.confidence_level,
        tiers=tiers,
    )


def _add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workspace", default=".", help="workspace root")
    parser.add_argument("--data-dir", default="data", help="dataset directory (default: data)")
    parser.add_argument("--tiers", default="small,medium,large")
    parser.add_argument("--generator-seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--bpe-merges", type=int, default=40)
    parser.add_argument("--bpe-min-count", type=int, default=2)
    parser.add_argument("--smoothing", type=float, default=0.25)
    parser.add_argument("--discard-threshold", type=float, default=-12.0)
    parser.add_argument("--em-iterations", type=int, default=4)
    parser.add_argument("--budget", type=int, default=20_000_000)
    parser.add_argument("--beam-width", type=int, default=None)
    parser.add_argument("--phrase-limit", type=int, default=None)
    parser.add_argument("--phrase-max-len", type=int, default=3)
    parser.add_argument("--metric-max-order", type=int, default=4)
    parser.add_argument("--confidence-level", type=float, default=0.95)


def _cmd_generate(args: argparse.Namespace) -> int:
    root = Path(args.workspace).resolve()
    root.mkdir(parents=True, exist_ok=True)
    manifest = generate_dataset(root, seed=args.seed)
    payload = {
        "status": "generated",
        "schema_version": manifest["schema_version"],
        "generator_version": manifest["generator_version"],
        "seed": manifest["generator_seed"],
        "tiers": {
            tier: {
                "pair_count": data["pair_count"],
                "avg_token_length": data["avg_token_length"],
                "vocab_size_union": data["vocab_size_union"],
                "total_token_upper_bound": data["total_token_upper_bound"],
                "corpus_sha256": data["corpus_sha256"],
            }
            for tier, data in manifest["tiers"].items()
        },
    }
    _print_json(payload)
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    root = Path(args.workspace).resolve()
    root.mkdir(parents=True, exist_ok=True)
    store = CheckpointStore(root)
    health = store.healthcheck()
    import numpy

    payload = {
        "status": "ok",
        "engine_version": ENGINE_VERSION,
        "python_version": sys.version.split()[0],
        "python_311_compatible": sys.version_info >= (3, 11),
        "numpy_version": numpy.__version__,
        **health,
        "stages": list(STAGE_ORDER),
    }
    _print_json(payload)
    return 0


def _install_stop_handler(event: threading.Event) -> None:
    def handler(signum: int, frame: Any) -> None:
        event.set()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def _cmd_run(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    stop_event = threading.Event()
    _install_stop_handler(stop_event)
    stop_after = args.stop_after_tier or os.environ.get("MTLAB_STOP_AFTER_TIER")
    if stop_after and stop_after not in config.tiers:
        raise MtlError(
            code="E_INVALID_STAGE",
            message="stop-after tier is not selected",
            details={"stop_after_tier": stop_after, "tiers": list(config.tiers)},
        )
    summary = run_experiment(
        Path(args.workspace),
        args.data_dir,
        config,
        stop_event=stop_event,
        stop_after_tier=stop_after,
    )
    _print_json(
        {
            "status": "completed",
            "engine_version": ENGINE_VERSION,
            "summary_digest": summary["summary_digest"],
            "summary_path": "artifacts/run_summary.json",
            "checkpoint_sequences": {
                tier: [record["sequence"] for record in data["checkpoints"]]
                for tier, data in summary["tiers"].items()
            },
        }
    )
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    status = query_status(Path(args.workspace), args.data_dir, config)
    _print_json(status)
    return 0


def _cmd_verify_policy(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    root = Path(args.workspace).resolve()
    _, manifest = resolve_dataset(root, args.data_dir)
    store = CheckpointStore(root)
    results: dict[str, Any] = {}
    for tier in config.tiers:
        record = store.record_for_stage(tier, "policy")
        if record is None:
            results[tier] = {"valid": False, "reason": "missing_policy_checkpoint"}
            continue
        artifact = load_policy(store.artifact_path(record))
        recomputed = make_policy_decision(tier, manifest["tiers"][tier], config)
        results[tier] = {
            "valid": artifact["decision_fingerprint"] == recomputed["decision_fingerprint"],
            "stored_fingerprint": artifact["decision_fingerprint"],
            "recomputed_fingerprint": recomputed["decision_fingerprint"],
            "selected": recomputed["selected"],
        }
    valid = all(item["valid"] for item in results.values())
    _print_json({"valid": valid, "tiers": results})
    return 0 if valid else 1


def _cmd_summary(args: argparse.Namespace) -> int:
    summary_path = Path(args.workspace).resolve() / "artifacts" / "run_summary.json"
    if not summary_path.exists():
        raise MtlError(
            code="E_INVALID_STAGE",
            message="run summary is missing",
            details={"path": str(summary_path)},
        )
    with summary_path.open("r", encoding="utf-8") as handle:
        summary = json.load(handle)
    _print_json(
        {
            "summary_digest": summary["summary_digest"],
            "tiers": {
                tier: {
                    "selected": data["policy"],
                    "metrics": data["metrics"],
                }
                for tier, data in summary["tiers"].items()
            },
        }
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m mtlab",
        description="Deterministic local phrase-based MT experiment system",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="generate fixed-seed corpora")
    generate.add_argument("--workspace", default=".")
    generate.add_argument("--seed", type=int, default=DEFAULT_SEED)
    generate.set_defaults(func=_cmd_generate)

    doctor = subparsers.add_parser("doctor", help="check writable workspace and lock support")
    doctor.add_argument("--workspace", default=".")
    doctor.set_defaults(func=_cmd_doctor)

    run = subparsers.add_parser("run", help="run all five stages with checkpoint reuse")
    _add_config_args(run)
    run.add_argument("--stop-after-tier", default=None)
    run.set_defaults(func=_cmd_run)

    status = subparsers.add_parser("status", help="show checkpoint and invalidation status")
    _add_config_args(status)
    status.set_defaults(func=_cmd_status)

    verify = subparsers.add_parser("verify-policy", help="recompute policy fingerprints")
    _add_config_args(verify)
    verify.set_defaults(func=_cmd_verify_policy)

    summary = subparsers.add_parser("summary", help="print the deterministic run summary")
    summary.add_argument("--workspace", default=".")
    summary.set_defaults(func=_cmd_summary)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except GracefulStop as exc:
        _print_json(
            {
                "status": "stopped",
                "tier": exc.tier,
                "stage": exc.stage,
                "restart": "rerun the same command to resume from the last complete checkpoint",
            }
        )
        return 75
    except MtlError as exc:
        _print_json({"error": exc.as_dict()})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

