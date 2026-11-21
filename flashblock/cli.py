"""Command-line interface for the block attention library."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np

from .checkpoint import CheckpointStore
from .config import AttentionConfig
from .engine import AttentionEngine
from .errors import FlashBlockError
from .samples import generate_samples


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def _load_engine(args: argparse.Namespace) -> AttentionEngine:
    config = AttentionConfig.from_yaml(args.config)
    return AttentionEngine.from_npz(
        args.workspace,
        args.shard_id,
        args.input,
        config,
    )


def _save_result(path: str | Path, result: Any, *, q: np.ndarray | None = None, k: np.ndarray | None = None, v: np.ndarray | None = None) -> None:
    payload = {
        "output": result.output,
        "row_max": result.row_max,
        "row_norm": result.row_norm,
    }
    if q is not None:
        payload["q"] = q
    if k is not None:
        payload["k"] = k
    if v is not None:
        payload["v"] = v
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(path, **payload)


def cmd_generate(args: argparse.Namespace) -> int:
    manifest = generate_samples(args.root, args.seed)
    _print(manifest)
    return 0


def cmd_attention(args: argparse.Namespace) -> int:
    engine = _load_engine(args)
    result = engine.run(max_new_row_blocks=args.max_new_row_blocks)
    if args.output_npz and result.output is not None:
        _save_result(args.output_npz, result)
    _print(result.to_dict())
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    engine = _load_engine(args)
    result = engine.resume(
        max_new_row_blocks=args.max_new_row_blocks,
        start_row_block=args.start_row_block,
    )
    if args.output_npz and result.output is not None:
        _save_result(args.output_npz, result)
    _print(result.to_dict())
    return 0


def cmd_prefix_update(args: argparse.Namespace) -> int:
    engine = _load_engine(args)
    count = args.key_end - args.key_start
    if count <= 0:
        raise FlashBlockError("E_SHAPE", "key_end must be greater than key_start")
    shape = (engine.q.shape[0], engine.q.shape[1], count, engine.q.shape[3])
    if args.values_npz:
        with np.load(args.values_npz, allow_pickle=False) as payload:
            k_values = payload["k"]
            v_values = payload["v"]
    else:
        rng = np.random.default_rng(args.seed)
        k_values = rng.normal(0.0, 0.6, size=shape)
        v_values = rng.normal(0.0, 0.6, size=shape)
    result = engine.apply_prefix_update(
        key_start=args.key_start,
        key_end=args.key_end,
        k_values=k_values,
        v_values=v_values,
        verify=not args.no_verify,
    )
    if args.output_npz and result.output is not None:
        _save_result(
            args.output_npz,
            result,
            q=engine.q,
            k=engine.k,
            v=engine.v,
        )
    _print(result.to_dict())
    return 0


def cmd_state(args: argparse.Namespace) -> int:
    store = CheckpointStore(args.workspace, args.shard_id)
    _print(store.query_state())
    return 0


def cmd_invalidations(args: argparse.Namespace) -> int:
    store = CheckpointStore(args.workspace, args.shard_id)
    _print(store.query_invalidations())
    return 0


def cmd_budget(args: argparse.Namespace) -> int:
    engine = _load_engine(args)
    _print(engine.budget_report())
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from scripts.run_demo import run_demo

    summary = run_demo(args.root, seed=args.seed, clean=args.clean)
    _print(summary)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m flashblock", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate deterministic local Q/K/V shards")
    generate.add_argument("--root", default=".")
    generate.add_argument("--seed", type=int, default=20260301)
    generate.set_defaults(func=cmd_generate)

    attention = subparsers.add_parser("attention", help="Run or return cached block attention")
    attention.add_argument("--input", required=True)
    attention.add_argument("--shard-id", required=True)
    attention.add_argument("--config", required=True)
    attention.add_argument("--workspace", required=True)
    attention.add_argument("--max-new-row-blocks", type=int, default=None)
    attention.add_argument("--output-npz")
    attention.set_defaults(func=cmd_attention)

    resume = subparsers.add_parser("resume", help="Resume a paused checkpointed run")
    resume.add_argument("--input", required=True)
    resume.add_argument("--shard-id", required=True)
    resume.add_argument("--config", required=True)
    resume.add_argument("--workspace", required=True)
    resume.add_argument("--max-new-row-blocks", type=int, default=None)
    resume.add_argument("--start-row-block", type=int, default=None)
    resume.add_argument("--output-npz")
    resume.set_defaults(func=cmd_resume)

    update = subparsers.add_parser("prefix-update", help="Apply a prefix K/V correction incrementally")
    update.add_argument("--input", required=True)
    update.add_argument("--shard-id", required=True)
    update.add_argument("--config", required=True)
    update.add_argument("--workspace", required=True)
    update.add_argument("--key-start", type=int, required=True)
    update.add_argument("--key-end", type=int, required=True)
    update.add_argument("--values-npz", help="NPZ containing K/V slices with keys 'k' and 'v'")
    update.add_argument("--seed", type=int, default=20260302)
    update.add_argument("--no-verify", action="store_true")
    update.add_argument("--output-npz")
    update.set_defaults(func=cmd_prefix_update)

    state = subparsers.add_parser("state", help="Query persisted row-block state")
    state.add_argument("--workspace", required=True)
    state.add_argument("--shard-id", required=True)
    state.set_defaults(func=cmd_state)

    invalidations = subparsers.add_parser("invalidations", help="Query latest invalidation report")
    invalidations.add_argument("--workspace", required=True)
    invalidations.add_argument("--shard-id", required=True)
    invalidations.set_defaults(func=cmd_invalidations)

    budget = subparsers.add_parser("budget", help="Query recorded memory-planning decisions")
    budget.add_argument("--input", required=True)
    budget.add_argument("--shard-id", required=True)
    budget.add_argument("--config", required=True)
    budget.add_argument("--workspace", required=True)
    budget.set_defaults(func=cmd_budget)

    demo = subparsers.add_parser("demo", help="Run the complete deterministic demonstration")
    demo.add_argument("--root", default=".")
    demo.add_argument("--seed", type=int, default=20260301)
    demo.add_argument("--no-clean", action="store_false", dest="clean", default=True)
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except FlashBlockError as exc:
        _print(exc.to_dict())
        return 2
    except Exception as exc:
        _print({"ok": False, "code": "E_INTERNAL", "message": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())

