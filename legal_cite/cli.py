"""Command-line interface for the citation resolution component."""

import argparse
import json
import sys
from pathlib import Path

from .batch import batch_vs_per_contract, evaluate, recompute_summary
from .config import default_config
from .errors import LegalCiteError
from .evidence import import_manifest
from .pipeline import StopController, run_pipeline, run_pipeline_resumable
from .sample_generator import generate
from .serde import canonical_text, read_jsonl, write_json_atomic


def _load_shard_paths(args):
    return [Path(p) for p in args.shards]


def cmd_generate(args):
    info = generate(args.output_dir)
    print(json.dumps(info, ensure_ascii=False, indent=2, sort_keys=True))


def _all_records(shard_paths):
    records = []
    for path in shard_paths:
        records.extend(read_jsonl(path))
    return records


def cmd_resolve(args):
    cfg = default_config()
    records = _all_records(_load_shard_paths(args))
    if args.contract:
        records = [r for r in records if r["contract_id"] == args.contract]
    state = run_pipeline(records, cfg=cfg, ckpt_dir=args.checkpoint_dir)
    payload = {
        "pipeline_fingerprint": state["pipeline_fingerprint"],
        "decisions": state["manifest"]["decisions"],
        "folded_duplicates": state["folded_duplicates"],
        "diagnostics": state["diagnostics"],
        "checkpoint": state.get("checkpoint"),
    }
    print(canonical_text(payload, indent=2))


def cmd_evidence(args):
    blob = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    cfg = default_config()
    from .config import config_fingerprint
    imported = import_manifest(blob, cfg, config_fingerprint(cfg))
    if args.citation_id:
        chains = [c for c in imported["evidence_chains"]
                  if c["citation_id"] == args.citation_id]
    else:
        chains = imported["evidence_chains"]
    print(canonical_text(chains, indent=2))


def cmd_batch(args):
    cfg = default_config()
    records = _all_records(_load_shard_paths(args))
    gold = read_jsonl(args.gold)
    if args.consistency:
        summary = batch_vs_per_contract(records, gold, cfg)
    else:
        summary, _ = recompute_summary(records, gold, cfg)
    if args.output:
        write_json_atomic(Path(args.output), summary, indent=2)
    print(canonical_text(summary, indent=2))


def cmd_stop_resume(args):
    cfg = default_config()
    records = _all_records(_load_shard_paths(args))
    controller = StopController()
    # Request a stop immediately: stage 1 finishes the current citation unit,
    # then exits without a partial checkpoint.
    controller.request_stop()
    partial = run_pipeline(records, cfg=cfg, ckpt_dir=args.checkpoint_dir,
                           stop_controller=controller)
    resumed = run_pipeline_resumable(records, cfg=cfg,
                                     ckpt_dir=args.checkpoint_dir)
    payload = {
        "first_run_stop_requested": partial["stop_requested"],
        "resumed_pipeline_fingerprint": resumed["pipeline_fingerprint"],
        "checkpoint": resumed.get("checkpoint"),
        "decision_count": len(resumed["manifest"]["decisions"]),
    }
    print(canonical_text(payload, indent=2))


def build_parser():
    parser = argparse.ArgumentParser(
        prog="legal-cite",
        description="Cross-language legal citation conflict resolution")
    sub = parser.add_subparsers(dest="command", required=True)

    gen = sub.add_parser("generate", help="generate deterministic sample data")
    gen.add_argument("--output-dir", default="data")
    gen.set_defaults(func=cmd_generate)

    res = sub.add_parser("resolve", help="resolve citations (one contract optional)")
    res.add_argument("shards", nargs="+")
    res.add_argument("--contract")
    res.add_argument("--checkpoint-dir", default="checkpoints")
    res.set_defaults(func=cmd_resolve)

    ev = sub.add_parser("evidence", help="query evidence chains from a manifest")
    ev.add_argument("manifest")
    ev.add_argument("--citation-id")
    ev.set_defaults(func=cmd_evidence)

    bat = sub.add_parser("batch", help="batch recompute and metrics")
    bat.add_argument("shards", nargs="+")
    bat.add_argument("--gold", required=True)
    bat.add_argument("--consistency", action="store_true")
    bat.add_argument("--output")
    bat.set_defaults(func=cmd_batch)

    sr = sub.add_parser("stop-resume", help="demonstrate graceful stop + resume")
    sr.add_argument("shards", nargs="+")
    sr.add_argument("--checkpoint-dir", default="checkpoints")
    sr.set_defaults(func=cmd_stop_resume)
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except LegalCiteError as exc:
        print(json.dumps(exc.to_dict(), ensure_ascii=False, sort_keys=True),
              file=sys.stderr)
        return 2
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        print(json.dumps({"code": "E_IO", "message": str(exc)},
                         ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
