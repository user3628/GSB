from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Callable

from .config import ExperimentConfig
from .errors import empty_sentence_error
from .tokenizer import (
    _expand_atoms,
    effective_bpe_limit,
    has_content_tokens,
    learn_and_apply_bpe,
    normalize_text,
    pretokenize,
    public_token,
)
from .util import (
    iter_jsonl,
    sha256_file,
    sha256_json,
    write_json_atomic,
    write_jsonl_atomic,
)

SPLIT_SCHEMA = "split-v1"


def split_input_descriptor(
    root: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
) -> dict[str, Any]:
    corpus_path = root / manifest["corpus_file"]
    return {
        "engine_version": config.engine_version,
        "stage": "split",
        "split_schema": SPLIT_SCHEMA,
        "tier": tier,
        "corpus_sha256": sha256_file(corpus_path),
        "normalizer": "nfkc_ws_lower_v1",
        "tokenizer": "cjk_char_latin_run_v1",
        "bpe_merges": int(config.bpe_merges),
        "effective_bpe_limit": effective_bpe_limit(config.bpe_merges, tier),
        "bpe_min_count": int(config.bpe_min_count),
        "bpe_scope": "per_language_offset_adjacent",
    }


def _write_meta(out_dir: Path, *, tier: str, descriptor: dict[str, Any]) -> None:
    write_json_atomic(
        out_dir / "_meta.json",
        {
            "schema_version": 1,
            "tier": tier,
            "stage": "split",
            "stage_index": 1,
            "output_schema": SPLIT_SCHEMA,
            "input_fingerprint": sha256_json(descriptor),
        },
    )


def run_split(
    root: Path,
    tier: str,
    config: ExperimentConfig,
    manifest: dict[str, Any],
    out_dir: Path,
    *,
    stop_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    corpus_path = root / manifest["corpus_file"]
    valid: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    source_atoms_all: list[list[dict[str, object]]] = []
    target_atoms_all: list[list[dict[str, object]]] = []

    for row in iter_jsonl(corpus_path):
        if stop_check:
            stop_check()
        normalized_source = normalize_text(row.get("source", ""))
        normalized_target = normalize_text(row.get("target", ""))
        source_base = pretokenize(normalized_source)
        target_base = pretokenize(normalized_target)
        source_ok = has_content_tokens(source_base)
        target_ok = has_content_tokens(target_base)
        if not source_ok or not target_ok:
            side = "both" if not source_ok and not target_ok else ("source" if not source_ok else "target")
            diagnostics.append(
                empty_sentence_error(
                    pair_id=row["pair_id"],
                    tier=tier,
                    side=side,
                    source=row.get("source", ""),
                    target=row.get("target", ""),
                    noise_flags=row.get("noise_flags", []),
                )
            )
            continue
        source_atoms = _expand_atoms(normalized_source, source_base)
        target_atoms = _expand_atoms(normalized_target, target_base)
        valid.append(
            {
                "pair_id": row["pair_id"],
                "source_lang": row["source_lang"],
                "target_lang": row["target_lang"],
                "lang_pair": row["lang_pair"],
                "domain": row["domain"],
                "noise_flags": sorted(row.get("noise_flags", [])),
                "normalized_source": normalized_source,
                "normalized_target": normalized_target,
            }
        )
        source_atoms_all.append(source_atoms)
        target_atoms_all.append(target_atoms)

    merge_limit = effective_bpe_limit(config.bpe_merges, tier)
    sequences = [sequence for pair in zip(source_atoms_all, target_atoms_all) for sequence in pair]
    rules, merged_sequences = learn_and_apply_bpe(
        sequences,
        merge_limit=merge_limit,
        min_count=config.bpe_min_count,
        stop_check=stop_check,
    )

    records: list[dict[str, Any]] = []
    source_token_counts: Counter[str] = Counter()
    target_token_counts: Counter[str] = Counter()
    source_token_total = 0
    target_token_total = 0
    for index, record in enumerate(valid):
        source_tokens = [public_token(atom) for atom in merged_sequences[2 * index]]
        target_tokens = [public_token(atom) for atom in merged_sequences[2 * index + 1]]
        source_token_total += len(source_tokens)
        target_token_total += len(target_tokens)
        source_token_counts.update(str(token["text"]) for token in source_tokens)
        target_token_counts.update(str(token["text"]) for token in target_tokens)
        record["source_tokens"] = source_tokens
        record["target_tokens"] = target_tokens
        records.append(record)

    rule_dicts = [rule.as_dict() for rule in rules]
    bpe_artifact = {
        "schema_version": 1,
        "tier": tier,
        "merge_limit": merge_limit,
        "min_count": config.bpe_min_count,
        "learned_rule_count": len(rules),
        "rules": rule_dicts,
        "rule_digest": sha256_json(rule_dicts),
        "source_vocab": [
            {"token": token, "count": count} for token, count in sorted(source_token_counts.items())
        ],
        "target_vocab": [
            {"token": token, "count": count} for token, count in sorted(target_token_counts.items())
        ],
    }
    summary = {
        "schema_version": 1,
        "tier": tier,
        "pair_count": manifest["pair_count"],
        "valid_pair_count": len(records),
        "diagnostic_count": len(diagnostics),
        "diagnostics_by_code": dict(
            sorted(Counter(item["code"] for item in diagnostics).items())
        ),
        "diagnostics_by_side": dict(
            sorted(Counter(item["side"] for item in diagnostics).items())
        ),
        "bpe": {
            "merge_limit": merge_limit,
            "learned_rule_count": len(rules),
            "rule_digest": bpe_artifact["rule_digest"],
            "source_vocab_size": len(source_token_counts),
            "target_vocab_size": len(target_token_counts),
            "source_token_total": source_token_total,
            "target_token_total": target_token_total,
        },
        "normalized_source_char_total": sum(len(r["normalized_source"]) for r in records),
        "normalized_target_char_total": sum(len(r["normalized_target"]) for r in records),
    }

    _write_meta(out_dir, tier=tier, descriptor=split_input_descriptor(root, tier, config, manifest))
    write_jsonl_atomic(out_dir / "tokens.jsonl", records)
    write_jsonl_atomic(out_dir / "diagnostics.jsonl", diagnostics)
    write_json_atomic(out_dir / "bpe.json", bpe_artifact)
    write_json_atomic(out_dir / "summary.json", summary)
    return summary


def load_split_records(root: Path, artifact_path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(root / artifact_path / "tokens.jsonl"))


def load_split_diagnostics(root: Path, artifact_path: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(root / artifact_path / "diagnostics.jsonl"))

