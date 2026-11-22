from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable, Sequence

from .config import ExperimentConfig
from .errors import MtlError, decode_error
from .policy_stage import load_policy
from .util import (
    iter_jsonl,
    round_float,
    sha256_json,
    write_json_atomic,
    write_jsonl_atomic,
)

DECODE_SCHEMA = "monotone-phrase-beam-v1"
PHRASE_SMOOTHING = 1e-4
_PUNCT_SPACE_RE = re.compile(r"\s+([.,!?;:])")


def decode_input_descriptor(
    tier: str,
    config: ExperimentConfig,
    *,
    split_digest: str,
    alignment_digest: str,
    policy_digest: str,
    alignment_mode: str,
    phrase_limit: int,
    beam_width: int,
) -> dict[str, Any]:
    return {
        "engine_version": config.engine_version,
        "stage": "decode",
        "decode_schema": DECODE_SCHEMA,
        "tier": tier,
        "split_digest": split_digest,
        "alignment_digest": alignment_digest,
        "policy_digest": policy_digest,
        "alignment_mode": alignment_mode,
        "phrase_limit": int(phrase_limit),
        "beam_width": int(beam_width),
        "phrase_max_len": int(config.phrase_max_len),
    }


def _target_surface(record: dict[str, Any], target_tokens: Sequence[dict[str, Any]]) -> str:
    normalized = record["normalized_target"]
    return normalized[int(target_tokens[0]["start"]) : int(target_tokens[-1]["end"])]


def extract_phrase_counts(
    records: Sequence[dict[str, Any]],
    alignments: dict[str, dict[str, Any]],
    *,
    alignment_mode: str,
    max_phrase_len: int,
    stop_check: Callable[[], None] | None = None,
) -> tuple[Counter[tuple[tuple[str, ...], tuple[str, ...]]], Counter[tuple[str, ...]], dict[tuple[tuple[str, ...], tuple[str, ...]], str]]:
    phrase_counts: Counter[tuple[tuple[str, ...], tuple[str, ...]]] = Counter()
    source_counts: Counter[tuple[str, ...]] = Counter()
    surfaces: dict[tuple[tuple[str, ...], tuple[str, ...]], str] = {}
    for record in records:
        if stop_check:
            stop_check()
        alignment = alignments.get(record["pair_id"])
        if alignment is None:
            raise MtlError(
                code="E_INVALID_STAGE",
                message="alignment artifact lacks a split-stage pair",
                details={"pair_id": record["pair_id"], "stage": "decode"},
            )
        alignment_key = "word_exact" if alignment_mode == "word_exact" else "subword_approx"
        links = alignment["exact" if alignment_key == "word_exact" else "approx"]
        source_tokens = record["source_tokens"]
        target_tokens = record["target_tokens"]
        link_by_source: dict[int, list[int]] = defaultdict(list)
        link_targets: set[int] = set()
        for link in links:
            link_by_source[int(link["source_pos"])].append(int(link["target_pos"]))
            link_targets.add(int(link["target_pos"]))
        source_len = len(source_tokens)
        target_len = len(target_tokens)
        for span_len in range(1, max_phrase_len + 1):
            for start in range(0, source_len - span_len + 1):
                end = start + span_len
                linked_targets = sorted(
                    target_pos
                    for source_pos in range(start, end)
                    for target_pos in link_by_source.get(source_pos, [])
                )
                if not linked_targets:
                    continue
                target_start = linked_targets[0]
                target_end = linked_targets[-1]
                if target_start < 0 or target_end >= target_len:
                    continue
                # A target token in the proposed span must be licensed by a
                # source token in the same span; this keeps phrase extraction
                # consistent with the selected alignment.
                if any(
                    target_pos in link_targets
                    and not any(
                        target_pos in link_by_source.get(source_pos, [])
                        for source_pos in range(start, end)
                    )
                    for target_pos in range(target_start, target_end + 1)
                ):
                    continue
                source_phrase = tuple(str(token["text"]) for token in source_tokens[start:end])
                target_span_tokens = target_tokens[target_start : target_end + 1]
                target_phrase = tuple(str(token["text"]) for token in target_span_tokens)
                if not source_phrase or not target_phrase:
                    continue
                key = (source_phrase, target_phrase)
                phrase_counts[key] += 1
                source_counts[source_phrase] += 1
                surfaces.setdefault(key, _target_surface(record, target_span_tokens))
    return phrase_counts, source_counts, surfaces


def prune_phrase_table(
    phrase_counts: Counter[tuple[tuple[str, ...], tuple[str, ...]]],
    source_counts: Counter[tuple[str, ...]],
    surfaces: dict[tuple[tuple[str, ...], tuple[str, ...]], str],
    *,
    phrase_limit: int,
    alignment_mode: str,
    target_vocab_size: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    all_entries: list[dict[str, Any]] = []
    for (source_phrase, target_phrase), count in phrase_counts.items():
        source_count = source_counts[source_phrase]
        if source_count > 0:
            base_probability = count / source_count
            uniform_probability = 1.0 / max(2, target_vocab_size)
            probability = (
                (1.0 - PHRASE_SMOOTHING) * base_probability
                + PHRASE_SMOOTHING * uniform_probability
            )
            log_p = math.log(probability)
        else:
            log_p = float("-inf")
        all_entries.append(
            {
                "source": list(source_phrase),
                "target": list(target_phrase),
                "target_surface": surfaces[(source_phrase, target_phrase)],
                "count": int(count),
                "source_count": int(source_count),
                "log_p": round_float(log_p),
            }
        )

    unigrams = [
        entry
        for entry in all_entries
        if len(entry["source"]) == 1
    ]
    unigrams.sort(
        key=lambda item: (
            item["source"][0],
            -float(item["log_p"]),
            tuple(item["target"]),
        )
    )
    remaining = [entry for entry in all_entries if len(entry["source"]) > 1]
    remaining.sort(
        key=lambda item: (
            -float(item["log_p"]),
            len(item["source"]),
            tuple(item["source"]),
            tuple(item["target"]),
        )
    )
    limit = max(0, int(phrase_limit))
    kept = unigrams[:limit]
    kept_keys = {(tuple(item["source"]), tuple(item["target"])) for item in kept}
    for entry in remaining:
        if len(kept) >= limit:
            break
        key = (tuple(entry["source"]), tuple(entry["target"]))
        if key not in kept_keys:
            kept.append(entry)
            kept_keys.add(key)
    kept.sort(
        key=lambda item: (
            len(item["source"]),
            tuple(item["source"]),
            -float(item["log_p"]),
            tuple(item["target"]),
        )
    )
    stats = {
        "candidate_entry_count": len(all_entries),
        "kept_entry_count": len(kept),
        "pruned_entry_count": len(all_entries) - len(kept),
        "unigram_entry_count": len(unigrams),
        "unique_source_count": len(source_counts),
        "phrase_limit": limit,
        "alignment_mode": alignment_mode,
        "phrase_smoothing": PHRASE_SMOOTHING,
        "target_vocab_size": int(target_vocab_size),
    }
    return kept, stats


def _candidate_text(surfaces: Sequence[str], target_lang: str) -> str:
    cleaned = [surface.strip() for surface in surfaces if surface.strip()]
    if not cleaned:
        return ""
    if target_lang == "zh":
        return "".join(cleaned)
    text = " ".join(cleaned)
    text = _PUNCT_SPACE_RE.sub(r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def decode_record(
    record: dict[str, Any],
    phrase_index: dict[str, list[dict[str, Any]]],
    *,
    beam_width: int,
) -> dict[str, Any]:
    pair_id = record["pair_id"]
    if beam_width <= 0:
        raise decode_error(
            pair_id=pair_id,
            step_index=0,
            message="search width must be positive",
        )
    source_tokens = record["source_tokens"]
    source_len = len(source_tokens)
    start_hyp = {"pos": 0, "target_tokens": (), "surfaces": (), "log_score": 0.0}
    beams: dict[int, list[dict[str, Any]]] = {0: [start_hyp]}
    trace_steps: list[dict[str, Any]] = []
    total_expansions = 0
    total_pruned = 0

    for source_pos in range(source_len):
        hypotheses = beams.get(source_pos, [])
        if not hypotheses:
            if source_pos == 0:
                raise decode_error(
                    pair_id=pair_id,
                    step_index=source_pos,
                    message="no phrase starts at source position",
                )
            continue
        hypotheses = sorted(
            hypotheses,
            key=lambda hyp: (
                -float(hyp["log_score"]),
                tuple(hyp["target_tokens"]),
                tuple(hyp["surfaces"]),
            ),
        )[:beam_width]
        expansions_this_step = 0
        best_score = hypotheses[0]["log_score"] if hypotheses else float("-inf")
        for hypothesis in hypotheses:
            start_text = str(source_tokens[source_pos]["text"])
            for phrase in phrase_index.get(start_text, []):
                phrase_source = phrase["source"]
                phrase_len = len(phrase_source)
                if source_pos + phrase_len > source_len:
                    continue
                if tuple(
                    str(token["text"]) for token in source_tokens[source_pos : source_pos + phrase_len]
                ) != tuple(phrase_source):
                    continue
                new_score = float(hypothesis["log_score"]) + float(phrase["log_p"])
                if not math.isfinite(new_score):
                    raise decode_error(
                        pair_id=pair_id,
                        step_index=source_pos,
                        message="non-finite beam score",
                    )
                new_hyp = {
                    "pos": source_pos + phrase_len,
                    "target_tokens": tuple(hypothesis["target_tokens"]) + tuple(phrase["target"]),
                    "surfaces": tuple(hypothesis["surfaces"]) + (phrase["target_surface"],),
                    "log_score": new_score,
                }
                expansions_this_step += 1
                beams.setdefault(new_hyp["pos"], []).append(new_hyp)

        # Deduplicate each destination position and retain a stable beam.
        for destination in sorted(beams):
            if destination <= source_pos:
                continue
            grouped: dict[tuple[tuple[str, ...], tuple[str, ...]], dict[str, Any]] = {}
            for hypothesis in beams[destination]:
                key = (tuple(hypothesis["target_tokens"]), tuple(hypothesis["surfaces"]))
                previous = grouped.get(key)
                if previous is None or hypothesis["log_score"] > previous["log_score"]:
                    grouped[key] = hypothesis
            ordered = sorted(
                grouped.values(),
                key=lambda hyp: (
                    -float(hyp["log_score"]),
                    tuple(hyp["target_tokens"]),
                    tuple(hyp["surfaces"]),
                ),
            )
            if len(ordered) > beam_width:
                total_pruned += len(ordered) - beam_width
                ordered = ordered[:beam_width]
            beams[destination] = ordered
        total_expansions += expansions_this_step
        trace_steps.append(
            {
                "source_pos": source_pos,
                "hypotheses": len(hypotheses),
                "expansions": expansions_this_step,
                "best_log_score": round_float(best_score),
            }
        )

    final_hypotheses = beams.get(source_len, [])
    if not final_hypotheses:
        raise decode_error(
            pair_id=pair_id,
            step_index=source_len,
            message="no complete hypothesis reached sentence end",
        )
    final = sorted(
        final_hypotheses,
        key=lambda hyp: (
            -float(hyp["log_score"]),
            tuple(hyp["target_tokens"]),
            tuple(hyp["surfaces"]),
        ),
    )[0]
    log_score = float(final["log_score"])
    if not math.isfinite(log_score):
        raise decode_error(
            pair_id=pair_id,
            step_index=source_len,
            message="NaN or infinite final score",
        )
    normalized_score = log_score / max(1, len(final["target_tokens"]))
    if normalized_score == 0.0 or not math.isfinite(normalized_score):
        raise decode_error(
            pair_id=pair_id,
            step_index=source_len,
            message="zero or non-finite length-normalized score",
        )
    trace = {
        "steps": trace_steps,
        "expansions": total_expansions,
        "pruned": total_pruned,
        "trace_sha256": sha256_json(trace_steps),
    }
    candidate_text = _candidate_text(final["surfaces"], record["target_lang"])
    if not candidate_text:
        raise decode_error(
            pair_id=pair_id,
            step_index=source_len,
            message="empty candidate surface",
        )
    return {
        "pair_id": pair_id,
        "source_lang": record["source_lang"],
        "target_lang": record["target_lang"],
        "lang_pair": record["lang_pair"],
        "domain": record["domain"],
        "candidate_tokens": list(final["target_tokens"]),
        "candidate_text": candidate_text,
        "log_score": round_float(log_score),
        "normalized_score": round_float(normalized_score),
        "search": trace,
    }


def run_decode(
    split_artifact: Path,
    alignment_artifact: Path,
    policy_artifact_path: Path,
    tier: str,
    config: ExperimentConfig,
    out_dir: Path,
    *,
    split_artifact_digest: str,
    alignment_artifact_digest: str,
    policy_artifact_digest: str,
    stop_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    records = list(iter_jsonl(split_artifact / "tokens.jsonl"))
    alignments = {row["pair_id"]: row for row in iter_jsonl(alignment_artifact / "alignments.jsonl")}
    policy_artifact = load_policy(policy_artifact_path)
    selected = policy_artifact["decision"]["selected"]
    alignment_mode = selected["alignment_mode"]
    phrase_limit = int(selected["phrase_limit"])
    beam_width = int(selected["beam_width"])

    phrase_counts, source_counts, surfaces = extract_phrase_counts(
        records,
        alignments,
        alignment_mode=alignment_mode,
        max_phrase_len=config.phrase_max_len,
        stop_check=stop_check,
    )
    target_vocab_size = len(
        {
            str(token["text"])
            for record in records
            for token in record["target_tokens"]
        }
    )
    table, table_stats = prune_phrase_table(
        phrase_counts,
        source_counts,
        surfaces,
        phrase_limit=phrase_limit,
        alignment_mode=alignment_mode,
        target_vocab_size=target_vocab_size,
    )
    phrase_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for phrase in table:
        phrase_index[phrase["source"][0]].append(phrase)
    for key in phrase_index:
        phrase_index[key].sort(
            key=lambda item: (
                -float(item["log_p"]),
                len(item["source"]),
                tuple(item["source"]),
                tuple(item["target"]),
            )
        )

    decoded: list[dict[str, Any]] = []
    for record in records:
        if stop_check:
            stop_check()
        decoded.append(decode_record(record, phrase_index, beam_width=beam_width))

    phrase_artifact = {
        "schema_version": 1,
        "tier": tier,
        "alignment_mode": alignment_mode,
        "beam_width": beam_width,
        "stats": table_stats,
        "entries": table,
    }
    summary = {
        "schema_version": 1,
        "tier": tier,
        "decoded_pair_count": len(decoded),
        "alignment_mode": alignment_mode,
        "phrase_limit": phrase_limit,
        "phrase_table_kept": len(table),
        "phrase_table_pruned": table_stats["pruned_entry_count"],
        "beam_width": beam_width,
        "average_normalized_score": round_float(
            sum(float(row["normalized_score"]) for row in decoded) / max(1, len(decoded))
        ),
        "total_search_expansions": sum(int(row["search"]["expansions"]) for row in decoded),
        "total_search_pruned": sum(int(row["search"]["pruned"]) for row in decoded),
        "trace_digest": sha256_json([row["search"]["trace_sha256"] for row in decoded]),
    }
    write_json_atomic(
        out_dir / "_meta.json",
        {
            "schema_version": 1,
            "tier": tier,
            "stage": "decode",
            "stage_index": 4,
            "output_schema": DECODE_SCHEMA,
            "input_fingerprint": sha256_json(
                decode_input_descriptor(
                    tier,
                    config,
                    split_digest=split_artifact_digest,
                    alignment_digest=alignment_artifact_digest,
                    policy_digest=policy_artifact_digest,
                    alignment_mode=alignment_mode,
                    phrase_limit=phrase_limit,
                    beam_width=beam_width,
                )
            ),
        },
    )
    write_json_atomic(out_dir / "phrase_table.json", phrase_artifact)
    write_jsonl_atomic(out_dir / "decoded.jsonl", decoded)
    write_json_atomic(out_dir / "summary.json", summary)
    return summary


def load_decoded(artifact: Path) -> list[dict[str, Any]]:
    return list(iter_jsonl(artifact / "decoded.jsonl"))

