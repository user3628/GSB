from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np

from .config import ExperimentConfig
from .errors import numeric_error
from .tokenizer import pretokenize
from .util import iter_jsonl, round_float, sha256_json, write_json_atomic, write_jsonl_atomic

ALIGNMENT_SCHEMA = "log-ibm1-symmetric-onebest-v1"
EXPECTED_LANGUAGE_PAIRS = ("en-zh", "zh-en")

def _first_position(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if records:
        record = records[0]
        return {
            "pair_id": record["pair_id"],
            "source_pos": 0,
            "target_pos": 0,
            "source_token": record["source_tokens"][0]["text"]
            if record.get("source_tokens")
            else None,
            "target_token": record["target_tokens"][0]["text"]
            if record.get("target_tokens")
            else None,
            "count": 0,
        }
    return {
        "pair_id": None,
        "source_pos": 0,
        "target_pos": 0,
        "source_token": None,
        "target_token": None,
        "count": 0,
    }


def _validate_inputs(
    counts: np.ndarray,
    source_vocab: list[str],
    target_vocab: list[str],
    *,
    lang_pair: str,
    records: Sequence[dict[str, Any]],
    smoothing: float,
) -> None:
    positions = [_first_position(records)]
    if not records or counts.size == 0 or not source_vocab or not target_vocab:
        raise numeric_error(
            lang_pair=lang_pair,
            message="insufficient corpus to estimate a positive transition table",
            positions=positions,
        )
    if smoothing <= 0.0:
        raise numeric_error(
            lang_pair=lang_pair,
            message="smoothing must be positive in log-domain estimation",
            positions=positions,
        )
    if np.any(counts < 0):
        bad = np.argwhere(counts < 0)[0]
        raise numeric_error(
            lang_pair=lang_pair,
            message="non-positive co-occurrence count",
            positions=[
                {
                    "pair_id": records[0]["pair_id"],
                    "source_pos": int(bad[0]),
                    "target_pos": int(bad[1]),
                    "source_token": source_vocab[int(bad[0])],
                    "target_token": target_vocab[int(bad[1])],
                    "count": int(counts[bad[0], bad[1]]),
                }
            ],
        )
    observed = counts[counts > 0]
    if observed.size == 0 or np.any(observed <= 0) or not np.all(np.isfinite(observed)):
        raise numeric_error(
            lang_pair=lang_pair,
            message="no positive observed co-occurrence count",
            positions=positions,
        )


def _logsumexp_rows(values: np.ndarray) -> np.ndarray:
    maxima = np.max(values, axis=1, keepdims=True)
    return maxima + np.log(np.sum(np.exp(values - maxima), axis=1, keepdims=True))


def estimate_log_transition(
    counts: np.ndarray,
    *,
    smoothing: float,
    iterations: int,
    sequences: Sequence[tuple[np.ndarray, np.ndarray]],
    lang_pair: str,
    records: Sequence[dict[str, Any]],
    source_vocab: list[str],
    target_vocab: list[str],
) -> np.ndarray:
    """Estimate IBM-1 transition probabilities entirely in log space."""
    _validate_inputs(
        counts,
        source_vocab,
        target_vocab,
        lang_pair=lang_pair,
        records=records,
        smoothing=smoothing,
    )
    source_totals = counts.sum(axis=1, keepdims=True).astype(np.float64)
    target_size = len(target_vocab)
    with np.errstate(divide="raise", invalid="raise"):
        logp = np.log(counts.astype(np.float64) + smoothing) - np.log(
            source_totals + smoothing * target_size
        )
    if not np.all(np.isfinite(logp)):
        raise numeric_error(
            lang_pair=lang_pair,
            message="non-finite transition probability in log domain",
            positions=[_first_position(records)],
        )

    for _ in range(max(0, iterations)):
        expected = np.zeros_like(logp, dtype=np.float64)
        for source_ids, target_ids in sequences:
            scores = logp[np.ix_(source_ids, target_ids)]
            if not np.all(np.isfinite(scores)):
                raise numeric_error(
                    lang_pair=lang_pair,
                    message="non-finite alignment score",
                    positions=[_first_position(records)],
                )
            log_z = _logsumexp_rows(scores)
            posterior = np.exp(scores - log_z)
            src, tgt = np.meshgrid(source_ids, target_ids, indexing="ij")
            np.add.at(expected, (src.ravel(), tgt.ravel()), posterior.ravel())
        with np.errstate(divide="raise", invalid="raise"):
            expected_totals = expected.sum(axis=1, keepdims=True)
            logp = np.log(expected + smoothing) - np.log(
                expected_totals + smoothing * target_size
            )
        if not np.all(np.isfinite(logp)):
            raise numeric_error(
                lang_pair=lang_pair,
                message="non-finite EM update",
                positions=[_first_position(records)],
            )
    return logp


def _prune_log_table(
    logp: np.ndarray,
    source_vocab: list[str],
    target_vocab: list[str],
    *,
    threshold: float,
    lang_pair: str,
    records: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    pruned = 0
    for source_index, source_token in enumerate(source_vocab):
        values = logp[source_index]
        best_index = int(np.argmax(values))
        keep = [
            target_index
            for target_index in range(len(target_vocab))
            if values[target_index] >= threshold or target_index == best_index
        ]
        pruned += len(target_vocab) - len(keep)
        if not keep:
            raise numeric_error(
                lang_pair=lang_pair,
                message="discard threshold erased an entire alignment row",
                positions=[
                    {
                        "pair_id": records[0]["pair_id"] if records else None,
                        "source_pos": source_index,
                        "target_pos": best_index,
                        "source_token": source_token,
                        "target_token": target_vocab[best_index],
                        "count": 0,
                    }
                ],
            )
        rows.append(
            {
                "source": source_token,
                "targets": [
                    [target_vocab[target_index], round_float(float(values[target_index]))]
                    for target_index in keep
                ],
            }
        )
    return rows, pruned


def _overlapping_token_indices(
    tokens: Sequence[dict[str, Any]], start: int, end: int
) -> list[int]:
    return [
        idx
        for idx, token in enumerate(tokens)
        if int(token["end"]) > start and int(token["start"]) < end
    ]


def _unit_link_rows(
    record: dict[str, Any],
    logp: np.ndarray,
    source_index: dict[str, int],
    target_index: dict[str, int],
    *,
    approximate: bool,
) -> list[dict[str, Any]]:
    """Align pretokenized word units, then project links to BPE tokens.

    Exact mode uses a monotone, length-normalized one-best over local source
    units with lexical scores from the log transition table.  Approximate mode
    uses only proportional unit positions, which is the low-parameter option
    selected for small data.
    """
    source_units = pretokenize(record["normalized_source"])
    target_units = pretokenize(record["normalized_target"])
    if not source_units or not target_units:
        return []
    source_tokens = record["source_tokens"]
    target_tokens = record["target_tokens"]
    source_unit_tokens = [
        _overlapping_token_indices(source_tokens, int(unit["start"]), int(unit["end"]))
        for unit in source_units
    ]
    target_unit_tokens = [
        _overlapping_token_indices(target_tokens, int(unit["start"]), int(unit["end"]))
        for unit in target_units
    ]
    source_unit_ids = [
        [source_index[str(source_tokens[idx]["text"])] for idx in indices]
        for indices in source_unit_tokens
    ]
    target_unit_ids = [
        [target_index[str(target_tokens[idx]["text"])] for idx in indices]
        for indices in target_unit_tokens
    ]

    def lexical_score(source_unit_idx: int, target_unit_idx: int) -> float:
        source_unit = source_units[source_unit_idx]
        target_unit = target_units[target_unit_idx]
        source_punct = str(source_unit["lang"]) in {"punct", "sym"}
        target_punct = str(target_unit["lang"]) in {"punct", "sym"}
        if source_punct != target_punct:
            return float("-inf")
        source_ids = source_unit_ids[source_unit_idx]
        target_ids = target_unit_ids[target_unit_idx]
        if not source_ids or not target_ids:
            return float("-inf")
        per_source: list[float] = []
        for source_id in source_ids:
            per_source.append(max(float(logp[source_id, target_id]) for target_id in target_ids))
        return sum(per_source) / len(per_source)

    aligned_units: list[tuple[int, int, float]] = []
    source_count = len(source_units)
    target_count = len(target_units)
    for source_unit_idx in range(source_count):
        expected = round(source_unit_idx * max(0, target_count - 1) / max(1, source_count - 1))
        if approximate or source_count == target_count:
            target_unit_idx = min(target_count - 1, max(0, expected))
            selected_score = lexical_score(source_unit_idx, target_unit_idx)
        else:
            low = max(0, expected - 1)
            high = min(target_count - 1, expected + 1)
            candidates: list[tuple[float, int]] = []
            for candidate in range(low, high + 1):
                score = lexical_score(source_unit_idx, candidate)
                source_ratio = (source_unit_idx + 1.0) / (source_count + 1.0)
                target_ratio = (candidate + 1.0) / (target_count + 1.0)
                score -= 0.75 * abs(source_ratio - target_ratio)
                candidates.append((score, candidate))
            selected_score, target_unit_idx = sorted(
                candidates, key=lambda item: (-item[0], item[1])
            )[0]
        aligned_units.append((source_unit_idx, target_unit_idx, float(selected_score)))

    mapping = {
        source_unit_idx: (target_unit_idx, unit_score)
        for source_unit_idx, target_unit_idx, unit_score in aligned_units
    }
    unit_log_probs: dict[int, float] = {}
    for source_unit_idx in range(source_count):
        candidate_scores: list[float] = []
        for target_unit_idx in range(target_count):
            score = lexical_score(source_unit_idx, target_unit_idx)
            if not math.isfinite(score):
                continue
            source_ratio = (source_unit_idx + 1.0) / (source_count + 1.0)
            target_ratio = (target_unit_idx + 1.0) / (target_count + 1.0)
            candidate_scores.append(score - 0.75 * abs(source_ratio - target_ratio))
        selected_score = mapping[source_unit_idx][1]
        if candidate_scores and math.isfinite(selected_score):
            max_score = max(candidate_scores)
            log_z = max_score + math.log(
                sum(math.exp(score - max_score) for score in candidate_scores)
            )
            unit_log_probs[source_unit_idx] = selected_score - log_z
        else:
            unit_log_probs[source_unit_idx] = -30.0

    links_by_pair: dict[tuple[int, int], tuple[float, float]] = {}
    for source_unit_idx, target_unit_idx, unit_score in aligned_units:
        for source_token_idx in source_unit_tokens[source_unit_idx]:
            for target_token_idx in target_unit_tokens[target_unit_idx]:
                key = (source_token_idx, target_token_idx)
                previous = links_by_pair.get(key)
                value = (unit_score, unit_log_probs[source_unit_idx])
                if previous is None or value[0] > previous[0]:
                    links_by_pair[key] = value
    links: list[dict[str, Any]] = []
    for (source_pos, target_pos), (_, log_align_prob) in sorted(links_by_pair.items()):
        links.append(
            {
                "source_pos": source_pos,
                "target_pos": target_pos,
                # Nine decimals are ample for a diagnostic link score and keep
                # artifacts byte-stable across supported NumPy/libm versions.
                "log_align_prob": round_float(log_align_prob, 9),
            }
        )
    return links


def alignment_input_descriptor_from_records(
    tier: str,
    config: ExperimentConfig,
    split_artifact_digest: str,
) -> dict[str, Any]:
    return {
        "engine_version": config.engine_version,
        "stage": "align",
        "alignment_schema": ALIGNMENT_SCHEMA,
        "tier": tier,
        "split_digest": split_artifact_digest,
        "smoothing": float(config.smoothing),
        "discard_threshold": float(config.discard_threshold),
        "em_iterations": int(config.em_iterations),
    }


def run_alignment(
    split_artifact: Path,
    tier: str,
    config: ExperimentConfig,
    out_dir: Path,
    *,
    split_artifact_digest: str,
    stop_check: Callable[[], None] | None = None,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    records = list(iter_jsonl(split_artifact / "tokens.jsonl"))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if stop_check:
            stop_check()
        grouped[record["lang_pair"]].append(record)

    language_pairs: dict[str, Any] = {}
    alignment_rows: list[dict[str, Any]] = []
    total_links = 0
    total_pruned = 0
    for lang_pair in EXPECTED_LANGUAGE_PAIRS:
        pair_records = grouped.get(lang_pair, [])
        if not pair_records:
            raise numeric_error(
                lang_pair=lang_pair,
                message="insufficient corpus for language pair",
                positions=[_first_position(pair_records)],
            )
        source_vocab = sorted(
            {str(token["text"]) for record in pair_records for token in record["source_tokens"]}
        )
        target_vocab = sorted(
            {str(token["text"]) for record in pair_records for token in record["target_tokens"]}
        )
        source_index = {token: idx for idx, token in enumerate(source_vocab)}
        target_index = {token: idx for idx, token in enumerate(target_vocab)}
        counts = np.zeros((len(source_vocab), len(target_vocab)), dtype=np.float64)
        sequences: list[tuple[np.ndarray, np.ndarray]] = []
        id_records: list[dict[str, Any]] = []
        for record in pair_records:
            if stop_check:
                stop_check()
            source_ids = np.array(
                [source_index[str(token["text"])] for token in record["source_tokens"]],
                dtype=np.int64,
            )
            target_ids = np.array(
                [target_index[str(token["text"])] for token in record["target_tokens"]],
                dtype=np.int64,
            )
            mesh_source, mesh_target = np.meshgrid(source_ids, target_ids, indexing="ij")
            np.add.at(counts, (mesh_source.ravel(), mesh_target.ravel()), 1.0)
            sequences.append((source_ids, target_ids))
            id_records.append(
                {"pair_id": record["pair_id"], "source_ids": source_ids, "target_ids": target_ids}
            )

        logp = estimate_log_transition(
            counts,
            smoothing=config.smoothing,
            iterations=config.em_iterations,
            sequences=sequences,
            lang_pair=lang_pair,
            records=pair_records,
            source_vocab=source_vocab,
            target_vocab=target_vocab,
        )
        table_rows, pruned = _prune_log_table(
            logp,
            source_vocab,
            target_vocab,
            threshold=config.discard_threshold,
            lang_pair=lang_pair,
            records=pair_records,
        )
        total_pruned += pruned
        language_pairs[lang_pair] = {
            "sentence_pair_count": len(pair_records),
            "source_vocab_size": len(source_vocab),
            "target_vocab_size": len(target_vocab),
            "observed_cooccurrence_cells": int(np.count_nonzero(counts)),
            "pruned_transition_cells": pruned,
            "log_transition": table_rows,
        }

        for record, ids in zip(pair_records, id_records):
            if stop_check:
                stop_check()
            source_ids = ids["source_ids"]
            target_ids = ids["target_ids"]
            exact_links = _unit_link_rows(
                record,
                logp,
                source_index,
                target_index,
                approximate=False,
            )
            approximate_links = _unit_link_rows(
                record,
                logp,
                source_index,
                target_index,
                approximate=True,
            )
            if not exact_links or not approximate_links:
                raise numeric_error(
                    lang_pair=lang_pair,
                    message="alignment table is empty",
                    positions=[_first_position([record])],
                )
            total_links += len(exact_links)
            alignment_rows.append(
                {
                    "pair_id": record["pair_id"],
                    "lang_pair": lang_pair,
                    "exact": [
                        {
                            "source_pos": link["source_pos"],
                            "target_pos": link["target_pos"],
                            "log_align_prob": link["log_align_prob"],
                        }
                        for link in exact_links
                    ],
                    "approx": [
                        {
                            "source_pos": link["source_pos"],
                            "target_pos": link["target_pos"],
                            "log_align_prob": link["log_align_prob"],
                        }
                        for link in approximate_links
                    ],
                }
            )

    probabilities = {
        "schema_version": 1,
        "tier": tier,
        "smoothing": config.smoothing,
        "discard_threshold": config.discard_threshold,
        "em_iterations": config.em_iterations,
        "language_pairs": language_pairs,
    }
    summary = {
        "schema_version": 1,
        "tier": tier,
        "language_pairs": {
            key: {
                "sentence_pair_count": value["sentence_pair_count"],
                "source_vocab_size": value["source_vocab_size"],
                "target_vocab_size": value["target_vocab_size"],
                "observed_cooccurrence_cells": value["observed_cooccurrence_cells"],
                "pruned_transition_cells": value["pruned_transition_cells"],
                "transition_rows": len(value["log_transition"]),
            }
            for key, value in language_pairs.items()
        },
        "alignment_count": len(alignment_rows),
        "exact_link_total": total_links,
        "approx_link_total": sum(len(row["approx"]) for row in alignment_rows),
        "pruned_transition_cell_total": total_pruned,
        "valid_pair_count": len(records),
    }

    write_json_atomic(
        out_dir / "_meta.json",
        {
            "schema_version": 1,
            "tier": tier,
            "stage": "align",
            "stage_index": 2,
            "output_schema": ALIGNMENT_SCHEMA,
            "input_fingerprint": sha256_json(
                alignment_input_descriptor_from_records(
                    tier, config, split_artifact_digest
                )
            ),
        },
    )
    write_json_atomic(out_dir / "probabilities.json", probabilities)
    write_jsonl_atomic(out_dir / "alignments.jsonl", alignment_rows)
    write_json_atomic(out_dir / "summary.json", summary)
    return summary


def load_alignments(artifact: Path) -> dict[str, dict[str, Any]]:
    return {row["pair_id"]: row for row in iter_jsonl(artifact / "alignments.jsonl")}

