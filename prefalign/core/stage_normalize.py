"""Stage 1: normalization and prompt grouping."""
from __future__ import annotations

from collections import defaultdict

from .errors import InvalidSampleError
from .hashing import canonical_json, normalize_text, simhash64, stable_hash64, tokenize
from .model import Sample

STAGE = "normalize"


def normalize_sample(sample: Sample) -> dict:
    norm_prompt = normalize_text(sample.prompt)
    norm_chosen = normalize_text(sample.chosen)
    norm_rejected = normalize_text(sample.rejected)
    if not norm_prompt or not (norm_prompt or norm_chosen or norm_rejected):
        raise InvalidSampleError(
            "empty prompt or all of prompt/chosen/rejected normalized to empty",
            details={"pair_id": sample.pair_id, "shard": sample.shard},
        )
    fingerprint = f"{simhash64(tokenize(norm_prompt)):016x}"
    return {
        "pair_id": sample.pair_id,
        "source": sample.source,
        "lang": sample.lang,
        "shard": sample.shard,
        "consistency": sample.consistency,
        "norm_prompt": norm_prompt,
        "norm_chosen": norm_chosen,
        "norm_rejected": norm_rejected,
        "prompt_fingerprint": fingerprint,
    }


def run(samples: list[Sample]) -> dict:
    diagnostics: list[dict] = []
    records: list[dict] = []
    for sample in samples:
        try:
            records.append(normalize_sample(sample))
        except InvalidSampleError as exc:
            diagnostics.append({
                "code": exc.code,
                "pair_id": exc.details.get("pair_id"),
                "shard": exc.details.get("shard"),
                "message": str(exc),
            })

    # Group by exact normalized prompt; SimHash is the displayed fingerprint and
    # the group key seed, with a disambiguator for (astronomically unlikely)
    # hash collisions so distinct prompts never merge.
    grouped: dict[str, list[dict]] = defaultdict(list)
    for record in records:
        grouped[record["norm_prompt"]].append(record)

    group_index: dict[str, str] = {}
    output_records: list[dict] = []
    group_summaries: list[dict] = []
    for norm_prompt in sorted(grouped):
        members = sorted(grouped[norm_prompt], key=lambda item: item["pair_id"])
        fingerprint = members[0]["prompt_fingerprint"]
        if fingerprint in group_index.values():
            # Collision disambiguation, deterministic on prompt content.
            fingerprint = stable_hash64("collision:" + norm_prompt)
        group_id = f"grp_{fingerprint}"
        group_index[norm_prompt] = group_id
        for ordinal, record in enumerate(members):
            output_records.append({
                **record,
                "group_id": group_id,
                "pair_ordinal": ordinal,
            })
        group_summaries.append({
            "group_id": group_id,
            "prompt_fingerprint": fingerprint,
            "size": len(members),
            "pair_ids": [item["pair_id"] for item in members],
        })

    output_records.sort(key=lambda item: item["pair_id"])
    group_summaries.sort(key=lambda item: item["group_id"])
    diagnostics.sort(key=lambda item: (item["pair_id"] or "", item["code"]))
    return {
        "stage": STAGE,
        "records": output_records,
        "groups": group_summaries,
        "diagnostics": diagnostics,
        "stats": {
            "input_samples": len(samples),
            "valid_samples": len(output_records),
            "invalid_samples": len(diagnostics),
            "groups": len(group_summaries),
        },
    }


def artifact_digest(result: dict) -> str:
    payload = {
        "records": [
            {key: record[key] for key in (
                "pair_id", "group_id", "pair_ordinal", "prompt_fingerprint",
                "norm_prompt", "norm_chosen", "norm_rejected", "source", "lang")}
            for record in result["records"]
        ],
        "groups": result["groups"],
        "diagnostics": result["diagnostics"],
    }
    return stable_hash64(canonical_json(payload))
