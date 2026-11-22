"""Endpoint alignment against manifest reference word timings."""

from .vocabulary import is_mixed_script

UNVERIFIED = "UNVERIFIED"
PASS = "PASS"
MISMATCH = "MISMATCH"


def align_shard(decode_result, manifest_entry, align_config):
    """Index-based alignment; missing references produce an explicit UNVERIFIED."""
    references = manifest_entry.get("reference_words") or []
    decoded = decode_result["words"]
    if not references:
        return {
            "status": UNVERIFIED,
            "shard_id": manifest_entry["shard_id"],
            "reason": "missing reference annotations",
            "hits": 0,
            "out_of_bounds": [],
            "mixed_script_words": [],
        }
    out_of_bounds = []
    hits = 0
    deviations = []
    mixed_script_words = [
        word["word"] for word in decoded if is_mixed_script(word["word"])
    ]
    for index, reference in enumerate(references):
        if index >= len(decoded):
            out_of_bounds.append(
                {
                    "shard_id": manifest_entry["shard_id"],
                    "word_index": index,
                    "reference_word": reference["word"],
                    "decoded_word": None,
                    "start_delta_ms": None,
                    "end_delta_ms": None,
                }
            )
            continue
        decoded_word = decoded[index]
        start_delta = decoded_word["start_ms"] - reference["start_ms"]
        end_delta = decoded_word["end_ms"] - reference["end_ms"]
        text_match = decoded_word["word"] == reference["word"]
        within_tolerance = (
            abs(start_delta) <= align_config.tolerance_ms
            and abs(end_delta) <= align_config.tolerance_ms
        )
        if text_match and within_tolerance:
            hits += 1
        else:
            out_of_bounds.append(
                {
                    "shard_id": manifest_entry["shard_id"],
                    "word_index": index,
                    "reference_word": reference["word"],
                    "decoded_word": decoded_word["word"],
                    "start_delta_ms": int(start_delta),
                    "end_delta_ms": int(end_delta),
                }
            )
        deviations.extend([abs(start_delta), abs(end_delta)])
    for extra_index in range(len(references), len(decoded)):
        out_of_bounds.append(
            {
                "shard_id": manifest_entry["shard_id"],
                "word_index": extra_index,
                "reference_word": None,
                "decoded_word": decoded[extra_index]["word"],
                "start_delta_ms": None,
                "end_delta_ms": None,
            }
        )
    mean_deviation = round(sum(deviations) / len(deviations), 3) if deviations else None
    max_deviation = max(deviations) if deviations else None
    status = PASS if not out_of_bounds else MISMATCH
    return {
        "status": status,
        "shard_id": manifest_entry["shard_id"],
        "tolerance_ms": align_config.tolerance_ms,
        "reference_count": len(references),
        "decoded_count": len(decoded),
        "hits": hits,
        "mean_abs_deviation_ms": mean_deviation,
        "max_abs_deviation_ms": max_deviation,
        "out_of_bounds": out_of_bounds,
        "mixed_script_words": mixed_script_words,
        "mixed_script_count": len(mixed_script_words),
    }
