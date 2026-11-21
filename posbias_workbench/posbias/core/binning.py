"""Extrapolation binning and monotonicity diagnostics."""

from __future__ import annotations

import numpy as np

STAGE = "binning"


def distance_matrix(seq_len):
    idx = np.arange(seq_len, dtype=np.float64)
    return np.abs(idx[:, None] - idx[None, :])


def bin_masks(seq_len, train_length, bins):
    distance = distance_matrix(seq_len)
    ratio = distance / float(train_length)
    masks = []
    for bin_cfg in bins:
        low = float(bin_cfg["ratio_low"])
        high = float(bin_cfg["ratio_high"])
        masks.append((ratio >= low) & (ratio < high))
    return masks


def _adjacent_diff_mean(values, mask):
    """Mean |B[i,j]-B[i,j+1]| where both cells belong to the bin."""
    pair_mask = mask[:, :-1] & mask[:, 1:]
    if not np.any(pair_mask):
        return 0.0, 0
    diffs = np.abs(values[:, :-1] - values[:, 1:])[pair_mask]
    return float(np.mean(diffs)), int(diffs.size)


def _monotonic_violations(values, mask, distance):
    """Count adjacent distance-band mean increases inside the bin."""
    distances_in_bin = np.unique(distance[mask])
    if distances_in_bin.size < 2:
        return 0, []
    band_means = []
    for dist in distances_in_bin:
        band_mask = mask & (distance == dist)
        band_means.append(float(np.mean(values[band_mask])))
    band_means = np.asarray(band_means, dtype=np.float64)
    violations = int(np.count_nonzero(np.diff(band_means) > 0.0))
    return violations, [float(x) for x in band_means]


def bin_statistics(bias_float, *, train_length, bins):
    seq_len = bias_float.shape[0]
    distance = distance_matrix(seq_len)
    records = []
    for bin_cfg, mask in zip(bins, bin_masks(seq_len, train_length, bins)):
        record = {
            "bin_id": bin_cfg["id"],
            "ratio_low": float(bin_cfg["ratio_low"]),
            "ratio_high": float(bin_cfg["ratio_high"]),
            "cell_count": int(np.count_nonzero(mask)),
            "empty": bool(not np.any(mask)),
        }
        if record["empty"]:
            record.update({
                "mean_bias": None,
                "variance": None,
                "bias_peak": None,
                "diff_abs_mean": None,
                "diff_pair_count": 0,
                "monotonic_violations": 0,
                "distance_band_means": [],
                "monotonic_break": False,
            })
        else:
            values = bias_float[mask]
            mean_bias = float(np.mean(values))
            diff_mean, pair_count = _adjacent_diff_mean(bias_float, mask)
            violations, band_means = _monotonic_violations(
                bias_float, mask, distance)
            record.update({
                "mean_bias": mean_bias,
                "variance": float(np.var(values, ddof=0)),
                "bias_peak": float(np.max(np.abs(values))),
                "diff_abs_mean": diff_mean,
                "diff_pair_count": pair_count,
                "monotonic_violations": violations,
                "distance_band_means": band_means,
                "monotonic_break": violations > 0,
            })
        records.append(record)
    return records


def aggregate_shard_records(per_shard_records, bins):
    """Average numeric bin stats across shards; empty-bin records survive."""
    merged = []
    for idx, bin_cfg in enumerate(bins):
        slice_records = [records[idx] for records in per_shard_records]
        empties = [r for r in slice_records if r["empty"]]
        filled = [r for r in slice_records if not r["empty"]]
        base = {
            "bin_id": bin_cfg["id"],
            "ratio_low": float(bin_cfg["ratio_low"]),
            "ratio_high": float(bin_cfg["ratio_high"]),
            "shard_cell_counts": [r["cell_count"] for r in slice_records],
        }
        if not filled:
            base.update({
                "empty": True,
                "mean_bias": None,
                "variance": None,
                "bias_peak": None,
                "diff_abs_mean": None,
                "monotonic_violations": 0,
                "monotonic_break": False,
            })
        else:
            base.update({
                "empty": False,
                "mean_bias": float(np.mean([r["mean_bias"]
                                            for r in filled])),
                "variance": float(np.mean([r["variance"]
                                           for r in filled])),
                "bias_peak": float(np.max([r["bias_peak"]
                                           for r in filled])),
                "diff_abs_mean": float(np.mean([r["diff_abs_mean"]
                                                for r in filled])),
                "monotonic_violations": int(
                    np.sum([r["monotonic_violations"] for r in filled])),
                "monotonic_break": any(r["monotonic_break"]
                                       for r in filled),
            })
        merged.append(base)
    return merged


def change_rates(merged_bins):
    """Per-bin change rate of mean bias against the first non-empty bin."""
    anchor = next((b for b in merged_bins if not b["empty"]), None)
    rates = []
    for record in merged_bins:
        if record["empty"] or anchor is None:
            rates.append(None)
            continue
        anchor_mean = anchor["mean_bias"]
        denom = abs(anchor_mean) if anchor_mean != 0.0 else 1.0
        rates.append(float((record["mean_bias"] - anchor_mean) / denom))
    return rates
