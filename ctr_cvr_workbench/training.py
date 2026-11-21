"""Training-data materialization, deterministic traversal and score metrics."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np

from .config import Config
from .data import DataBundle
from .features import FeatureBundle, dense_matrix
from .model import ShardTrainData, auc_rank, log1mexp, traversal_order
from .scheduler import SchedulePlan
from .util import canonical_hash


@dataclass
class ScoreRecord:
    impression_id: str
    shard_id: str
    shard_index: int
    row_number: int
    log_ctr: float
    log_cvr: float
    log_conversion: float
    ctr_label: int
    cvr_label: int | None

    def score_dict(self, threshold_log: float) -> dict[str, Any]:
        return {
            "impression_id": self.impression_id,
            "shard_id": self.shard_id,
            "shard_index": self.shard_index,
            "row_number": self.row_number,
            "log_ctr": self.log_ctr,
            "log_cvr": self.log_cvr,
            "log_conversion": self.log_conversion,
            "ctr_probability": math.exp(self.log_ctr),
            "cvr_probability": math.exp(self.log_cvr),
            "conversion_probability": math.exp(self.log_conversion),
            "ctr_above_confidence_threshold": self.log_ctr >= threshold_log,
            "conversion_above_confidence_threshold": self.log_conversion >= threshold_log,
        }


def materialize_train_data(
    features: FeatureBundle,
    data: DataBundle,
    plan: SchedulePlan,
    config: Config,
) -> list[ShardTrainData]:
    feature_by_id = features.by_id()
    data_by_id = data.by_id()
    shard_data: list[ShardTrainData] = []
    for accepted in sorted(plan.accepted, key=lambda item: item.index):
        shard_features = feature_by_id[accepted.shard_id]
        loaded = data_by_id[accepted.shard_id]
        limit = min(accepted.accepted_rows, len(shard_features.rows), len(loaded.rows))
        rows = shard_features.rows[:limit]
        raw_rows = loaded.rows[:limit]
        matrix = dense_matrix(rows, config.feature_dimension)
        ctr_labels = np.asarray([int(raw_rows[index]["ctr_label"]) for index in range(limit)], dtype=np.float64)
        cvr_labels = np.zeros(limit, dtype=np.float64)
        cvr_observed = np.zeros(limit, dtype=bool)
        combo_targets = np.zeros(limit, dtype=np.float64)
        combo_mask = np.zeros(limit, dtype=bool)
        for index, row in enumerate(raw_rows):
            ctr = int(row["ctr_label"])
            cvr_value = row.get("cvr_label")
            observed = cvr_value is not None
            if observed:
                cvr_observed[index] = True
                cvr_labels[index] = float(cvr_value)
            if ctr == 0:
                combo_mask[index] = True
                combo_targets[index] = 0.0
            elif observed:
                combo_mask[index] = True
                combo_targets[index] = float(cvr_value)
        shard_data.append(
            ShardTrainData(
                shard_id=accepted.shard_id,
                shard_index=accepted.index,
                impression_ids=[row.impression_id for row in rows],
                row_numbers=[row.row_number for row in rows],
                features=matrix,
                ctr_labels=ctr_labels,
                cvr_labels=cvr_labels,
                cvr_observed=cvr_observed,
                combo_targets=combo_targets,
                combo_mask=combo_mask,
            )
        )
    return shard_data


def initial_rates(shard_data: list[ShardTrainData]) -> tuple[float, float]:
    rows = sum(len(shard.ctr_labels) for shard in shard_data)
    ctr_positive = sum(int(shard.ctr_labels.sum()) for shard in shard_data)
    exposed_observed = 0
    conversion = 0
    for shard in shard_data:
        mask = (shard.ctr_labels > 0.5) & shard.cvr_observed
        exposed_observed += int(mask.sum())
        conversion += int(shard.cvr_labels[mask].sum())
    return (
        ctr_positive / rows if rows else 0.0,
        conversion / exposed_observed if exposed_observed else 0.0,
    )


def epoch_orders(
    shard_data: list[ShardTrainData],
    seed: int,
    epoch: int,
) -> tuple[dict[str, list[int]], dict[str, Any]]:
    orders: dict[str, list[int]] = {}
    traversal_shards: list[dict[str, Any]] = []
    global_ids: list[str] = []
    for shard in sorted(shard_data, key=lambda item: item.shard_index):
        order = traversal_order(shard.shard_id, shard.impression_ids, seed, epoch)
        orders[shard.shard_id] = order
        ids = [shard.impression_ids[position] for position in order]
        global_ids.extend(ids)
        traversal_shards.append(
            {
                "shard_id": shard.shard_id,
                "shard_index": shard.shard_index,
                "impression_order": ids,
            }
        )
    traversal = {
        "epoch": epoch,
        "shards": traversal_shards,
        "global_impression_order": global_ids,
        "digest": canonical_hash(
            {
                "epoch": epoch,
                "shards": [
                    {
                        "shard_id": item["shard_id"],
                        "shard_index": item["shard_index"],
                        "impression_order": item["impression_order"],
                    }
                    for item in traversal_shards
                ],
            }
        ),
    }
    return orders, traversal


def score_records(
    shard_data: list[ShardTrainData],
    successes: list[dict[str, Any]],
) -> list[ScoreRecord]:
    outcomes = {item["shard_id"]: item for item in successes}
    records: list[ScoreRecord] = []
    for shard in sorted(shard_data, key=lambda item: item.shard_index):
        outcome = outcomes[shard.shard_id]
        log_ctr = np.asarray(outcome["log_ctr"], dtype=np.float64)
        log_cvr = np.asarray(outcome["log_cvr"], dtype=np.float64)
        for index, impression_id in enumerate(shard.impression_ids):
            ctr_value = int(shard.ctr_labels[index])
            cvr_value = int(shard.cvr_labels[index]) if shard.cvr_observed[index] else None
            records.append(
                ScoreRecord(
                    impression_id=impression_id,
                    shard_id=shard.shard_id,
                    shard_index=shard.shard_index,
                    row_number=shard.row_numbers[index],
                    log_ctr=float(log_ctr[index]),
                    log_cvr=float(log_cvr[index]),
                    log_conversion=float(log_ctr[index] + log_cvr[index]),
                    ctr_label=ctr_value,
                    cvr_label=cvr_value,
                )
            )
    return records


def score_digest(records: list[ScoreRecord]) -> str:
    return canonical_hash(
        [
            {
                "impression_id": record.impression_id,
                "log_ctr": record.log_ctr,
                "log_cvr": record.log_cvr,
                "log_conversion": record.log_conversion,
            }
            for record in records
        ]
    )


def objective_metrics(records: list[ScoreRecord], threshold: float) -> dict[str, Any]:
    threshold_log = math.log(threshold)
    ctr_loss_values: list[float] = []
    cvr_loss_values: list[float] = []
    combo_loss_values: list[float] = []
    ctr_scores: list[float] = []
    ctr_labels: list[int] = []
    cvr_scores: list[float] = []
    cvr_labels: list[int] = []
    predicted_ctr = 0
    predicted_conversion = 0
    conversion_positive = 0
    exposure_count = 0
    for record in records:
        ctr_array = np.asarray([record.log_ctr], dtype=np.float64)
        ctr_loss_values.append(
            float(-(record.ctr_label * record.log_ctr + (1 - record.ctr_label) * float(log1mexp(ctr_array)[0])))
        )
        ctr_scores.append(record.log_ctr)
        ctr_labels.append(record.ctr_label)
        exposure_count += record.ctr_label
        if record.ctr_label:
            if record.cvr_label is not None:
                cvr_array = np.asarray([record.log_cvr], dtype=np.float64)
                cvr_loss_values.append(
                    float(
                        -(
                            record.cvr_label * record.log_cvr
                            + (1 - record.cvr_label) * float(log1mexp(cvr_array)[0])
                        )
                    )
                )
                cvr_scores.append(record.log_cvr)
                cvr_labels.append(int(record.cvr_label))
                conversion_positive += int(record.cvr_label)
                combo_target = 1 if record.cvr_label == 1 else 0
                combo_array = np.asarray([record.log_conversion], dtype=np.float64)
                combo_loss_values.append(
                    float(
                        -(
                            combo_target * record.log_conversion
                            + (1 - combo_target) * float(log1mexp(combo_array)[0])
                        )
                    )
                )
        else:
            combo_array = np.asarray([record.log_conversion], dtype=np.float64)
            combo_loss_values.append(float(-float(log1mexp(combo_array)[0])))
        if record.log_ctr >= threshold_log:
            predicted_ctr += 1
        if record.log_conversion >= threshold_log:
            predicted_conversion += 1
    return {
        "rows": len(records),
        "exposure_count": exposure_count,
        "conversion_positive_count": conversion_positive,
        "ctr_logloss": sum(ctr_loss_values) / len(ctr_loss_values) if ctr_loss_values else None,
        "cvr_logloss": sum(cvr_loss_values) / len(cvr_loss_values) if cvr_loss_values else None,
        "combo_logloss": sum(combo_loss_values) / len(combo_loss_values) if combo_loss_values else None,
        "ctr_auc": auc_rank(ctr_scores, ctr_labels),
        "cvr_auc": auc_rank(cvr_scores, cvr_labels),
        "predicted_ctr_rate_at_threshold": predicted_ctr / len(records) if records else 0.0,
        "predicted_conversion_rate_at_threshold": predicted_conversion / len(records) if records else 0.0,
        "observed_conversion_rate_on_exposure": conversion_positive / exposure_count if exposure_count else 0.0,
        "confidence_threshold": threshold,
        "confidence_threshold_log": threshold_log,
    }
