"""Order-independent fixed-point prediction accumulator."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

import numpy as np

from .calibrators import Calibrator, apply_calibrator
from .config import Config
from .errors import WorkbenchError
from .precision import (
    dequantize,
    int64_digest,
    probability_terms,
    quantize_probabilities,
    quantize_raw_scores,
    scale_for_decimals,
)
from .util import sha256_json


class PredictionAccumulator:
    """A transactional, order-independent fixed-point reduction state.

    A rejected batch never mutates any field, including the batch and sample
    counters. Finalization sorts by explicit sample id, so neither batch size
    nor batch arrival order can affect the probability vector or metrics.
    """

    def __init__(self, config: Config, calibrator: Calibrator):
        self.config = config.validate()
        self.calibrator = calibrator
        self.scale = scale_for_decimals(config.quantize_decimals)
        self.batch_count = 0
        self.sample_count = 0
        self._seen: set[int] = set()
        self._raw_q: dict[int, int] = {}
        self._probability_q: dict[int, int] = {}
        self._loss_q: dict[int, int] = {}
        self._brier_q: dict[int, int] = {}
        self._labels: dict[int, int] = {}

    def add_raw_batch(
        self,
        raw_scores: np.ndarray,
        labels: np.ndarray,
        sample_ids: Iterable[int],
        *,
        batch_size: int | None = None,
        stage: str = "batch_prediction",
    ) -> None:
        raw_scores = np.asarray(raw_scores, dtype=np.float64)
        labels = np.asarray(labels)
        try:
            ids = [int(value) for value in sample_ids]
        except (TypeError, ValueError) as exc:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "sample ids must be integer-valued",
                {"preserved_state": self.summary()},
            ) from exc
        effective_size = len(ids) if batch_size is None else batch_size
        if not isinstance(effective_size, int) or effective_size <= 0:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "batch size must be a positive integer",
                {"batch_size": batch_size, "preserved_state": self.summary()},
            )
        if effective_size != raw_scores.shape[0] or len(ids) != raw_scores.shape[0]:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "batch size, sample ids and prediction vectors must align",
                {
                    "batch_size": effective_size,
                    "raw_scores": int(raw_scores.shape[0]),
                    "sample_ids": len(ids),
                    "preserved_state": self.summary(),
                },
            )
        if raw_scores.shape[0] == 0:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "empty batches are not accepted",
                {"preserved_state": self.summary()},
            )
        if labels.shape != raw_scores.shape:
            raise WorkbenchError(
                "E_INVALID_PARAM",
                "labels must align with raw scores",
                {"raw_scores": list(raw_scores.shape), "labels": list(labels.shape)},
            )
        if not np.all((labels == 0) | (labels == 1)):
            raise WorkbenchError("E_INVALID_PARAM", "labels must be binary", {"stage": stage})
        id_counts = Counter(ids)
        duplicate_ids = sorted(value for value, count in id_counts.items() if count > 1 or value in self._seen)
        if duplicate_ids:
            raise WorkbenchError(
                "E_BATCH_STATE",
                "batch samples have already been reduced or are duplicated within the batch",
                {
                    "duplicate_sample_ids": duplicate_ids,
                    "preserved_state": self.summary(),
                },
            )
        try:
            raw_q = quantize_raw_scores(raw_scores, self.config.quantize_decimals, stage=stage)
        except WorkbenchError as exc:
            if exc.code == "E_NUMERIC" and ids:
                exc.details.setdefault("sample_id", ids[int(exc.details.get("sample_index", 0))])
            raise
        reference_scores = dequantize(raw_q, self.config.quantize_decimals)
        try:
            probability = apply_calibrator(self.calibrator, reference_scores, stage=stage)
        except WorkbenchError as exc:
            if exc.code == "E_NUMERIC" and ids:
                exc.details.setdefault("sample_id", ids[int(exc.details.get("sample_index", 0))])
            raise
        probability_q = quantize_probabilities(probability, self.config.quantize_decimals, stage=stage)
        loss_q, brier_q = probability_terms(
            probability_q,
            labels,
            self.config.quantize_decimals,
            stage=stage,
        )

        # Mutation starts only after every validation and numerical conversion succeeds.
        for position, sample_id in enumerate(ids):
            self._seen.add(sample_id)
            self._raw_q[sample_id] = int(raw_q[position])
            self._probability_q[sample_id] = int(probability_q[position])
            self._loss_q[sample_id] = int(loss_q[position])
            self._brier_q[sample_id] = int(brier_q[position])
            self._labels[sample_id] = int(labels[position])
        self.batch_count += 1
        self.sample_count += len(ids)

    def summary(self) -> dict[str, Any]:
        ids = sorted(self._raw_q)
        digest_rows = [
            [sample_id, self._raw_q[sample_id], self._probability_q[sample_id], self._labels[sample_id]]
            for sample_id in ids
        ]
        return {
            "batches": int(self.batch_count),
            "samples": int(self.sample_count),
            "raw_sum_q": int(sum(self._raw_q.values())),
            "probability_sum_q": int(sum(self._probability_q.values())),
            "loss_sum_q": int(sum(self._loss_q.values())),
            "brier_sum_q": int(sum(self._brier_q.values())),
            "state_digest": sha256_json(digest_rows),
        }

    def finalize(self) -> dict[str, Any]:
        ids = sorted(self._raw_q)
        if not ids:
            raise WorkbenchError("E_BATCH_STATE", "no samples have been reduced", {"preserved_state": self.summary()})
        raw_q = np.asarray([self._raw_q[sample_id] for sample_id in ids], dtype=np.int64)
        probability_q = np.asarray([self._probability_q[sample_id] for sample_id in ids], dtype=np.int64)
        loss_q = np.asarray([self._loss_q[sample_id] for sample_id in ids], dtype=np.int64)
        brier_q = np.asarray([self._brier_q[sample_id] for sample_id in ids], dtype=np.int64)
        labels = np.asarray([self._labels[sample_id] for sample_id in ids], dtype=np.uint8)
        total_loss_q = int(sum(int(value) for value in loss_q))
        total_brier_q = int(sum(int(value) for value in brier_q))
        scale = float(self.scale)
        sample_count = len(ids)
        core = {
            "scale": self.scale,
            "sample_ids": ids,
            "raw_scores_q": [int(value) for value in raw_q],
            "probabilities_q": [int(value) for value in probability_q],
            "labels": [int(value) for value in labels],
            "loss_terms_q": [int(value) for value in loss_q],
            "brier_terms_q": [int(value) for value in brier_q],
            "log_loss_q": total_loss_q,
            "brier_q": total_brier_q,
            "total_log_loss": total_loss_q / scale,
            "total_brier": total_brier_q / scale,
            "log_loss": total_loss_q / (scale * sample_count),
            "brier": total_brier_q / (scale * sample_count),
            "raw_vector_digest": int64_digest(raw_q),
            "probability_vector_digest": int64_digest(probability_q),
        }
        return {
            **core,
            "result_digest": sha256_json(core),
            "accumulator": self.summary(),
            "probability_values": [value / scale for value in probability_q.tolist()],
            "raw_score_values": [value / scale for value in raw_q.tolist()],
        }


def make_batches(rows: int, batch_size: int, *, reverse: bool = False) -> list[np.ndarray]:
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise WorkbenchError("E_BATCH_STATE", "batch size must be a positive integer", {"batch_size": batch_size})
    if rows < 0:
        raise WorkbenchError("E_INVALID_PARAM", "row count must be non-negative", {"rows": rows})
    batches = [np.arange(start, min(start + batch_size, rows), dtype=np.int64) for start in range(0, rows, batch_size)]
    if reverse:
        batches.reverse()
    return batches


def comparable_core(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "scale": result["scale"],
        "sample_ids": result["sample_ids"],
        "raw_scores_q": result["raw_scores_q"],
        "probabilities_q": result["probabilities_q"],
        "labels": result["labels"],
        "loss_terms_q": result["loss_terms_q"],
        "brier_terms_q": result["brier_terms_q"],
        "log_loss_q": result["log_loss_q"],
        "brier_q": result["brier_q"],
        "total_log_loss": result["total_log_loss"],
        "total_brier": result["total_brier"],
        "log_loss": result["log_loss"],
        "brier": result["brier"],
        "raw_vector_digest": result["raw_vector_digest"],
        "probability_vector_digest": result["probability_vector_digest"],
    }


def comparable_digest(result: dict[str, Any]) -> str:
    return sha256_json(comparable_core(result))
