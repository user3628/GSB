"""Validated, non-polluting batch ingestion for current-distribution samples."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

import numpy as np

from .errors import WorkbenchError


@dataclass
class Batch:
    features: np.ndarray
    row_indices: np.ndarray | None = None
    source_labels: np.ndarray | None = None


@dataclass
class BatchAccumulator:
    """Collect valid batches; rejected batches never mutate accepted state."""

    expected_columns: int
    column_names: Sequence[str]
    expected_rows: int | None = None
    batches: list[Batch] = field(default_factory=list)
    accepted_rows: int = 0
    rejected_batches: list[int] = field(default_factory=list)

    def _invalid(self, batch_index: int, location: str, message: str, **details: object) -> WorkbenchError:
        return WorkbenchError(
            "E_INVALID_BATCH",
            message,
            stage="batch_ingest",
            location=location,
            details={"batch_index": batch_index, "position": location, **details},
        )

    def add_batch(
        self,
        features: np.ndarray,
        *,
        row_indices: np.ndarray | None = None,
        source_labels: np.ndarray | None = None,
    ) -> int:
        batch_index = len(self.batches) + len(self.rejected_batches)
        location = f"batch[{batch_index}].features"
        try:
            matrix = np.asarray(features, dtype=np.float64)
        except (TypeError, ValueError) as exc:
            self.rejected_batches.append(batch_index)
            raise self._invalid(batch_index, location, "features must be numeric", reason=str(exc))
        if matrix.ndim != 2:
            self.rejected_batches.append(batch_index)
            raise self._invalid(batch_index, location, "features must be a two-dimensional matrix")
        if matrix.shape[0] == 0:
            self.rejected_batches.append(batch_index)
            raise self._invalid(batch_index, location, "empty batches are not accepted", shape=list(matrix.shape))
        if matrix.shape[1] != self.expected_columns:
            self.rejected_batches.append(batch_index)
            raise self._invalid(
                batch_index,
                f"batch[{batch_index}].features.shape[1]",
                "batch feature dimension does not match the reference",
                expected_columns=self.expected_columns,
                actual_columns=int(matrix.shape[1]),
            )
        if np.isinf(matrix).any():
            self.rejected_batches.append(batch_index)
            raise self._invalid(
                batch_index,
                f"batch[{batch_index}].features",
                "infinite feature value in batch",
            )
        if self.expected_rows is not None and self.accepted_rows + matrix.shape[0] > self.expected_rows:
            self.rejected_batches.append(batch_index)
            raise self._invalid(
                batch_index,
                f"batch[{batch_index}].features.shape[0]",
                "batches contain more rows than declared",
                expected_rows=self.expected_rows,
                accepted_rows=self.accepted_rows,
                batch_rows=int(matrix.shape[0]),
            )

        index_array: np.ndarray | None = None
        if row_indices is not None:
            index_array = np.asarray(row_indices, dtype=np.int64)
            if index_array.ndim != 1 or index_array.shape[0] != matrix.shape[0]:
                self.rejected_batches.append(batch_index)
                raise self._invalid(
                    batch_index,
                    f"batch[{batch_index}].row_indices",
                    "row_indices must be one-dimensional and match the batch row count",
                )

        label_array: np.ndarray | None = None
        if source_labels is not None:
            label_array = np.asarray(source_labels)
            if label_array.ndim != 1 or label_array.shape[0] != matrix.shape[0]:
                self.rejected_batches.append(batch_index)
                raise self._invalid(
                    batch_index,
                    f"batch[{batch_index}].source_labels",
                    "source_labels must match the batch row count",
                )

        accepted = Batch(
            features=np.ascontiguousarray(matrix, dtype=np.float64),
            row_indices=index_array,
            source_labels=label_array,
        )
        self.batches.append(accepted)
        self.accepted_rows += int(matrix.shape[0])
        return batch_index

    def finalize(self) -> np.ndarray:
        if not self.batches:
            raise WorkbenchError(
                "E_INVALID_BATCH",
                "no valid batch has been accepted",
                stage="batch_ingest",
                location="batch_ingest.finalize",
                details={"accepted_rows": 0},
            )
        if self.expected_rows is not None and self.accepted_rows != self.expected_rows:
            raise WorkbenchError(
                "E_INVALID_BATCH",
                "accepted rows do not match the declared row count",
                stage="batch_ingest",
                location="batch_ingest.finalize",
                details={"expected_rows": self.expected_rows, "accepted_rows": self.accepted_rows},
            )
        merged = np.concatenate([batch.features for batch in self.batches], axis=0)
        return np.ascontiguousarray(merged, dtype=np.float64)

    def summary(self) -> dict[str, object]:
        return {
            "accepted_batches": len(self.batches),
            "rejected_batches": len(self.rejected_batches),
            "accepted_rows": self.accepted_rows,
            "batch_rows": [int(batch.features.shape[0]) for batch in self.batches],
        }


def split_into_batches(
    features: np.ndarray,
    *,
    batch_size: int | None,
    order: str = "forward",
) -> list[np.ndarray]:
    matrix = np.asarray(features, dtype=np.float64)
    if matrix.ndim != 2:
        raise WorkbenchError("E_INVALID_BATCH", "features must be a matrix", stage="batch_ingest")
    if order not in {"forward", "reverse"}:
        raise WorkbenchError(
            "E_INVALID_BATCH",
            "batch order must be forward or reverse",
            stage="batch_ingest",
            location="batch_order",
        )
    if batch_size is None or batch_size <= 0 or batch_size >= matrix.shape[0]:
        batches = [matrix]
    else:
        batches = [matrix[start : start + batch_size] for start in range(0, matrix.shape[0], batch_size)]
    if order == "reverse":
        batches = list(reversed(batches))
    return batches


def ingest_batches(
    batches: Iterable[np.ndarray | Batch],
    *,
    expected_columns: int,
    column_names: Sequence[str],
    expected_rows: int | None = None,
) -> tuple[np.ndarray, dict[str, object]]:
    accumulator = BatchAccumulator(
        expected_columns=expected_columns,
        column_names=column_names,
        expected_rows=expected_rows,
    )
    for item in batches:
        if isinstance(item, Batch):
            accumulator.add_batch(
                item.features,
                row_indices=item.row_indices,
                source_labels=item.source_labels,
            )
        else:
            accumulator.add_batch(item)
    matrix = accumulator.finalize()
    return matrix, accumulator.summary()
