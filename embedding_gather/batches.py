from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .digest import array_digest, json_scalar, load_npz, sha256_json
from .errors import E_BATCH_INVALID, E_LINK_MISMATCH, fail
from .model import BatchLink, IndexProject


@dataclass(frozen=True)
class IndexBatch:
    batch_id: str
    table_id: str
    batch_number: int
    ids: np.ndarray
    weights: np.ndarray | None
    gather_axis: int | None
    ids_digest: str
    weights_digest: str
    metadata_digest: str

    def validate_keys(self) -> None:
        shape = self.ids.shape
        if self.weights is not None:
            if self.weights.ndim > len(shape) or self.weights.shape != shape:
                raise fail(
                    E_BATCH_INVALID,
                    "per-element weights must have exactly the index array shape",
                    batch_id=self.batch_id,
                    ids_shape=list(shape),
                    weights_shape=list(self.weights.shape),
                )


def _scalar_int(value: Any, *, key: str, path: Path) -> int:
    scalar = json_scalar(value)
    if isinstance(scalar, bool):
        raise fail(E_BATCH_INVALID, f"{key} must be an integer", key=key, path=str(path))
    try:
        integer = int(scalar)
    except (TypeError, ValueError) as exc:
        raise fail(E_BATCH_INVALID, f"{key} must be an integer", key=key, path=str(path)) from exc
    if isinstance(scalar, float) and not scalar.is_integer():
        raise fail(E_BATCH_INVALID, f"{key} must be an integer", key=key, value=scalar, path=str(path))
    return integer


def load_index_batch(project: IndexProject, link: BatchLink, root: str | Path) -> IndexBatch:
    path = Path(link.file)
    if not path.is_absolute():
        path = Path(root) / path
    arrays = load_npz(path)
    if "ids" not in arrays:
        raise fail(E_BATCH_INVALID, "index batch has no ids array", batch_id=link.batch_id, path=str(path))
    ids = np.asarray(arrays["ids"])
    embedded_batch_id = str(json_scalar(arrays.get("batch_id", link.batch_id)))
    if embedded_batch_id != link.batch_id:
        raise fail(
            E_LINK_MISMATCH,
            "index batch id differs from manifest link",
            batch_id=link.batch_id,
            embedded_batch_id=embedded_batch_id,
        )
    table_id = str(json_scalar(arrays.get("table_id", project.table_id)))
    if table_id != project.table_id or link.table_id != table_id:
        raise fail(
            E_LINK_MISMATCH,
            "index batch is linked to a different embedding table",
            batch_id=link.batch_id,
            batch_table_id=table_id,
            manifest_table_id=project.table_id,
            link_table_id=link.table_id,
        )
    batch_number = _scalar_int(arrays.get("batch_number", link.batch_number), key="batch_number", path=path)
    if batch_number != link.batch_number:
        raise fail(
            E_LINK_MISMATCH,
            "index batch number differs from manifest link",
            batch_id=link.batch_id,
            manifest_batch_number=link.batch_number,
            embedded_batch_number=batch_number,
        )
    raw_axis = arrays.get("gather_axis")
    if raw_axis is None or json_scalar(raw_axis) is None:
        gather_axis = link.gather_axis
    else:
        gather_axis = _scalar_int(raw_axis, key="gather_axis", path=path)
    weights = None if "weights" not in arrays else np.asarray(arrays["weights"])
    ids_digest = array_digest(ids)
    weights_digest = "absent" if weights is None else array_digest(weights)
    metadata = {
        "batch_id": embedded_batch_id,
        "table_id": table_id,
        "batch_number": batch_number,
        "gather_axis": gather_axis,
    }
    return IndexBatch(
        batch_id=link.batch_id,
        table_id=table_id,
        batch_number=batch_number,
        ids=ids,
        weights=weights,
        gather_axis=gather_axis,
        ids_digest=ids_digest,
        weights_digest=weights_digest,
        metadata_digest=sha256_json(metadata),
    )
