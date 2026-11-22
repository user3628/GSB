"""Deterministic, two-dimensional chunked row-wise TopK kernel."""

from __future__ import annotations

from typing import Any, Callable

import numpy as np

from .errors import E_INVALID_K, E_NUMERIC, PipelineError
from .util import finite_or_negative_inf


StopCheck = Callable[[], bool]


class StopRequested(Exception):
    """Raised only at a row-block boundary after a graceful-stop request."""

    def __init__(self, *, shard_id: str, completed_rows: int, stage: str = "filter"):
        super().__init__(f"stop requested after {completed_rows} rows of {shard_id}")
        self.shard_id = shard_id
        self.completed_rows = completed_rows
        self.stage = stage


def _validate_k(k: Any, vocab_size: int, shard_id: str | None) -> int:
    valid = isinstance(k, (int, np.integer)) and not isinstance(k, bool)
    k_int = int(k) if valid else k
    if not valid or k_int <= 0 or k_int > vocab_size:
        raise PipelineError(
            code=E_INVALID_K,
            message=f"K must be in [1, {vocab_size}], received {k!r}",
            stage="filter",
            shard_id=shard_id,
            k=k_int if isinstance(k_int, int) else None,
            details={"vocab_size": vocab_size},
        )
    return k_int


def chunked_topk(
    logits: np.ndarray,
    *,
    k: int,
    shard_id: str,
    row_block: int = 8,
    col_block: int = 256,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    """Return the per-row TopK candidate set.

    Scanning order is row block -> column block -> (score descending, token id
    ascending).  ``admission_turn`` is the deterministic block-level admission
    sequence at which a candidate first entered the current TopK set; final
    ``rank``/``comparison_order`` is the unique ranking after the scan.
    """

    if not isinstance(logits, np.ndarray):
        logits = np.asarray(logits)
    if logits.ndim != 2:
        raise PipelineError(
            code=E_NUMERIC,
            message="logits must be a two-dimensional matrix",
            stage="filter",
            shard_id=shard_id,
            details={"shape": list(logits.shape)},
        )
    rows, vocab_size = logits.shape
    k = _validate_k(k, vocab_size, shard_id)
    if isinstance(row_block, bool) or not isinstance(row_block, (int, np.integer)) or row_block <= 0:
        raise PipelineError(E_NUMERIC, "row_block must be positive", "filter", shard_id)
    if isinstance(col_block, bool) or not isinstance(col_block, (int, np.integer)) or col_block <= 0:
        raise PipelineError(E_NUMERIC, "col_block must be positive", "filter", shard_id)

    row_block = int(row_block)
    col_block = int(col_block)
    row_results: list[dict[str, Any]] = []

    for row_start in range(0, rows, row_block):
        row_end = min(rows, row_start + row_block)
        if stop_check is not None and stop_check():
            raise StopRequested(shard_id=shard_id, completed_rows=row_start)

        for row in range(row_start, row_end):
            best: list[dict[str, Any]] = []
            admission_turn = 0
            blocks_scanned = 0
            for col_start in range(0, vocab_size, col_block):
                col_end = min(vocab_size, col_start + col_block)
                block = np.asarray(logits[row, col_start:col_end], dtype=np.float64)
                bad_mask = np.isnan(block) | np.isposinf(block)
                if bool(np.any(bad_mask)):
                    bad_offset = int(np.flatnonzero(bad_mask)[0])
                    raise PipelineError(
                        code=E_NUMERIC,
                        message="NaN or +Inf logit encountered during filtering",
                        stage="filter",
                        shard_id=shard_id,
                        row=row,
                        details={
                            "column": col_start + bad_offset,
                            "column_block": col_start // col_block,
                            "value": "NaN" if np.isnan(block[bad_offset]) else "+Infinity",
                        },
                    )

                block_ids = np.arange(col_start, col_end, dtype=np.int64)
                # lexsort's last key is primary: score descending, then token ascending.
                order = np.lexsort((block_ids, -block))
                selected = order[:k]
                block_candidates: list[dict[str, Any]] = []
                for offset in selected:
                    admission_turn += 1
                    token = int(col_start + int(offset))
                    block_candidates.append(
                        {
                            "token": token,
                            "score": finite_or_negative_inf(float(block[int(offset)])),
                            "source_column_block": col_start // col_block,
                            "admission_turn": admission_turn,
                        }
                    )

                merged = best + block_candidates
                merged.sort(key=lambda candidate: (-float(candidate["score"]), candidate["token"]))
                best = merged[:k]
                blocks_scanned += 1

            for rank, candidate in enumerate(best, start=1):
                candidate["rank"] = rank
                candidate["comparison_order"] = rank
            row_results.append(
                {
                    "row": row,
                    "candidate_count": len(best),
                    "compared_elements": vocab_size,
                    "column_blocks_scanned": blocks_scanned,
                    "candidates": best,
                }
            )

    return {
        "shard_id": shard_id,
        "stage": "filter",
        "shape": [rows, k],
        "scan_order": "row_block,col_block,score_desc,token_asc",
        "tie_break": "higher score first; equal scores by lower token id",
        "row_block": row_block,
        "col_block": col_block,
        "compared_elements": rows * vocab_size,
        "rows": row_results,
    }
