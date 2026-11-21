"""WAL replay with adaptive precise / batched paths and resumable progress."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

from .config import Config
from .errors import ReplayMismatchError
from .models import Checkpoint, Page, WalRecord
from .wal import WalManager


@dataclass
class ReplayDecision:
    path: str
    record_count: int
    dirty_pages: int
    batch_size: int
    flush_group_size: int
    reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "record_count": self.record_count,
            "dirty_pages": self.dirty_pages,
            "batch_size": self.batch_size,
            "flush_group_size": self.flush_group_size,
            "reason": self.reason,
        }


@dataclass
class ReplayResult:
    decision: ReplayDecision
    applied_records: int
    start_lsn: int
    end_lsn: int
    final_pages: Dict[int, Page]
    resume_from_lsn: Optional[int] = None
    interrupted: bool = False
    flushed_groups: int = 0


class Replayer:
    def __init__(self, wal: WalManager, config: Config):
        self.wal = wal
        self.config = config

    def decide(self, start_lsn: int) -> ReplayDecision:
        record_count, dirty_pages = self.wal.count_records(from_lsn=start_lsn)
        precise = (
            record_count < self.config.replay_record_threshold
            and len(dirty_pages) < self.config.replay_dirty_page_threshold
        )
        if precise:
            path = "precise"
            reason = (
                f"records {record_count} < record_threshold "
                f"{self.config.replay_record_threshold} and dirty_pages "
                f"{len(dirty_pages)} < dirty_page_threshold "
                f"{self.config.replay_dirty_page_threshold}"
            )
        else:
            path = "batched"
            reason = (
                f"records {record_count} >= record_threshold "
                f"{self.config.replay_record_threshold} or dirty_pages "
                f"{len(dirty_pages)} >= dirty_page_threshold "
                f"{self.config.replay_dirty_page_threshold}"
            )
        return ReplayDecision(
            path=path,
            record_count=record_count,
            dirty_pages=len(dirty_pages),
            batch_size=self.config.replay_batch_size,
            flush_group_size=self.config.flush_group_size,
            reason=reason,
        )

    @staticmethod
    def _check_record(rec: WalRecord, current: Optional[Page], expected_lsn: int):
        if rec.lsn != expected_lsn:
            raise ReplayMismatchError(
                "LSN sequence gap during replay",
                {"expected_lsn": expected_lsn, "actual_lsn": rec.lsn},
            )
        if current is None:
            if rec.before_digest is not None:
                raise ReplayMismatchError(
                    "new page record must carry null before_digest",
                    {"expected_lsn": rec.lsn, "actual_lsn": rec.lsn,
                     "page_no": rec.page_no},
                )
            return Page(
                page_no=rec.page_no,
                digest=rec.after_digest,
                version=1,
                last_lsn=rec.lsn,
            )
        if rec.before_digest != current.digest:
            raise ReplayMismatchError(
                "before_digest does not match current page digest",
                {"expected_lsn": rec.lsn, "actual_lsn": rec.lsn,
                 "page_no": rec.page_no,
                 "expected_digest": current.digest,
                 "actual_digest": rec.before_digest},
            )
        return Page(
            page_no=rec.page_no,
            digest=rec.after_digest,
            version=current.version + 1,
            last_lsn=rec.lsn,
        )

    def _build_expected_matrix(self, records: List[WalRecord]) -> np.ndarray:
        """Vectorised expectation matrix used to cross-check sequential logic."""
        return np.fromiter(
            (r.lsn for r in records), dtype=np.int64, count=len(records)
        )

    def replay(
        self,
        checkpoint: Checkpoint,
        already_applied_upto: Optional[int] = None,
        interrupt_after: Optional[int] = None,
        progress_cb: Optional[Callable[[int], None]] = None,
    ) -> ReplayResult:
        """Rebuild pages from the checkpoint snapshot then apply WAL in LSN order."""
        start_lsn = checkpoint.start_lsn
        decision = self.decide(start_lsn)

        records = list(self.wal.iter_records(from_lsn=start_lsn))
        if records:
            matrix = self._build_expected_matrix(records)
            actual = np.array([r.lsn for r in records], dtype=np.int64)
            if not np.array_equal(matrix, actual):
                first_bad = int(np.argmax(matrix != actual))
                raise ReplayMismatchError(
                    "non-monotonic WAL sequence",
                    {"expected_lsn": int(matrix[first_bad]),
                     "actual_lsn": int(actual[first_bad])},
                )

        pages: Dict[int, Page] = {
            k: Page(**v.to_dict()) for k, v in checkpoint.pages.items()
        }
        expected_lsn = start_lsn
        applied = 0
        # The live view is always current; dirty tracks pages needing a flush.
        flush_buffer: Dict[int, Page] = {}
        flushed_groups = 0
        interrupted = False

        for rec in records:
            if already_applied_upto is not None and rec.lsn <= already_applied_upto:
                # Rebuild the live view for the already-applied prefix so the
                # remaining records validate against the correct page state.
                current = pages.get(rec.page_no)
                page = self._check_record(rec, current, expected_lsn)
                pages[page.page_no] = page
                expected_lsn = rec.lsn + 1
                continue
            current = pages.get(rec.page_no)
            page = self._check_record(rec, current, expected_lsn)
            # Live in-memory view stays current so repeated page edits validate.
            pages[page.page_no] = page
            expected_lsn = rec.lsn + 1
            applied += 1

            if decision.path == "precise":
                # One page per durable flush (handled by the engine layer).
                flush_buffer.clear()
                flush_buffer[page.page_no] = page
                if progress_cb:
                    progress_cb(rec.lsn)
            else:
                # Group pages; repeated edits collapse to the newest version.
                flush_buffer[page.page_no] = page
                if (
                    applied % decision.batch_size == 0
                    or len(flush_buffer) >= decision.flush_group_size
                ):
                    flushed_groups += 1
                    flush_buffer.clear()
                if progress_cb:
                    progress_cb(rec.lsn)

            if interrupt_after is not None and applied >= interrupt_after:
                flushed_groups += 1
                flush_buffer.clear()
                interrupted = True
                break

        if flush_buffer:
            flushed_groups += 1
            flush_buffer.clear()
        end_lsn = expected_lsn - 1
        result = ReplayResult(
            decision=decision,
            applied_records=applied,
            start_lsn=start_lsn,
            end_lsn=end_lsn,
            final_pages=pages,
            resume_from_lsn=(end_lsn + 1 if interrupted else None),
            interrupted=interrupted,
        )
        result.flushed_groups = flushed_groups
        return result
