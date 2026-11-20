"""Parallel shard execution with deterministic ordered reduction."""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Callable

from .checkpoint import CheckpointStore
from .config import PipelineConfig
from .errors import (
    E_VERSION_MISMATCH,
    E_WORKER_FAILED,
    E_WORKER_QUOTA,
    PipeError,
)
from .matching import match_image
from .scheduler import BudgetScheduler, ShardPlan, plan_shards
from .stats import build_image_stats
from .versioning import fingerprint, sample_fingerprint, short_fp

StopCallback = Callable[[], bool]


class Pipeline:
    def __init__(self, dataset: dict[str, Any], config: PipelineConfig,
                 workspace: str | Path, stop_callback: StopCallback | None = None) -> None:
        config.validate()
        self.dataset = dataset
        self.config = config
        self.store = CheckpointStore(workspace)
        self.images = sorted(dataset["images"], key=lambda img: img["image_id"])
        self.image_by_id = {img["image_id"]: img for img in self.images}
        self.plans = plan_shards(self.images, config.shard_size)
        self._stop_callback = stop_callback

    # ---------------------------------------------------------------- manifest
    def build_manifest(self) -> dict[str, Any]:
        image_fingerprints = {
            img["image_id"]: sample_fingerprint(img) for img in self.images
        }
        return {
            "schema_version": 1,
            "created_at_note": "deterministic local run",
            "dataset_fingerprint": fingerprint(
                {
                    "schema_version": self.dataset.get("schema_version"),
                    "seed": self.dataset.get("seed"),
                    "images": [
                        {"image_id": iid, "fp": image_fingerprints[iid]}
                        for iid in sorted(image_fingerprints)
                    ],
                }
            ),
            "threshold_version": self.config.threshold_version,
            "budget_version": self.config.budget_version,
            "config": self.config.to_dict(),
            "image_fingerprints": image_fingerprints,
            "shards": [
                {
                    "shard_index": plan.index,
                    "image_ids": list(plan.image_ids),
                    "candidate_count": plan.candidate_count,
                }
                for plan in self.plans
            ],
        }

    def _ensure_manifest(self, resume: bool) -> dict[str, Any]:
        manifest = self.build_manifest()
        if not self.store.exists():
            self.store.write_manifest(manifest)
            return manifest
        stored = self.store.read_manifest()
        if stored["threshold_version"] != manifest["threshold_version"]:
            raise PipeError(
                E_VERSION_MISMATCH,
                "stored manifest uses different threshold configuration",
                field="threshold_version",
            )
        if stored["budget_version"] != manifest["budget_version"]:
            raise PipeError(
                E_VERSION_MISMATCH,
                "stored manifest uses different budget configuration",
                field="budget_version",
            )
        changed = [
            image_id
            for image_id, fp in manifest["image_fingerprints"].items()
            if stored["image_fingerprints"].get(image_id) != fp
        ]
        if changed:
            raise PipeError(
                E_VERSION_MISMATCH,
                "input samples changed since the workspace was initialized",
                field="image_fingerprints",
                details={"changed_images": sorted(changed)},
            )
        if not resume:
            raise PipeError(
                E_VERSION_MISMATCH,
                "workspace already contains a run; use resume=True or another path",
                field="workspace",
            )
        return stored

    def stop_requested(self) -> bool:
        if self.store.stop_requested():
            return True
        if self._stop_callback is not None and self._stop_callback():
            return True
        return False

    def affected_shard_indices(self, stored_manifest: dict[str, Any]) -> list[int]:
        """Shards invalidated by sample, threshold or budget fingerprint drift."""
        new_manifest = self.build_manifest()
        changed_images = {
            image_id
            for image_id, fp in new_manifest["image_fingerprints"].items()
            if stored_manifest["image_fingerprints"].get(image_id) != fp
        }
        threshold_changed = (
            stored_manifest["threshold_version"] != new_manifest["threshold_version"]
        )
        budget_changed = (
            stored_manifest["budget_version"] != new_manifest["budget_version"]
        )
        affected: set[int] = set()
        for plan in self.plans:
            if threshold_changed or budget_changed:
                affected.add(plan.index)
            elif changed_images.intersection(plan.image_ids):
                affected.add(plan.index)
        return sorted(affected)

    def rebase(self) -> dict[str, Any]:
        """Rebuild the manifest and invalidate only affected shards.

        Unaffected shards stay on disk and are reused; every shard at or after
        the earliest affected index is removed so the contiguous chain remains
        valid.  Returns an observable invalidation report.
        """
        if not self.store.exists():
            raise FileNotFoundError(f"no existing run under {self.store.root}")
        stored = self.store.read_manifest()
        affected = self.affected_shard_indices(stored)
        new_manifest = self.build_manifest()
        removed: list[int] = []
        if affected:
            first_affected = min(affected)
            removed = self.store.truncate_from(first_affected)
        self.store.write_manifest(new_manifest)
        return {
            "affected_shard_indices": affected,
            "removed_shard_indices": removed,
            "reused_shard_indices": [
                i for i in self.store.existing_indices()
            ],
            "threshold_changed":
                stored["threshold_version"] != new_manifest["threshold_version"],
            "budget_changed":
                stored["budget_version"] != new_manifest["budget_version"],
        }

    # ---------------------------------------------------------------- shard work
    def process_shard(self, plan: ShardPlan) -> list[dict[str, Any]]:
        summaries: list[dict[str, Any]] = []
        for image_id in plan.image_ids:
            sample = self.image_by_id[image_id]
            summary = match_image(
                sample,
                self.config.iou_threshold,
                self.config.score_threshold,
            )
            summary["image_stats"] = build_image_stats(summary)
            summaries.append(summary)
        return summaries

    def build_record(self, plan: ShardPlan, summaries: list[dict[str, Any]],
                     status: str, reason: dict[str, Any] | None = None,
                     worker_id: int | None = None) -> dict[str, Any]:
        return {
            "shard_index": plan.index,
            "status": status,
            "worker_id": worker_id,
            "image_ids": list(plan.image_ids),
            "candidate_count": plan.candidate_count,
            "sample_fingerprints": {
                image_id: sample_fingerprint(self.image_by_id[image_id])
                for image_id in plan.image_ids
            },
            "threshold_version": self.config.threshold_version,
            "budget_version": self.config.budget_version,
            "reason": reason,
            "image_summaries": summaries,
        }

    # ---------------------------------------------------------------- run loop
    def run(self, resume: bool = True) -> dict[str, Any]:
        manifest = self._ensure_manifest(resume=resume)
        start_index = self.store.recover(manifest)

        # Budget already consumed by published shards must not be re-reserved.
        preloaded = sum(self.plans[i].candidate_count for i in range(start_index))
        scheduler = BudgetScheduler(
            self.plans,
            self.config.max_evaluations,
            self.config.worker_quota,
            self.config.workers,
        )
        with scheduler._lock:
            scheduler._state.used = preloaded
            # Per-worker quota is a forward-running guarantee; restored workers
            # start with zero individual usage.

        state_lock = threading.Lock()
        publish_lock = threading.Lock()
        cursor = start_index
        in_flight: set[int] = set()
        quota_attempts: dict[int, set[int]] = {}
        ready: dict[int, dict[str, Any]] = {}
        next_publish = start_index
        errors: list[dict[str, Any]] = []
        stop_event = threading.Event()
        stopped = False

        def flush_ready() -> None:
            nonlocal next_publish
            while next_publish in ready:
                self.store.publish(ready.pop(next_publish))
                next_publish += 1

        def worker_loop(worker_id: int) -> None:
            nonlocal cursor, stopped
            while not stop_event.is_set():
                with state_lock:
                    if stop_event.is_set():
                        return
                    if self.stop_requested():
                        stop_event.set()
                        stopped = True
                        return
                    if cursor >= len(self.plans):
                        return
                    # Skip indices currently processed by another worker.
                    while cursor < len(self.plans):
                        candidate = self.plans[cursor]
                        if candidate.index not in in_flight:
                            plan = candidate
                            break
                        # Still claimed by another worker; look at the next one.
                        cursor += 1
                    in_flight.add(plan.index)

                admission = scheduler.request(plan, worker_id)
                if not admission.accepted:
                    if admission.code == E_WORKER_QUOTA:
                        with state_lock:
                            tried = quota_attempts.setdefault(plan.index, set())
                            tried.add(worker_id)
                            give_up = len(tried) >= self.config.workers
                        if not give_up:
                            with state_lock:
                                in_flight.discard(plan.index)
                            time.sleep(0.002)
                            continue
                        reason = {
                            "code": admission.code,
                            "reason": admission.reason,
                            "detail": admission.detail,
                        }
                        record = self.build_record(
                            plan, [], status="unprocessed",
                            reason=reason, worker_id=worker_id,
                        )
                        with state_lock:
                            quota_attempts.pop(plan.index, None)
                            in_flight.discard(plan.index)
                            ready[plan.index] = record
                            cursor = max(cursor, plan.index + 1)
                        with publish_lock:
                            flush_ready()
                        continue

                    reason = {
                        "code": admission.code,
                        "reason": admission.reason,
                        "detail": admission.detail,
                    }
                    record = self.build_record(
                        plan, [], status="unprocessed",
                        reason=reason, worker_id=worker_id,
                    )
                    with state_lock:
                        in_flight.discard(plan.index)
                        ready[plan.index] = record
                        cursor = max(cursor, plan.index + 1)
                    with publish_lock:
                        flush_ready()
                    continue

                try:
                    summaries = self.process_shard(plan)
                    record = self.build_record(
                        plan, summaries, status="processed", worker_id=worker_id
                    )
                except PipeError as exc:
                    errors.append(exc.to_dict())
                    stop_event.set()
                    with state_lock:
                        in_flight.discard(plan.index)
                    return
                except Exception as exc:  # noqa: BLE001 - structured reporting
                    errors.append(
                        PipeError(
                            E_WORKER_FAILED,
                            f"{type(exc).__name__}: {exc}",
                            shard_index=plan.index,
                        ).to_dict()
                    )
                    stop_event.set()
                    with state_lock:
                        in_flight.discard(plan.index)
                    return

                with state_lock:
                    in_flight.discard(plan.index)
                    ready[plan.index] = record
                    cursor = max(cursor, plan.index + 1)
                with publish_lock:
                    flush_ready()

        threads = [
            threading.Thread(
                target=worker_loop,
                args=(worker_id,),
                name=f"detpipe-worker-{worker_id}",
                daemon=True,
            )
            for worker_id in range(self.config.workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            # No timeout-based killing: a worker always finishes its current
            # indivisible shard and only observes stop between shards.
            thread.join()
        # Drain anything buffered if a stop arrived late but work completed.
        with publish_lock:
            flush_ready()

        records = self.store.read_all_records()
        return {
            "status": "stopped" if stopped else ("failed" if errors else "completed"),
            "start_index": start_index,
            "shards_total": len(self.plans),
            "shards_published": len(records),
            "errors": errors,
            "scheduler_state": scheduler.state_snapshot(),
            "stop_requested": self.stop_requested(),
            "records": records,
            "manifest_fingerprint": short_fp(manifest),
        }
