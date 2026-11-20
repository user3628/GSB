"""End-to-end streaming engine with resumable checkpoints and strict validation."""

from __future__ import annotations

import io
import json
import os
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import numpy as np

from .acoustic import AcousticPredictor, AcousticResult, Prosody, acoustic_summary, assert_finite
from .calibration import calibration_digest, ensure_calibration
from .checkpoint import StageStore, record_key
from .config import EngineConfig, load_config
from .errors import TTSError, stream_mismatch
from .frontend import (
    FrontendResult,
    PhonemeTable,
    TextChunk,
    front_end,
    front_end_single_chunk,
)
from .inputs import Utterance, load_samples
from .scheduler import ExecutionBlock, SchedulePlan, plan_blocks
from .utils import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    canonical_json,
    json_line,
    sha256_bytes,
    sha256_json,
    write_jsonl_atomic,
)
from .versioning import FingerprintSet
from .waveform import SynthesisResult, pcm_summary, synth_chunk, wav_bytes, write_wav


@dataclass
class UtteranceOutcome:
    utt_id: str
    trace: list[dict[str, Any]]
    summary: dict[str, Any]
    fingerprints: dict[str, str]


@dataclass
class RunOutcome:
    status: str
    run_id: str
    trace_path: str | None
    timing_path: str | None
    summary_path: str | None
    aggregate_sha256: str | None
    utterances: list[dict[str, Any]]
    stopped_after_blocks: int | None = None
    error: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.status != "failed",
            "status": self.status,
            "run_id": self.run_id,
            "trace_path": self.trace_path,
            "timing_path": self.timing_path,
            "summary_path": self.summary_path,
            "aggregate_sha256": self.aggregate_sha256,
            "utterances": self.utterances,
            "stopped_after_blocks": self.stopped_after_blocks,
            "error": self.error,
        }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _npy_bytes(values: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, np.ascontiguousarray(values, dtype="<f8"), allow_pickle=False)
    return buffer.getvalue()


def _load_npy(data: bytes) -> np.ndarray:
    return np.load(io.BytesIO(data), allow_pickle=False)


def _first_diff_frame(left: np.ndarray, right: np.ndarray) -> tuple[int, str | None]:
    common = min(left.shape[0], right.shape[0])
    for index in range(common):
        if not np.array_equal(left[index], right[index]):
            columns = np.where(left[index] != right[index])[0]
            column = int(columns[0]) if columns.size else -1
            if column >= 0:
                name = f"band_{column}" if column < left.shape[1] - 2 else ("f0_hz" if column == left.shape[1] - 2 else "envelope")
            else:
                name = None
            return index, name
    return common, None


class StreamingTTSEngine:
    def __init__(
        self,
        *,
        workspace: str | Path,
        config_path: str | Path,
        run_id: str = "main",
    ):
        self.workspace = Path(workspace).resolve()
        config_candidate = Path(config_path)
        if not config_candidate.is_absolute():
            config_candidate = self.workspace / config_candidate
        self.config = load_config(config_candidate, workspace=self.workspace)
        self.config_path = self.config.config_path
        self.run_id = run_id
        self.run_root = self.workspace / "runtime" / run_id
        self.output_root = self.workspace / "output" / run_id
        self.stop_path = self.run_root / "stop.request"
        self.status_path = self.run_root / "status.json"
        self.trace_path = self.run_root / "latency_trace.jsonl"
        self.timing_path = self.run_root / "timing_audit.jsonl"
        self.summary_path = self.run_root / "run_summary.json"
        self.phoneme_table = PhonemeTable.load(self.config.resolved_path("phoneme_table"))
        self.predictor = AcousticPredictor(self.config)
        self.calibration = ensure_calibration(self.config, self.predictor)
        self.config_version = self.config.version_fingerprint(
            self.predictor.weight_digest,
            self.phoneme_table.digest,
            calibration_digest(self.calibration),
        )
        self.store = StageStore(
            workspace=self.workspace,
            run_id=run_id,
            config_version=self.config_version,
        )
        self.run_root.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ status
    def _write_status(self, **fields: Any) -> dict[str, Any]:
        existing = {}
        if self.status_path.exists():
            try:
                existing = json.loads(self.status_path.read_text(encoding="utf-8"))
            except Exception:
                existing = {}
        for transient in (
            "error",
            "aggregate_sha256",
            "trace_sha256",
            "summary_sha256",
            "stop_reason",
            "no_exit_report",
            "elapsed_ms",
        ):
            if transient not in fields:
                existing.pop(transient, None)
        payload = {
            **existing,
            **fields,
            "run_id": self.run_id,
            "updated_at": _now_iso(),
        }
        atomic_write_json(self.status_path, payload)
        return payload

    def status(self) -> dict[str, Any]:
        if not self.status_path.exists():
            return {
                "run_id": self.run_id,
                "status": "not_started",
                "stop_requested": self.stop_path.exists(),
                "config_version": self.config_version,
            }
        payload = json.loads(self.status_path.read_text(encoding="utf-8"))
        payload["stop_requested"] = self.stop_path.exists()
        payload["config_version"] = self.config_version
        return payload

    def request_stop(self) -> dict[str, Any]:
        self.run_root.mkdir(parents=True, exist_ok=True)
        atomic_write_text(self.stop_path, "stop-after-current-indivisible-block\n")
        self._write_status(stop_requested=True)
        return self.status()

    def clear_stop(self) -> None:
        self.stop_path.unlink(missing_ok=True)

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    # --------------------------------------------------------------- versions
    def fingerprints_for(self, utt: Utterance) -> FingerprintSet:
        return FingerprintSet.compute(
            config=self.config,
            text=utt.text,
            language=utt.language,
            speaker=utt.speaker,
            prosody=utt.prosody,
            phoneme_table_digest=self.phoneme_table.digest,
            weights_digest=self.predictor.weight_digest,
            calibration=self.calibration,
        )

    def version_report(self, samples_path: str | Path | None = None) -> dict[str, Any]:
        path = self._resolve_samples_path(samples_path)
        utterances = load_samples(path)
        return {
            "run_id": self.run_id,
            "config_version": self.config_version,
            "weights_digest": self.predictor.weight_digest,
            "phoneme_table_digest": self.phoneme_table.digest,
            "calibration_digest": calibration_digest(self.calibration),
            "config_digests": {
                "model": self.config.model_structure_digest(),
                "split": self.config.split_digest(),
                "budget": self.config.budget_digest(),
                "synthesis": self.config.synthesis_digest(),
            },
            "utterances": [
                {"utt_id": utt.utt_id, **self.fingerprints_for(utt).to_dict()} for utt in utterances
            ],
        }

    def stage_report(self) -> dict[str, Any]:
        ledger = self.store.ledger()
        utt_ids = list(ledger.get("utt_order", []))
        stages: dict[str, Any] = {}
        for utt_id in utt_ids:
            utt_stages: dict[str, Any] = {}
            for stage in ("frontend", "acoustic", "waveform", "validation"):
                records = self.store.get_records(utt_id, stage)
                indices = [int(record["chunk_index"]) for record in records]
                expected = (
                    int(records[0]["stage_total_chunks"])
                    if records and stage != "validation"
                    else (1 if records else None)
                )
                contiguous = bool(records) and indices == list(range(expected or 0))
                products_ok = True
                for record in records:
                    try:
                        self.store.read_product(record)
                    except TTSError:
                        products_ok = False
                        break
                utt_stages[stage] = {
                    "published_records": len(records),
                    "expected_records": expected,
                    "chunk_indices": indices,
                    "seqs": [int(record["seq"]) for record in records],
                    "fingerprints": list(
                        dict.fromkeys(record["fingerprint"] for record in records)
                    ),
                    "products_ok": products_ok,
                    "complete": contiguous and products_ok,
                }
            stages[utt_id] = utt_stages
        return {
            "run_id": self.run_id,
            "next_seq": int(ledger.get("next_seq", 0)),
            "last_key": ledger.get("last_key"),
            "stages": stages,
            "invalidations": ledger.get("invalidations", []),
            "config_version": self.config_version,
        }

    # ------------------------------------------------------------------- run
    def run(
        self,
        *,
        samples_path: str | Path | None = None,
        resume: bool = False,
        stop_after_chunks: int | None = None,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> RunOutcome:
        start_ns = time.monotonic_ns()
        samples_file = self._resolve_samples_path(samples_path)
        utterances = load_samples(samples_file)
        self.store.ensure_utt_order([item.utt_id for item in utterances])
        if resume:
            self.clear_stop()
        if self.stop_requested():
            self._write_status(
                status="stopped",
                phase="before_start",
                stop_requested=True,
                started_at=_now_iso(),
            )
            return RunOutcome(
                status="stopped",
                run_id=self.run_id,
                trace_path=None,
                timing_path=None,
                summary_path=None,
                aggregate_sha256=None,
                utterances=[],
            )

        self._write_status(
            status="running",
            phase="start",
            stop_requested=False,
            started_at=_now_iso(),
            config_version=self.config_version,
            sample_count=len(utterances),
            completed_blocks=0,
        )
        completed_blocks = 0
        trace: list[dict[str, Any]] = []
        timing_audit: list[dict[str, Any]] = []
        outcomes: list[UtteranceOutcome] = []
        local_stop_reason: str | None = None

        try:
            for utt_index, utt in enumerate(utterances):
                outcome, stop = self._run_utterance(
                    utt=utt,
                    stop_after_chunks=stop_after_chunks,
                    completed_blocks=completed_blocks,
                    timing_audit=timing_audit,
                    progress=progress,
                )
                outcomes.append(outcome)
                trace.extend(outcome.trace)
                completed_blocks += len(outcome.trace)
                if stop is not None:
                    local_stop_reason = stop
                    break
                self._write_status(
                    status="running",
                    phase="utterance_complete",
                    current_utt_id=utt.utt_id,
                    utterance_index=utt_index,
                    completed_blocks=completed_blocks,
                )
            if local_stop_reason is not None:
                status = {
                    "status": "stopped",
                    "phase": "graceful_stop",
                    "stop_reason": local_stop_reason,
                    "completed_blocks": completed_blocks,
                    "no_exit_report": True,
                    "elapsed_ms": (time.monotonic_ns() - start_ns) / 1_000_000.0,
                }
                self._write_status(**status)
                return RunOutcome(
                    status="stopped",
                    run_id=self.run_id,
                    trace_path=None,
                    timing_path=None,
                    summary_path=None,
                    aggregate_sha256=None,
                    utterances=[item.summary for item in outcomes],
                    stopped_after_blocks=completed_blocks,
                )

            write_jsonl_atomic(self.trace_path, trace)
            write_jsonl_atomic(self.timing_path, timing_audit)
            summary = self._build_run_summary(outcomes, trace)
            atomic_write_json(self.summary_path, summary)
            self._write_status(
                status="complete",
                phase="complete",
                completed_blocks=completed_blocks,
                aggregate_sha256=summary["aggregate_sha256"],
                trace_sha256=summary["trace_sha256"],
                summary_sha256=sha256_json(summary),
                elapsed_ms=(time.monotonic_ns() - start_ns) / 1_000_000.0,
                no_exit_report=False,
            )
            return RunOutcome(
                status="complete",
                run_id=self.run_id,
                trace_path=str(self.trace_path),
                timing_path=str(self.timing_path),
                summary_path=str(self.summary_path),
                aggregate_sha256=summary["aggregate_sha256"],
                utterances=[item.summary for item in outcomes],
            )
        except TTSError as exc:
            self._write_status(
                status="failed",
                phase="error",
                error=exc.to_dict()["error"],
                elapsed_ms=(time.monotonic_ns() - start_ns) / 1_000_000.0,
            )
            raise

        except Exception as exc:
            self._write_status(
                status="failed",
                phase="error",
                error={"code": "E_INTERNAL", "message": str(exc), "type": type(exc).__name__},
                elapsed_ms=(time.monotonic_ns() - start_ns) / 1_000_000.0,
            )
            raise

    def _resolve_samples_path(self, samples_path: str | Path | None) -> Path:
        if samples_path is None:
            return self.config.resolved_path("samples")
        candidate = Path(samples_path)
        return candidate if candidate.is_absolute() else self.workspace / candidate

    # ------------------------------------------------------------- utterance
    def _run_utterance(
        self,
        *,
        utt: Utterance,
        stop_after_chunks: int | None,
        completed_blocks: int,
        timing_audit: list[dict[str, Any]],
        progress: Callable[[dict[str, Any]], None] | None,
    ) -> tuple[UtteranceOutcome, str | None]:
        # A user-visible WAV is always the product of one complete, validated
        # utterance. Remove any stale prior publication before rebuilding it.
        self._purge_utterance_outputs(utt.utt_id)
        fps = self.fingerprints_for(utt)
        prosody = Prosody.from_dict(utt.prosody)

        frontend = front_end(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=self.config.split,
            phoneme_table=self.phoneme_table,
        )
        self._synchronize_frontend(utt=utt, frontend=frontend, fingerprint=fps.frontend)
        plan = plan_blocks(
            frontend=frontend,
            config=self.config,
            calibration=self.calibration,
            prosody=prosody,
        )
        self._synchronize_blocks(
            utt=utt,
            plan=plan,
            acoustic_fingerprint=fps.acoustic,
            waveform_fingerprint=fps.waveform,
        )

        block_acoustics: list[AcousticResult] = []
        block_waves: list[SynthesisResult] = []
        trace: list[dict[str, Any]] = []
        total_blocks = len(plan.blocks)
        for block in plan.blocks:
            acoustic, acoustic_cache = self._obtain_acoustic(
                utt=utt,
                block=block,
                prosody=prosody,
                fingerprint=fps.acoustic,
                stage_total_chunks=total_blocks,
                timing_audit=timing_audit,
            )
            wave, wave_cache = self._obtain_waveform(
                utt=utt,
                block=block,
                acoustic=acoustic,
                fingerprint=fps.waveform,
                stage_total_chunks=total_blocks,
                timing_audit=timing_audit,
            )
            block_acoustics.append(acoustic)
            block_waves.append(wave)
            record = block.trace_record(utt_id=utt.utt_id, calibration=self.calibration)
            record.update(
                {
                    "version_fingerprint": fps.overall,
                    "acoustic_digest": acoustic.digest,
                    "pcm_digest": wave.digest,
                    "clip_count": wave.clip_count,
                }
            )
            trace.append(record)
            if progress:
                progress(
                    {
                        "utt_id": utt.utt_id,
                        "block_index": block.block_index,
                        "blocks_total": total_blocks,
                        "degraded": block.degraded,
                    }
                )
            block_count = completed_blocks + len(trace)
            stop_reason = None
            if stop_after_chunks is not None and block_count >= stop_after_chunks:
                stop_reason = f"stop_after_chunks:{stop_after_chunks}"
            elif self.stop_requested():
                stop_reason = "stop_request"
            if stop_reason is not None:
                return (
                    UtteranceOutcome(utt_id=utt.utt_id, trace=trace, summary={}, fingerprints=fps.to_dict()),
                    stop_reason,
                )

        outcome = self._finalize_utterance(
            utt=utt,
            fps=fps,
            plan=plan,
            block_acoustics=block_acoustics,
            block_waves=block_waves,
            trace=trace,
            timing_audit=timing_audit,
        )
        return outcome, None

    def _purge_utterance_outputs(self, utt_id: str) -> None:
        (self.output_root / f"{utt_id}.wav").unlink(missing_ok=True)
        (self.output_root / "acoustic" / f"{utt_id}.npy").unlink(missing_ok=True)
        (self.output_root / "markers" / f"{utt_id}.json").unlink(missing_ok=True)
        chunks_dir = self.output_root / "chunks" / utt_id
        if chunks_dir.exists():
            shutil.rmtree(chunks_dir)

    def _synchronize_frontend(
        self, *, utt: Utterance, frontend: FrontendResult, fingerprint: str
    ) -> None:
        existing = self.store.get_records(utt.utt_id, "frontend")
        if any(record["fingerprint"] != fingerprint for record in existing):
            self.store.invalidate_from(
                utt_id=utt.utt_id,
                stage="frontend",
                reason=f"frontend fingerprint changed to {fingerprint}",
            )
            existing = []
        by_index = {int(record["chunk_index"]): record for record in existing}
        previous_key: str | None = None
        for chunk in frontend.chunks:
            record = by_index.get(chunk.index)
            if record and record["fingerprint"] == fingerprint:
                previous_key = record["key"]
                continue
            published = self.store.publish_json(
                utt_id=utt.utt_id,
                stage="frontend",
                chunk_index=chunk.index,
                stage_total_chunks=len(frontend.chunks),
                fingerprint=fingerprint,
                value=chunk.to_dict(),
                prerequisites=[previous_key] if previous_key else [],
            )
            previous_key = published["key"]

    def _synchronize_blocks(
        self,
        *,
        utt: Utterance,
        plan: SchedulePlan,
        acoustic_fingerprint: str,
        waveform_fingerprint: str,
    ) -> None:
        acoustic_records = self.store.get_records(utt.utt_id, "acoustic")
        acoustic_bad = any(
            record["fingerprint"] != acoustic_fingerprint
            or int(record["stage_total_chunks"]) != len(plan.blocks)
            or int(record["chunk_index"]) >= len(plan.blocks)
            for record in acoustic_records
        )
        if acoustic_bad:
            self.store.invalidate_from(
                utt_id=utt.utt_id,
                stage="acoustic",
                reason=f"acoustic fingerprint/schedule changed to {acoustic_fingerprint}",
            )
        waveform_records = self.store.get_records(utt.utt_id, "waveform")
        waveform_bad = any(
            record["fingerprint"] != waveform_fingerprint
            or int(record["stage_total_chunks"]) != len(plan.blocks)
            or int(record["chunk_index"]) >= len(plan.blocks)
            for record in waveform_records
        )
        if waveform_bad:
            self.store.invalidate_from(
                utt_id=utt.utt_id,
                stage="waveform",
                reason=f"waveform fingerprint changed to {waveform_fingerprint}",
            )
    def _obtain_acoustic(
        self,
        *,
        utt: Utterance,
        block: ExecutionBlock,
        prosody: Prosody,
        fingerprint: str,
        stage_total_chunks: int,
        timing_audit: list[dict[str, Any]],
    ) -> tuple[AcousticResult, bool]:
        record = self.store.get_record(utt.utt_id, "acoustic", block.block_index)
        if (
            record is not None
            and record["fingerprint"] == fingerprint
            and int(record["stage_total_chunks"]) == stage_total_chunks
        ):
            values = _load_npy(self.store.read_product(record))
            if values.shape != (block.frame_count, self.config.model.output_dim):
                raise TTSError(
                    "E_INVALID_STAGE",
                    "cached acoustic product has an unexpected shape",
                    {
                        "utt_id": utt.utt_id,
                        "block_index": block.block_index,
                        "expected": [block.frame_count, self.config.model.output_dim],
                        "actual": list(values.shape),
                    },
                )
            assert_finite(utt_id=utt.utt_id, values=values, frame_offset=block.frame_start)
            timing_audit.append(
                {
                    "utt_id": utt.utt_id,
                    "block_index": block.block_index,
                    "stage": "acoustic",
                    "cache_hit": True,
                    "elapsed_ms": 0.0,
                }
            )
            markers = tuple(
                marker
                for phone in block.chunk.phonemes
                for marker in [phone.marker] * phone.frames
            )
            return (
                AcousticResult(
                    utt_id=utt.utt_id,
                    frame_start=block.frame_start,
                    frame_end=block.frame_end,
                    values=values,
                    markers=markers,
                ),
                True,
            )
        start = time.perf_counter_ns()
        embedding_cache = self.predictor.prepare_embeddings(
            phones=block.chunk.phonemes,
            speaker=utt.speaker,
            workers=block.worker_count,
            depth=block.preprocess_depth,
        )
        acoustic = self.predictor.predict_phones(
            utt_id=utt.utt_id,
            phones=block.chunk.phonemes,
            speaker=utt.speaker,
            prosody=prosody,
            frame_start=block.frame_start,
            embedding_cache=embedding_cache,
        )
        assert_finite(utt_id=utt.utt_id, values=acoustic.values, frame_offset=block.frame_start)
        self.store.publish_bytes(
            utt_id=utt.utt_id,
            stage="acoustic",
            chunk_index=block.block_index,
            stage_total_chunks=stage_total_chunks,
            fingerprint=fingerprint,
            product=_npy_bytes(acoustic.values),
            suffix="npy",
            prerequisites=[
                record_key(utt.utt_id, "frontend", block.planned_chunk_index)
            ],
        )
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        timing_audit.append(
            {
                "utt_id": utt.utt_id,
                "block_index": block.block_index,
                "stage": "acoustic",
                "cache_hit": False,
                "elapsed_ms": elapsed_ms,
            }
        )
        return acoustic, False

    def _obtain_waveform(
        self,
        *,
        utt: Utterance,
        block: ExecutionBlock,
        acoustic: AcousticResult,
        fingerprint: str,
        stage_total_chunks: int,
        timing_audit: list[dict[str, Any]],
    ) -> tuple[SynthesisResult, bool]:
        record = self.store.get_record(utt.utt_id, "waveform", block.block_index)
        if (
            record is not None
            and record["fingerprint"] == fingerprint
            and int(record["stage_total_chunks"]) == stage_total_chunks
        ):
            pcm = self.store.read_product(record)
            expected_bytes = block.frame_count * self.config.hop_samples * self.config.output.frame_bytes
            if len(pcm) != expected_bytes:
                raise TTSError(
                    "E_INVALID_STAGE",
                    "cached waveform product has an unexpected byte length",
                    {
                        "utt_id": utt.utt_id,
                        "block_index": block.block_index,
                        "expected": expected_bytes,
                        "actual": len(pcm),
                    },
                )
            timing_audit.append(
                {
                    "utt_id": utt.utt_id,
                    "block_index": block.block_index,
                    "stage": "waveform",
                    "cache_hit": True,
                    "elapsed_ms": 0.0,
                }
            )
            return (
                SynthesisResult(
                    utt_id=utt.utt_id,
                    frame_start=block.frame_start,
                    frame_end=block.frame_end,
                    sample_start=block.sample_start,
                    sample_end=block.sample_end,
                    channels=self.config.output.channels,
                    bit_depth=self.config.output.bit_depth,
                    sample_rate=self.config.output.sample_rate,
                    pcm=pcm,
                    clip_count=int(record.get("clip_count", 0)),
                ),
                True,
            )
        start = time.perf_counter_ns()
        wave = synth_chunk(utt_id=utt.utt_id, acoustic=acoustic, config=self.config)
        self.store.publish_bytes(
            utt_id=utt.utt_id,
            stage="waveform",
            chunk_index=block.block_index,
            stage_total_chunks=stage_total_chunks,
            fingerprint=fingerprint,
            product=wave.pcm,
            suffix="pcm",
            prerequisites=[record_key(utt.utt_id, "acoustic", block.block_index)],
            extra_record={"clip_count": wave.clip_count},
        )
        elapsed_ms = (time.perf_counter_ns() - start) / 1_000_000.0
        timing_audit.append(
            {
                "utt_id": utt.utt_id,
                "block_index": block.block_index,
                "stage": "waveform",
                "cache_hit": False,
                "elapsed_ms": elapsed_ms,
            }
        )
        return wave, False

    def _finalize_utterance(
        self,
        *,
        utt: Utterance,
        fps: FingerprintSet,
        plan: SchedulePlan,
        block_acoustics: list[AcousticResult],
        block_waves: list[SynthesisResult],
        trace: list[dict[str, Any]],
        timing_audit: list[dict[str, Any]],
    ) -> UtteranceOutcome:
        chunked_values = np.concatenate([item.values for item in block_acoustics], axis=0)
        chunked_markers = [marker for item in block_acoustics for marker in item.markers]
        chunked_pcm = b"".join(item.pcm for item in block_waves)
        chunked_samples = sum(item.sample_count_per_channel for item in block_waves)
        if chunked_samples != chunked_values.shape[0] * self.config.hop_samples:
            raise TTSError(
                "E_INTERNAL",
                "sample/frame cardinality invariant failed",
                {"utt_id": utt.utt_id},
            )

        full_frontend = front_end_single_chunk(
            utt_id=utt.utt_id,
            text=utt.text,
            language=utt.language,
            split=self.config.split,
            phoneme_table=self.phoneme_table,
        )
        full_acoustic = self.predictor.predict_all(
            frontend=full_frontend,
            speaker=utt.speaker,
            prosody=Prosody.from_dict(utt.prosody),
        )
        assert_finite(utt_id=utt.utt_id, values=full_acoustic.values, frame_offset=0)
        full_wave = synth_chunk(utt_id=utt.utt_id, acoustic=full_acoustic, config=self.config)

        chunked_summary = {
            "acoustic": acoustic_summary(
                values=chunked_values,
                frame_start=0,
                markers=chunked_markers,
            ),
            "pcm": pcm_summary(
                pcm=chunked_pcm,
                sample_count=chunked_samples,
                channels=self.config.output.channels,
                clip_count=sum(item.clip_count for item in block_waves),
                markers=chunked_markers,
            ),
        }
        full_summary = {
            "acoustic": acoustic_summary(
                values=full_acoustic.values,
                frame_start=0,
                markers=full_acoustic.markers,
            ),
            "pcm": pcm_summary(
                pcm=full_wave.pcm,
                sample_count=full_wave.sample_count_per_channel,
                channels=self.config.output.channels,
                clip_count=full_wave.clip_count,
                markers=full_acoustic.markers,
            ),
        }

        def mismatch(
            *,
            detail: str,
            frame_index: int = 0,
            sample_index: int = 0,
            parameter: str | None = None,
            extra: dict[str, Any] | None = None,
        ) -> TTSError:
            block_index = 0
            for block in plan.blocks:
                if frame_index >= block.frame_start:
                    block_index = block.block_index
            return stream_mismatch(
                utt_id=utt.utt_id,
                first_deviating_block=block_index,
                frame_index=frame_index,
                sample_index=sample_index,
                chunked_summary=chunked_summary,
                full_summary=full_summary,
                detail=detail,
                parameter_name=parameter,
                **(extra or {}),
            )

        if chunked_values.shape != full_acoustic.values.shape:
            raise mismatch(
                detail="chunked/full acoustic frame counts differ",
                frame_index=min(chunked_values.shape[0], full_acoustic.values.shape[0]),
            )
        if not np.array_equal(chunked_values, full_acoustic.values):
            frame_index, parameter = _first_diff_frame(chunked_values, full_acoustic.values)
            raise mismatch(
                detail="chunked/full acoustic parameters differ",
                frame_index=frame_index,
                parameter=parameter,
            )
        if tuple(chunked_markers) != tuple(full_acoustic.markers):
            frame_index = 0
            for index, (left, right) in enumerate(zip(chunked_markers, full_acoustic.markers)):
                if left != right:
                    frame_index = index
                    break
            raise mismatch(detail="prosody marker sequence differs", frame_index=frame_index)
        if chunked_samples != full_wave.sample_count_per_channel:
            raise mismatch(
                detail="chunked/full PCM sample counts differ",
                frame_index=min(chunked_values.shape[0], full_acoustic.values.shape[0]),
                sample_index=min(chunked_samples, full_wave.sample_count_per_channel),
            )
        if chunked_pcm != full_wave.pcm:
            frame_bytes = self.config.output.frame_bytes
            first_byte = next(
                index for index, (left, right) in enumerate(zip(chunked_pcm, full_wave.pcm)) if left != right
            )
            sample_index = first_byte // frame_bytes
            raise mismatch(
                detail="chunked/full PCM bytes differ",
                frame_index=sample_index // self.config.hop_samples,
                sample_index=sample_index,
                extra={"pcm_byte_index": first_byte, "expected_byte": full_wave.pcm[first_byte], "actual_byte": chunked_pcm[first_byte]},
            )
        if sum(item.clip_count for item in block_waves) != full_wave.clip_count:
            raise mismatch(
                detail="chunked/full clipping counters differ",
                extra={"chunked_clip_count": sum(item.clip_count for item in block_waves), "full_clip_count": full_wave.clip_count},
            )

        full_wav = wav_bytes(pcm=chunked_pcm, config=self.config)
        reference_path = self.config.resolved_path("reference_dir") / f"{utt.utt_id}.wav"
        reference_match: bool | None = None
        reference_digest: str | None = None
        if reference_path.exists():
            reference_bytes = reference_path.read_bytes()
            reference_digest = sha256_bytes(reference_bytes)
            reference_match = reference_bytes == full_wav

        # Only after every strict check succeeds are user-visible artifacts created.
        chunks_dir = self.output_root / "chunks" / utt.utt_id
        for block, wave in zip(plan.blocks, block_waves):
            write_wav(chunks_dir / f"{block.block_index:04d}.wav", pcm=wave.pcm, config=self.config)
        wav_path = self.output_root / f"{utt.utt_id}.wav"
        atomic_write_bytes(wav_path, full_wav)
        wav_rel = wav_path.relative_to(self.workspace).as_posix()
        acoustic_path = self.output_root / "acoustic" / f"{utt.utt_id}.npy"
        atomic_write_bytes(acoustic_path, _npy_bytes(chunked_values))
        acoustic_rel = acoustic_path.relative_to(self.workspace).as_posix()
        markers_path = self.output_root / "markers" / f"{utt.utt_id}.json"
        atomic_write_json(markers_path, list(chunked_markers))
        markers_rel = markers_path.relative_to(self.workspace).as_posix()

        validation_summary = {
            "utt_id": utt.utt_id,
            "version_fingerprint": fps.overall,
            "stage_fingerprints": fps.to_dict(),
            "normalized_text": full_frontend.normalized_text,
            "frontend_chunk_count": max(
                (block.planned_chunk_index for block in plan.blocks), default=-1
            ) + 1,
            "one_shot_chunk_count": len(full_frontend.chunks),
            "active_blocks": [
                {
                    "block_index": block.block_index,
                    "planned_chunk_index": block.planned_chunk_index,
                    "char_start": block.chunk.char_start,
                    "char_end": block.chunk.char_end,
                    "frame_start": block.frame_start,
                    "frame_end": block.frame_end,
                    "sample_start": wave.sample_start,
                    "sample_end": wave.sample_end,
                    "budget_ms": block.budget_ms,
                    "estimated_ms": block.estimated_ms,
                    "degraded": block.degraded,
                    "degrade_reason": block.degrade_reason,
                    "worker_count": block.worker_count,
                    "preprocess_depth": block.preprocess_depth,
                }
                for block, wave in zip(plan.blocks, block_waves)
            ],
            "degradation_events": list(plan.degradation_events),
            "chunked_summary": chunked_summary,
            "full_summary": full_summary,
            "sample_count_per_channel": chunked_samples,
            "frame_count": int(chunked_values.shape[0]),
            "prosody_markers": chunked_markers,
            "clip_count": sum(item.clip_count for item in block_waves),
            "wav_path": wav_rel,
            "wav_sha256": sha256_bytes(full_wav),
            "wav_bytes": len(full_wav),
            "acoustic_path": acoustic_rel,
            "markers_path": markers_rel,
            "pcm_sha256": sha256_bytes(chunked_pcm),
            "acoustic_sha256": sha256_bytes(chunked_values.astype("<f8", copy=False).tobytes()),
            "reference_path": (
                reference_path.relative_to(self.workspace).as_posix()
                if reference_path.exists()
                else None
            ),
            "reference_digest": reference_digest,
            "reference_match": reference_match,
            "consistent": True,
        }
        existing_validation = self.store.get_record(utt.utt_id, "validation", 0)
        if not (
            existing_validation
            and existing_validation["fingerprint"] == fps.validation
        ):
            if existing_validation:
                self.store.invalidate_from(
                    utt_id=utt.utt_id,
                    stage="validation",
                    reason=f"validation fingerprint changed to {fps.validation}",
                )
            prerequisites = [
                record_key(utt.utt_id, "waveform", block.block_index) for block in plan.blocks
            ]
            self.store.publish_json(
                utt_id=utt.utt_id,
                stage="validation",
                chunk_index=0,
                stage_total_chunks=1,
                fingerprint=fps.validation,
                value=validation_summary,
                prerequisites=prerequisites,
            )

        summary = {
            "utt_id": utt.utt_id,
            "wav_path": wav_rel,
            "acoustic_path": acoustic_rel,
            "markers_path": markers_rel,
            "wav_sha256": validation_summary["wav_sha256"],
            "wav_bytes": validation_summary["wav_bytes"],
            "pcm_sha256": validation_summary["pcm_sha256"],
            "acoustic_sha256": validation_summary["acoustic_sha256"],
            "sample_count_per_channel": validation_summary["sample_count_per_channel"],
            "frame_count": validation_summary["frame_count"],
            "marker_count": len(chunked_markers),
            "markers_sha256": sha256_bytes("\n".join(chunked_markers).encode("utf-8")),
            "block_count": len(plan.blocks),
            "frontend_chunk_count": validation_summary["frontend_chunk_count"],
            "one_shot_chunk_count": validation_summary["one_shot_chunk_count"],
            "clip_count": validation_summary["clip_count"],
            "degraded": any(block.degraded for block in plan.blocks),
            "degrade_reasons": list(
                dict.fromkeys(
                    block.degrade_reason for block in plan.blocks if block.degrade_reason
                )
            ),
            "worker_count": plan.worker_count,
            "reference_match": reference_match,
            "version_fingerprint": fps.overall,
        }
        return UtteranceOutcome(
            utt_id=utt.utt_id,
            trace=trace,
            summary=summary,
            fingerprints=fps.to_dict(),
        )

    def _build_run_summary(
        self, outcomes: list[UtteranceOutcome], trace: list[dict[str, Any]]
    ) -> dict[str, Any]:
        utterances = [item.summary for item in outcomes]
        aggregate = sha256_json({"utterances": utterances})
        return {
            "schema_version": 1,
            "run_id": self.run_id,
            "config_version": self.config_version,
            "weights_digest": self.predictor.weight_digest,
            "phoneme_table_digest": self.phoneme_table.digest,
            "calibration_digest": calibration_digest(self.calibration),
            "utterance_count": len(utterances),
            "utterances": utterances,
            "degraded_utterances": [
                item["utt_id"] for item in utterances if item.get("degraded")
            ],
            "trace_records": len(trace),
            "trace_sha256": sha256_bytes("".join(json_line(item) for item in trace).encode("utf-8")),
            "aggregate_sha256": aggregate,
            "consistent": all(True for _ in utterances),
        }

    # ------------------------------------------------------------- exports
    def export_paths(self, utt_id: str | None = None) -> dict[str, Any]:
        if utt_id:
            wav = self.output_root / f"{utt_id}.wav"
            chunks = self.output_root / "chunks" / utt_id
            return {
                "wav": str(wav) if wav.exists() else None,
                "chunks": [
                    str(path) for path in sorted(chunks.glob("*.wav"))
                ] if chunks.exists() else [],
            }
        return {
            "wavs": [str(path) for path in sorted(self.output_root.glob("*.wav"))],
            "trace": str(self.trace_path) if self.trace_path.exists() else None,
            "timing": str(self.timing_path) if self.timing_path.exists() else None,
            "summary": str(self.summary_path) if self.summary_path.exists() else None,
        }
