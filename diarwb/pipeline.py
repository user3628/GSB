"""Stage pipeline: fingerprints, incremental recompute, snapshots, recovery."""
from __future__ import annotations

import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np

from .audio import read_wav
from .config import PipelineConfig
from .embedding import embedding_bytes
from .errors import (EIncrementalMismatch, EInvalidStage, EStaleSnapshot,
                     EStopped, WorkbenchError)
from .recognition import recognize_channels
from .separation import (content_seed, separated_bytes, separate_mixture)
from .timeline import Segment, Timeline
from .workspace import (SNAPSHOT_REASON_EDIT, SNAPSHOT_REASON_RUN, Workspace,
                        atomic_write_json, canonical_json, digest_json)

STAGE_IMPORT = "import"
STAGE_SEPARATION = "separation"
STAGE_RECOGNITION = "recognition"
STAGE_ORDER = [STAGE_SEPARATION, STAGE_RECOGNITION]


def stage_fingerprint(parts: List[str]) -> str:
    return "fp-" + digest_json(parts)[:16]


class QueryCache:
    """Small in-process cache keyed by content, config and edit versions."""

    def __init__(self) -> None:
        self._store: Dict[str, Tuple[Any, int]] = {}

    def key(self, shard_id: str, kind: str, content_fp: str,
            config_fp: str, edit_version: int) -> str:
        return digest_json([shard_id, kind, content_fp, config_fp, edit_version])

    def get(self, key: str) -> Optional[Any]:
        item = self._store.get(key)
        return None if item is None else item[0]

    def put(self, key: str, value: Any) -> None:
        self._store[key] = (value, time.monotonic_ns())

    def clear(self) -> None:
        self._store.clear()


class Pipeline:
    def __init__(self, workspace: Workspace, config: PipelineConfig):
        self.ws = workspace
        self.cfg = config
        self.cache = QueryCache()
        self.infos = {info.shard_id: info
                      for info in workspace.load_manifest()}

    # ---------- helpers ----------
    def _speaker_count(self, shard_id: str) -> int:
        return int(self.cfg.speaker_overrides.get(
            shard_id, self.infos[shard_id].n_speakers))

    def _check_stop(self) -> None:
        if self.ws.stop_requested():
            raise EStopped("stop requested; atomic unit completed")

    def _snapshot(self, shard_id: str, stage: str, reason: str,
                  stage_version: str, result: Dict[str, Any],
                  edit_version: int) -> None:
        info = self.infos[shard_id]
        record = {
            "shard_id": shard_id,
            "stage": stage,
            "reason": reason,
            "content_fingerprint": info.content_fingerprint,
            "config_version": self.cfg.fingerprint(),
            "edit_version": edit_version,
            "stage_version": stage_version,
            "artifact_digest": digest_json(result.get("summary", {})),
        }
        self.ws.write_snapshot(shard_id, stage, record)

    def _detect_replacement(self, shard_id: str, stage: str,
                            expected_stage_version: str) -> None:
        """Guard against out-of-band snapshot replacement.

        Normal pipeline progression recomputes a stage with a *new* stage
        version and simply overwrites the artifact/snapshot, which is allowed.
        A rejection only happens when an existing snapshot claims a different
        version than the artifact it certifies (rollback of one while the
        other advanced) or fails its own integrity digest (tamper).
        """
        snap = self.ws.read_snapshot(shard_id, stage)
        if snap is None:
            return
        result = self.ws.read_stage_result(shard_id, stage)
        if snap["stage_version"] != result["stage_version"]:
            kind = ("rollback"
                    if str(snap["stage_version"]) < str(result["stage_version"])
                    else "tamper")
            raise EStaleSnapshot(kind, f"snapshot {kind} detected",
                                 {"shard_id": shard_id, "stage": stage,
                                  "snapshot_version": snap["stage_version"],
                                  "artifact_version": result["stage_version"]})
        # Same version: the on-disk record must still be internally intact.
        self.ws.verify_snapshot(snap)

    # ---------- stage: separation ----------
    def run_separation(self, shard_id: str,
                       reason: str = SNAPSHOT_REASON_RUN) -> Dict[str, Any]:
        info = self.infos[shard_id]
        n_speakers = self._speaker_count(shard_id)
        timeline = self.ws.load_timeline(shard_id)
        stage_version = stage_fingerprint([
            STAGE_SEPARATION, info.content_fingerprint,
            self.cfg.fingerprint(), str(n_speakers),
            str(self.cfg.mask_iterations), str(self.cfg.mask_floor)])
        if self.ws.has_stage(shard_id, STAGE_SEPARATION):
            self._detect_replacement(shard_id, STAGE_SEPARATION, stage_version)
            existing = self.ws.read_stage_result(shard_id, STAGE_SEPARATION)
            if existing["stage_version"] == stage_version:
                return existing
        samples = read_wav(info.wav_path)
        separated, frame_energy, masks = separate_mixture(
            samples, n_speakers, self.cfg.frame_size, self.cfg.hop_size,
            self.cfg.mask_iterations, self.cfg.mask_floor,
            info.content_fingerprint)
        wave_path = self.ws.artifact_path(
            shard_id, STAGE_SEPARATION, "separated.i16.bin")
        with open(wave_path, "wb") as fh:
            fh.write(separated_bytes(separated))
        energy_path = self.ws.artifact_path(
            shard_id, STAGE_SEPARATION, "frame_energy.json")
        energy_q = np.round(frame_energy * 1_000_000).astype(np.int64)
        from .workspace import atomic_write_json
        atomic_write_json(energy_path, energy_q.tolist())
        embeddings = []
        seed = content_seed(info.content_fingerprint)
        for k in range(n_speakers):
            raw = embedding_bytes(self._embed(separated[k], seed, k))
            emb_path = self.ws.artifact_path(
                shard_id, STAGE_SEPARATION, f"embedding-{k}.bin")
            with open(emb_path, "wb") as fh:
                fh.write(raw)
            embeddings.append(digest_json(list(raw)))
        summary = {
            "n_speakers": n_speakers,
            "n_samples": int(info.n_samples),
            "waveform_digest": digest_json(list(separated_bytes(separated))),
            "energy_digest": digest_json(energy_q.tolist()),
            "embedding_digests": embeddings,
            "mask_iterations": self.cfg.mask_iterations,
        }
        result = {
            "shard_id": shard_id,
            "stage": STAGE_SEPARATION,
            "stage_version": stage_version,
            "content_fingerprint": info.content_fingerprint,
            "config_version": self.cfg.fingerprint(),
            "edit_version": timeline.version,
            "summary": summary,
        }
        atomic_write_json(self.ws.artifact_path(
            shard_id, STAGE_SEPARATION, "result.json"), result)
        self._snapshot(shard_id, STAGE_SEPARATION, reason, stage_version,
                       result, timeline.version)
        return result

    def _embed(self, channel_int16: np.ndarray, base_seed: int,
               channel: int) -> np.ndarray:
        from .embedding import embed_channel
        seed = (base_seed + channel * 104729) & 0x7FFFFFFF
        return embed_channel(channel_int16, self.cfg.sample_rate,
                             self.cfg.frame_size, self.cfg.hop_size,
                             self.cfg.n_mels, self.cfg.embedding_dim, seed)

    # ---------- stage: recognition ----------
    def run_recognition(self, shard_id: str,
                        reason: str = SNAPSHOT_REASON_RUN) -> Dict[str, Any]:
        if not self.ws.has_stage(shard_id, STAGE_SEPARATION):
            raise EInvalidStage("separation must run before recognition",
                                {"shard_id": shard_id})
        info = self.infos[shard_id]
        timeline = self.ws.load_timeline(shard_id)
        n_speakers = self._speaker_count(shard_id)
        sep_result = self.ws.read_stage_result(shard_id, STAGE_SEPARATION)
        stage_version = stage_fingerprint([
            STAGE_RECOGNITION, info.content_fingerprint,
            self.cfg.fingerprint(), str(n_speakers),
            sep_result["stage_version"], str(self.cfg.recognition_margin),
            str(self.cfg.overlap_energy_quantile),
            digest_json(timeline.fingerprint_payload())])
        if self.ws.has_stage(shard_id, STAGE_RECOGNITION):
            self._detect_replacement(shard_id, STAGE_RECOGNITION, stage_version)
            existing = self.ws.read_stage_result(shard_id, STAGE_RECOGNITION)
            if existing["stage_version"] == stage_version:
                return existing
        separated = self._load_separated(shard_id, n_speakers, info.n_samples)
        energy_q = np.array(self.ws.read_stage_artifact_json(
            shard_id, STAGE_SEPARATION, "frame_energy.json"), dtype=np.int64)
        frame_energy = energy_q.astype(np.float64) / 1_000_000.0
        reference = timeline.segments
        centroids, diagnostics, overlaps, stats = recognize_channels(
            separated, frame_energy, reference, self.cfg,
            info.content_fingerprint)
        centroid_record = {name: c.to_list()
                           for name, c in sorted(centroids.items())}
        summary = {
            "stats": stats,
            "overlaps": overlaps,
            "centroid_digest": digest_json(centroid_record),
            "diagnostics_digest": digest_json(diagnostics),
            "n_segments": len(timeline.segments),
            "timeline_version": timeline.version,
        }
        result = {
            "shard_id": shard_id,
            "stage": STAGE_RECOGNITION,
            "stage_version": stage_version,
            "content_fingerprint": info.content_fingerprint,
            "config_version": self.cfg.fingerprint(),
            "edit_version": timeline.version,
            "summary": summary,
            "diagnostics": diagnostics,
            "overlaps": overlaps,
            "centroids": centroid_record,
            "stats": stats,
        }
        atomic_write_json(self.ws.artifact_path(
            shard_id, STAGE_RECOGNITION, "result.json"), result)
        self._snapshot(shard_id, STAGE_RECOGNITION, reason, stage_version,
                       result, timeline.version)
        return result

    def _load_separated(self, shard_id: str, n_speakers: int,
                        n_samples: int) -> np.ndarray:
        path = self.ws.artifact_path(
            shard_id, STAGE_SEPARATION, "separated.i16.bin")
        with open(path, "rb") as fh:
            raw = fh.read()
        return np.frombuffer(raw, dtype="<i2").reshape(
            n_speakers, n_samples).copy()

    # ---------- full / incremental runs ----------
    def run_all(self, reason: str = SNAPSHOT_REASON_RUN,
                shard_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        ids = shard_ids or sorted(self.infos)
        results = []
        for shard_id in ids:
            self._check_stop()
            sep = self.run_separation(shard_id, reason)
            self._check_stop()
            rec = self.run_recognition(shard_id, reason)
            results.append({"shard_id": shard_id,
                            "separation": sep["stage_version"],
                            "recognition": rec["stage_version"]})
            self._check_stop()
        self._refresh_high_water()
        return results

    def _refresh_high_water(self) -> None:
        records = []
        for shard_id in sorted(self.infos):
            for stage in STAGE_ORDER:
                snap = self.ws.read_snapshot(shard_id, stage)
                if snap:
                    records.append({"shard_id": shard_id, "stage": stage,
                                    "stage_version": snap["stage_version"],
                                    "edit_version": snap["edit_version"],
                                    "config_version": snap["config_version"]})
        self.ws.write_high_water(records)

    def recover(self) -> Dict[str, Any]:
        """Re-validate snapshots and resume only missing/outdated stages."""
        self.ws.clear_stop()
        for snap in list(self.ws.iter_snapshots()):
            self.ws.verify_snapshot(snap)
        resumed = self.run_all(reason=SNAPSHOT_REASON_RUN)
        return {"resumed": resumed}

    # ---------- consistency gate ----------
    def consistency_report(self, shard_ids: Optional[List[str]] = None
                           ) -> Dict[str, Any]:
        """Compare current artifacts against an independent from-scratch build."""
        ids = shard_ids or sorted(self.infos)
        differences = []
        rebuilt = {}
        for shard_id in ids:
            info = self.infos[shard_id]
            timeline = self.ws.load_timeline(shard_id)
            n_speakers = self._speaker_count(shard_id)
            samples = read_wav(info.wav_path)
            sep, energy, _ = separate_mixture(
                samples, n_speakers, self.cfg.frame_size, self.cfg.hop_size,
                self.cfg.mask_iterations, self.cfg.mask_floor,
                info.content_fingerprint)
            centroids, diagnostics, overlaps, stats = recognize_channels(
                sep, energy, timeline.segments, self.cfg,
                info.content_fingerprint)
            rebuilt[shard_id] = {
                "summary": {
                    "waveform_digest": digest_json(list(separated_bytes(sep))),
                    "stats": stats,
                    "overlaps": overlaps,
                    "diagnostics_digest": digest_json(diagnostics),
                }
            }
            current_sep = self.ws.read_stage_result(
                shard_id, STAGE_SEPARATION)
            current_rec = self.ws.read_stage_result(
                shard_id, STAGE_RECOGNITION)
            if current_sep["summary"]["waveform_digest"] != \
                    rebuilt[shard_id]["summary"]["waveform_digest"]:
                differences.append({"shard_id": shard_id,
                                    "area": "separation_waveform"})
            for area, key in (("stats", "stats"), ("overlaps", "overlaps")):
                if canonical_json(current_rec[key]) != \
                        canonical_json(rebuilt[shard_id]["summary"][key]):
                    differences.append({"shard_id": shard_id, "area": area})
            if current_rec["summary"]["diagnostics_digest"] != \
                    rebuilt[shard_id]["summary"]["diagnostics_digest"]:
                differences.append({"shard_id": shard_id,
                                    "area": "diagnostics"})
        if differences:
            raise EIncrementalMismatch(
                "incremental artifacts differ from full rebuild",
                {"differences": differences})
        return {"status": "byte-identical", "shards": ids,
                "rebuilt": rebuilt}
