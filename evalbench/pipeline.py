"""Pipeline orchestration: parse -> calibrate -> diagnose per shard.

Each stage's checkpoint carries a record content fingerprint and a config
version fingerprint. On a run, the earliest stage whose expected inputs
no longer match its stored fingerprints is identified; that stage and all
downstream calibration/diagnostic artifacts are invalidated and rebuilt,
while unaffected shards keep reusing their upstream parse results. The
invalidation reason and fingerprints are observable through status().

Graceful stop: when the stop event is set, the current indivisible unit
(one stage of one shard) finishes, then the run exits. A later resume
reuses valid checkpoints, so the final reliability table, calibration
parameters and summary are byte-identical to an uninterrupted run.
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import numpy as np

from . import codec
from .binning import assign_buckets
from .calibrate import apply_calibrator, fit_calibrator
from .checkpoint import CheckpointStore, STAGE_ORDER
from .config import CalibrationConfig, load_config
from .diagnostics import diagnose
from .errors import ShardNotFoundError
from .fingerprint import fingerprint_of_file


def load_shard_records(path: Path, cfg: CalibrationConfig, shard_id: str):
    """Decode a shard file; returns (records, provenance, roundtrip report)."""
    records = []
    conversions = []
    chains_ok = 0
    with open(path, "r", encoding="utf-8") as fh:
        lines = [ln for ln in fh.read().splitlines() if ln.strip()]
    for line_no, line in enumerate(lines, start=1):
        obj = json.loads(line)
        version = codec.detect_declared_version(obj, shard_id=shard_id, line_no=line_no)
        if version == codec.SCHEMA_V2:
            codec.roundtrip_chain_from_v2(
                obj, shard_id=shard_id, line_no=line_no, n_bins=cfg.n_bins,
                rounding=cfg.rounding, id_delimiter=cfg.id_delimiter)
        else:
            codec.roundtrip_chain_from_v1(
                obj, shard_id=shard_id, line_no=line_no, n_bins=cfg.n_bins,
                rounding=cfg.rounding, id_delimiter=cfg.id_delimiter)
        chains_ok += 1
        rec, evidence, _ = codec.decode_line(
            line, shard_id=shard_id, line_no=line_no, n_bins=cfg.n_bins,
            rounding=cfg.rounding, id_delimiter=cfg.id_delimiter)
        records.append(rec)
        if evidence is not None:
            conversions.append(evidence)
    provenance = {
        "rounding_rule": cfg.rounding,
        "id_delimiter": cfg.id_delimiter,
        "id_split_rule": "split last segment on delimiter; prefix '<model><delim><batch>' verified",
        "bucket_fill_rule": f"equal_width(n_bins={cfg.n_bins}), half-open [lo,hi), 1.0->last bucket",
        "pct_to_score_rule": "score = score_pct / 100 (exact)",
        "conversions": conversions,
    }
    roundtrip = {"chains_verified": chains_ok, "ok": True}
    return records, provenance, roundtrip


def _stage_valid(store: CheckpointStore, shard_id: str, stage: str,
                 expected_record_fp: str, expected_config_fp: str,
                 expected_config_view: dict) -> tuple[bool, list[str]]:
    rec = store.get(shard_id, stage)
    if rec is None:
        return False, [f"stage '{stage}' artifact missing"]
    reasons = []
    if rec["record_fingerprint"] != expected_record_fp:
        reasons.append(
            "record content fingerprint changed "
            f"(stored {rec['record_fingerprint'][:12]}... != expected {expected_record_fp[:12]}...)")
    if rec["config_fingerprint"] != expected_config_fp:
        stored_view = rec.get("config_view", {})
        changed = [k for k in expected_config_view
                   if stored_view.get(k) != expected_config_view.get(k)]
        reasons.append(
            "config version fingerprint changed"
            + (f" (fields: {', '.join(changed)})" if changed else ""))
    return (not reasons), reasons


class Pipeline:
    def __init__(self, workspace: str | Path, config: CalibrationConfig):
        self.cfg = config
        self.store = CheckpointStore(workspace)
        self.workspace = Path(workspace)

    # ---------------------------------------------------------- stages
    def _run_parse(self, shard_id: str, path: Path) -> dict:
        records, provenance, roundtrip = load_shard_records(path, self.cfg, shard_id)
        return {
            "records": [r.to_dict() for r in records],
            "provenance": provenance,
            "roundtrip": roundtrip,
            "n_records": len(records),
        }

    def _run_calibrate(self, parse_artifact: dict) -> dict:
        from .models import EvalRecord
        records = parse_artifact["records"]
        scores = np.array([r["score"] for r in records], dtype=float)
        labels = np.array([r["label"] for r in records], dtype=float)
        buckets = assign_buckets([EvalRecord.from_dict(r) for r in records],
                                 self.cfg.n_bins, self.cfg.binning)
        model = fit_calibrator(scores, labels, self.cfg.method)
        calibrated = apply_calibrator(model, scores)
        return {
            "calibrator": model,
            "buckets": [int(b) for b in buckets],
            "calibrated_scores": [float(x) for x in calibrated],
            "n_records": len(records),
        }

    def _run_diagnose(self, parse_artifact: dict, calibrate_artifact: dict) -> dict:
        from .models import EvalRecord
        records = [EvalRecord.from_dict(r) for r in parse_artifact["records"]]
        labels = np.array([r.label for r in records], dtype=float)
        raw = np.array([r.score for r in records], dtype=float)
        calibrated = np.array(calibrate_artifact["calibrated_scores"], dtype=float)
        buckets = np.array(calibrate_artifact["buckets"], dtype=int)
        return diagnose(records, labels, raw, calibrated, buckets,
                        self.cfg.n_bins, self.cfg.reliability_min_samples,
                        self.cfg.expected_positive_rate)

    # ------------------------------------------------------------ run
    def run(self, data_dir: str | Path, *,
            stop_event: threading.Event | None = None,
            unit_delay_ms: int = 0,
            shard_ids: list[str] | None = None) -> dict:
        data_dir = Path(data_dir)
        shard_paths = self._discover_shards(data_dir, shard_ids)
        report = {"status": "completed", "shards": {}, "workspace": self.workspace.name}
        for shard_id, path in shard_paths:
            shard_report = self._run_shard(shard_id, path, stop_event, unit_delay_ms)
            report["shards"][shard_id] = shard_report
            if shard_report.get("stopped"):
                report["status"] = "stopped"
                return report
        self._export(report)
        return report

    def _discover_shards(self, data_dir: Path, shard_ids: list[str] | None):
        if shard_ids is not None:
            out = []
            for shard_id in shard_ids:
                path = data_dir / f"{shard_id}.jsonl"
                if not path.is_file():
                    raise ShardNotFoundError("shard file not found",
                                             shard_id=shard_id, path=str(path))
                out.append((shard_id, path))
            return out
        return sorted((p.stem, p) for p in data_dir.glob("*.jsonl"))

    def _run_shard(self, shard_id: str, path: Path,
                   stop_event: threading.Event | None, unit_delay_ms: int) -> dict:
        cfg = self.cfg
        shard_fp = fingerprint_of_file(path)
        shard_report: dict = {"stages": {}, "invalidations": []}

        # expected fingerprints chaining upstream artifact digests
        parse_ckpt = self.store.get(shard_id, "parse")
        parse_digest = parse_ckpt["artifact_digest"] if parse_ckpt else None
        cal_ckpt = self.store.get(shard_id, "calibrate")
        cal_digest = cal_ckpt["artifact_digest"] if cal_ckpt else None

        expectations = {
            "parse": (shard_fp, cfg.stage_fingerprint("parse")),
            "calibrate": (parse_digest, cfg.stage_fingerprint("calibrate")),
            "diagnose": (cal_digest, cfg.stage_fingerprint("diagnose")),
        }

        invalidated_from = None
        invalidation_reasons: list[str] = []
        for stage in STAGE_ORDER:
            exp_rec, exp_cfg = expectations[stage]
            if exp_rec is None:
                valid, reasons = False, [f"upstream stage artifact missing for '{stage}'"]
            else:
                valid, reasons = _stage_valid(self.store, shard_id, stage, exp_rec,
                                              exp_cfg, cfg.stage_view(stage))
            if not valid and invalidated_from is None:
                invalidated_from = stage
                invalidation_reasons = reasons
            expectations[stage] = (exp_rec, exp_cfg)

        if invalidated_from is not None:
            # withdraw the invalidated stage and all downstream artifacts
            # under the file lock before rebuilding them.
            self.store.invalidate_from(shard_id, invalidated_from)

        for stage in STAGE_ORDER:
            if stop_event is not None and stop_event.is_set():
                shard_report["stopped"] = True
                return shard_report
            if invalidated_from is None:
                shard_report["stages"][stage] = "reused"
                continue
            if STAGE_ORDER.index(stage) < STAGE_ORDER.index(invalidated_from):
                shard_report["stages"][stage] = "reused"
                continue
            # rebuild this stage
            if stage == "parse":
                artifact = self._run_parse(shard_id, path)
                record_fp, config_fp = shard_fp, cfg.stage_fingerprint("parse")
            elif stage == "calibrate":
                parse_art = self.store.get(shard_id, "parse")["artifact"]
                artifact = self._run_calibrate(parse_art)
                record_fp = self.store.get(shard_id, "parse")["artifact_digest"]
                config_fp = cfg.stage_fingerprint("calibrate")
            else:
                parse_art = self.store.get(shard_id, "parse")["artifact"]
                cal_art = self.store.get(shard_id, "calibrate")["artifact"]
                artifact = self._run_diagnose(parse_art, cal_art)
                record_fp = self.store.get(shard_id, "calibrate")["artifact_digest"]
                config_fp = cfg.stage_fingerprint("diagnose")
            self.store.publish(
                shard_id, stage,
                record_fingerprint=record_fp,
                config_fingerprint=config_fp,
                artifact=artifact,
                extra={"config_view": cfg.stage_view(stage)},
            )
            shard_report["stages"][stage] = "rebuilt"
            if unit_delay_ms:
                time.sleep(unit_delay_ms / 1000.0)

        if invalidated_from is not None:
            shard_report["invalidations"].append({
                "earliest_stage": invalidated_from,
                "reasons": invalidation_reasons,
                "downstream_rebuilt": [s for s in STAGE_ORDER
                                       if STAGE_ORDER.index(s) >= STAGE_ORDER.index(invalidated_from)],
            })
        return shard_report

    # ---------------------------------------------------------- exports
    def _export(self, report: dict) -> None:
        from .checkpoint import _atomic_write_json
        export_dir = self.workspace / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        shards = {}
        all_records = []
        all_calibrated = []
        all_buckets = []
        for shard_id in self.store.shard_ids():
            parse_rec = self.store.get(shard_id, "parse")
            cal_rec = self.store.get(shard_id, "calibrate")
            diag_rec = self.store.get(shard_id, "diagnose")
            if not (parse_rec and cal_rec and diag_rec):
                continue
            shards[shard_id] = {
                "calibrator": cal_rec["artifact"]["calibrator"],
                "diagnostics": diag_rec["artifact"],
                "provenance": parse_rec["artifact"]["provenance"],
                "roundtrip": parse_rec["artifact"]["roundtrip"],
            }
            from .models import EvalRecord
            recs = [EvalRecord.from_dict(r) for r in parse_rec["artifact"]["records"]]
            all_records.extend(recs)
            all_calibrated.extend(cal_rec["artifact"]["calibrated_scores"])
            all_buckets.extend(cal_rec["artifact"]["buckets"])
        summary = None
        if all_records:
            labels = np.array([r.label for r in all_records], dtype=float)
            raw = np.array([r.score for r in all_records], dtype=float)
            calibrated = np.array(all_calibrated, dtype=float)
            buckets = np.array(all_buckets, dtype=int)
            summary = diagnose(all_records, labels, raw, calibrated, buckets,
                               self.cfg.n_bins, self.cfg.reliability_min_samples,
                               self.cfg.expected_positive_rate)
        payload = {
            "config": self.cfg.to_v2_dict(),
            "config_upgrade_notes": self.cfg.upgrade_notes_dicts(),
            "shards": shards,
            "summary": summary,
        }
        _atomic_write_json(export_dir / "reliability_report.json", payload)
        report["export"] = str(export_dir / "reliability_report.json")

    # ---------------------------------------------------------- status
    def status(self, data_dir: str | Path | None = None) -> dict:
        out = {"workspace": self.workspace.name, "shards": {}}
        shard_ids = set(self.store.shard_ids())
        if data_dir is not None:
            for _sid, _p in self._discover_shards(Path(data_dir), None):
                shard_ids.add(_sid)
        for shard_id in sorted(shard_ids):
            stages = {}
            upstream_digest = None
            if data_dir is not None:
                path = Path(data_dir) / f"{shard_id}.jsonl"
                upstream_digest = fingerprint_of_file(path) if path.is_file() else None
            for stage in STAGE_ORDER:
                rec = self.store.get(shard_id, stage)
                exp_cfg = self.cfg.stage_fingerprint(stage)
                entry = {"expected_config_fingerprint": exp_cfg}
                if rec is None:
                    entry["state"] = "missing"
                else:
                    entry.update({
                        "seq": rec["seq"],
                        "stored_record_fingerprint": rec["record_fingerprint"],
                        "stored_config_fingerprint": rec["config_fingerprint"],
                        "artifact_digest": rec["artifact_digest"],
                    })
                    reasons = []
                    if upstream_digest is not None and rec["record_fingerprint"] != upstream_digest:
                        reasons.append("upstream content fingerprint mismatch")
                    if rec["config_fingerprint"] != exp_cfg:
                        stored_view = rec.get("config_view", {})
                        changed = [k for k in self.cfg.stage_view(stage)
                                   if stored_view.get(k) != self.cfg.stage_view(stage)[k]]
                        reasons.append("config fingerprint mismatch"
                                       + (f" (fields: {', '.join(changed)})" if changed else ""))
                    entry["state"] = "invalid" if reasons else "valid"
                    entry["invalidation_reasons"] = reasons
                stages[stage] = entry
                if rec is not None:
                    upstream_digest = rec["artifact_digest"]
                else:
                    upstream_digest = None
            out["shards"][shard_id] = stages
        return out


def run_pipeline(workspace: str | Path, data_dir: str | Path, config_path: str | Path,
                 **kwargs) -> dict:
    cfg = load_config(str(config_path))
    return Pipeline(workspace, cfg).run(data_dir, **kwargs)
