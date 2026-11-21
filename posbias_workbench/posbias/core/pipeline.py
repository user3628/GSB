"""Four-stage diagnostic pipeline with stop/resume and cache reuse."""

from __future__ import annotations

import json
import os
import threading

import numpy as np

from ..errors import NumericError, StoppedError, WorkbenchError
from . import binning as binmod
from .checkpoints import (CheckpointStore, build_record, atomic_write)
from .config import AppConfig, canonical_json, content_fingerprint
from .encodings import compute_scheme_bias, pad_head_dim
from .reference import build_reference, compare, load_reference, write_reference
from .shards import list_shard_metas, load_shard

STAGES = ("config", "bias", "binning", "tolerance")


class StopFlag:
    def __init__(self):
        self._event = threading.Event()

    def request(self):
        self._event.set()

    def requested(self):
        return self._event.is_set()

    def reset(self):
        self._event.clear()


class Pipeline:
    def __init__(self, config: AppConfig, work_dir, *,
                 shard_dir=None, reference_path=None, stop_flag=None):
        self.config = config
        self.work_dir = work_dir
        self.shard_dir = shard_dir or os.path.join(work_dir, "shards")
        self.reference_path = reference_path or os.path.join(
            work_dir, "reference", "reference_metrics.yaml")
        self.artifact_dir = os.path.join(work_dir, "artifacts")
        self.checkpoint_dir = os.path.join(work_dir, "checkpoints")
        os.makedirs(self.artifact_dir, exist_ok=True)
        self.store = CheckpointStore(self.checkpoint_dir)
        self.stop_flag = stop_flag or StopFlag()
        self.metas = list_shard_metas(self.shard_dir)

    # ------------------------------------------------------------- helpers
    def _check_stop(self):
        if self.stop_flag.requested():
            raise StoppedError(
                "stop requested after completing the current atomic unit",
                stage="control")

    def _shard_fingerprint(self):
        return content_fingerprint([
            {"meta": meta,
             "q_sha256": meta["sha256"]["q"],
             "k_sha256": meta["sha256"]["k"]}
            for meta in self.metas])

    def _next_sequence(self):
        return self.store.next_sequence()

    # ----------------------------------------------------------- stage 1/2
    def run_config_stage(self):
        self.config.validate()
        self.config.check_sequence_length(
            max(meta["seq_len"] for meta in self.metas))
        fingerprint = self.config.fingerprint()
        record = self.store.load("config")
        if self.store.reusable("config", fingerprint):
            return record, True
        self.store.invalidate_from("config")
        record = build_record(
            "config", sequence=self._next_sequence(),
            input_fingerprint=self._shard_fingerprint(),
            config_version=self.config.version,
            artifact_summary={
                "config_fingerprint": fingerprint,
                "scheme_ids": [s["id"] for s in self.config.schemes],
                "bin_ids": [b["id"] for b in self.config.bins],
                "tolerance_keys": sorted(self.config.tolerance),
            })
        self.store.publish("config", record)
        return record, False

    def run_bias_stage(self, config_record):
        input_fp = content_fingerprint({
            "config": config_record["fingerprint"],
            "shards": self._shard_fingerprint(),
        })
        if self.store.reusable("bias", input_fp):
            return self.store.load("bias"), True
        self.store.invalidate_from("bias")
        padding = None
        scheme_summaries = []
        for scheme in self.config.schemes:
            per_shard = []
            for meta in self.metas:
                self._check_stop()
                try:
                    shard = load_shard(self.shard_dir,
                                       meta["shard_index"])
                except NumericError as exc:
                    exc.scheme = scheme["id"]
                    exc.context["shard_index"] = meta["shard_index"]
                    raise
                padded_dim, padding = pad_head_dim(
                    self.config.head_dim, rule=self.config.padding_rule)
                positions = np.arange(meta["seq_len"], dtype=np.float64)
                quantized, metadata = compute_scheme_bias(
                    query=shard["query"], key=shard["key"],
                    scheme=scheme, padding_info=padding, positions=positions)
                q_path = os.path.join(
                    self.artifact_dir,
                    f"bias_{scheme['id']}_shard_{meta['shard_index']:02d}.npy")
                np.save(q_path, quantized.qvalues)
                meta_path = os.path.join(
                    self.artifact_dir,
                    f"bias_{scheme['id']}_shard_{meta['shard_index']:02d}.json")
                atomic_write(meta_path, {
                    **metadata,
                    "shard_index": meta["shard_index"],
                    "q_path": os.path.basename(q_path),
                })
                dequant = quantized.dequantize()
                per_shard.append({
                    "shard_index": meta["shard_index"],
                    "mean_bias": float(np.mean(dequant)),
                    "variance": float(np.var(dequant, ddof=0)),
                    "bias_peak": float(np.max(np.abs(dequant))),
                    "quant_scale": quantized.scale,
                    "q_path": os.path.basename(q_path),
                    "meta_path": os.path.basename(meta_path),
                })
            scheme_summaries.append({
                "scheme": scheme["id"],
                "type": scheme["type"],
                "shards": per_shard,
                "overall_bias_peak": float(max(
                    item["bias_peak"] for item in per_shard)),
            })
        record = build_record(
            "bias", sequence=self._next_sequence(),
            input_fingerprint=input_fp,
            config_version=self.config.version,
            artifact_summary={
                "padding": padding,
                "schemes": scheme_summaries,
            })
        self.store.publish("bias", record)
        return record, False

    # ------------------------------------------------------------- stage 3
    def run_binning_stage(self, bias_record):
        input_fp = content_fingerprint({
            "bias": bias_record["fingerprint"],
            "bins": self.config.bins,
            "train_length": self.config.train_length,
        })
        if self.store.reusable("binning", input_fp):
            return self.store.load("binning"), True
        self.store.invalidate_from("binning")
        scheme_outputs = []
        for scheme in self.config.schemes:
            self._check_stop()
            per_shard_records = []
            for meta in self.metas:
                q_path = os.path.join(
                    self.artifact_dir,
                    f"bias_{scheme['id']}_shard_{meta['shard_index']:02d}.npy")
                scale = self._quant_scale(bias_record, scheme["id"],
                                          meta["shard_index"])
                bias_float = np.load(q_path).astype(np.float64) * scale
                per_shard_records.append(binmod.bin_statistics(
                    bias_float, train_length=self.config.train_length,
                    bins=self.config.bins))
            merged = binmod.aggregate_shard_records(
                per_shard_records, self.config.bins)
            rates = binmod.change_rates(merged)
            for record, rate in zip(merged, rates):
                record["change_rate"] = rate
            scheme_outputs.append({
                "scheme": scheme["id"],
                "type": scheme["type"],
                "bins": merged,
                "monotonic_scheme_break": any(
                    record["monotonic_break"] for record in merged),
            })
        record = build_record(
            "binning", sequence=self._next_sequence(),
            input_fingerprint=input_fp,
            config_version=self.config.version,
            artifact_summary={"schemes": scheme_outputs})
        self.store.publish("binning", record)
        return record, False

    def _quant_scale(self, bias_record, scheme_id, shard_index):
        for scheme in bias_record["artifact_summary"]["schemes"]:
            if scheme["scheme"] != scheme_id:
                continue
            for item in scheme["shards"]:
                if item["shard_index"] == shard_index:
                    return float(item["quant_scale"])
        raise KeyError(f"missing quant scale for {scheme_id}/{shard_index}")

    # ------------------------------------------------------------- stage 4
    def run_tolerance_stage(self, binning_record):
        reference = load_reference(self.reference_path)
        input_fp = content_fingerprint({
            "binning": binning_record["fingerprint"],
            "reference": reference,
            "tolerance": self.config.tolerance,
        })
        if self.store.reusable("tolerance", input_fp):
            return self.store.load("tolerance"), True
        self.store.invalidate_from("tolerance")
        report = self._assemble_report(binning_record)
        comparison = compare(report, reference, self.config.tolerance)
        record = build_record(
            "tolerance", sequence=self._next_sequence(),
            input_fingerprint=input_fp,
            config_version=self.config.version,
            artifact_summary=comparison)
        self.store.publish("tolerance", record)
        return record, False

    def _assemble_report(self, binning_record):
        bias_record = self.store.load("bias")
        schemes = []
        for bin_scheme in binning_record["artifact_summary"]["schemes"]:
            bias_scheme = next(
                item for item in
                bias_record["artifact_summary"]["schemes"]
                if item["scheme"] == bin_scheme["scheme"])
            schemes.append({
                "scheme": bin_scheme["scheme"],
                "type": bin_scheme["type"],
                "overall_bias_peak": bias_scheme["overall_bias_peak"],
                "bins": bin_scheme["bins"],
                "monotonic_scheme_break":
                    bin_scheme["monotonic_scheme_break"],
            })
        return {
            "config_fingerprint": self.config.fingerprint(),
            "config_version": self.config.version,
            "train_length": self.config.train_length,
            "padding": bias_record["artifact_summary"]["padding"],
            "shard_count": len(self.metas),
            "schemes": schemes,
        }

    # ------------------------------------------------------------ entrypoint
    def run(self, *, write_report=True, report_path=None):
        reuse_flags = {}
        config_record, reuse_flags["config"] = self.run_config_stage()
        bias_record, reuse_flags["bias"] = self.run_bias_stage(config_record)
        binning_record, reuse_flags["binning"] = \
            self.run_binning_stage(bias_record)
        tolerance_record, reuse_flags["tolerance"] = \
            self.run_tolerance_stage(binning_record)
        report = self._assemble_report(binning_record)
        report["tolerance"] = tolerance_record["artifact_summary"]
        report["checkpoint_sequences"] = {
            stage: self.store.load(stage)["sequence"] for stage in STAGES}
        report["reused_stages"] = [stage for stage, reused in reuse_flags.items()
                                   if reused]
        if write_report:
            target = report_path or os.path.join(
                self.work_dir, "reports", "diagnostic_report.json")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            atomic_write(target, report)
        return report

    def ensure_reference(self, *, reference_path=None):
        """Generate the golden reference from a clean full run."""
        config_record, _ = self.run_config_stage()
        bias_record, _ = self.run_bias_stage(config_record)
        binning_record, _ = self.run_binning_stage(bias_record)
        report = self._assemble_report(binning_record)
        target = reference_path or self.reference_path
        write_reference(target, build_reference(report))
        return target
