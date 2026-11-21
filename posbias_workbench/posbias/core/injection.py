"""Deterministic fault injection entry points."""

from __future__ import annotations

import copy
import os

from ..errors import WorkbenchError
from .config import AppConfig
from .pipeline import Pipeline, StopFlag
from .shards import write_shard

INJECTION_KINDS = (
    "seq_too_long",
    "invalid_scale",
    "zero_bins",
    "nan_shard",
    "zero_base_freq",
    "zero_quant_scale",
)


def run_injection(*, kind, config: AppConfig, work_dir,
                  shard_shape=None, seed=None):
    """Run a pipeline with one injected fault; return a structured outcome."""
    data = copy.deepcopy(config.data)
    seed = seed if seed is not None else config.seed
    shape = shard_shape or {"batch": 2, "heads": 4, "seq_len": 12,
                            "head_dim": config.head_dim}
    inject_dir = os.path.join(work_dir, f"inject_{kind}")
    shard_dir = os.path.join(inject_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)
    try:
        if kind == "seq_too_long":
            shape["seq_len"] = data["limits"]["max_seq_len"] + 8
            _write_clean_shards(seed, shard_dir, shape)
        elif kind == "invalid_scale":
            for scheme in data["schemes"]:
                if scheme["type"] != "alibi":
                    scheme["scale"] = -0.5
            _write_clean_shards(seed, shard_dir, shape)
        elif kind == "zero_bins":
            data["bins"] = []
            _write_clean_shards(seed, shard_dir, shape)
        elif kind == "nan_shard":
            write_shard(shard_dir, seed=seed, shard_index=0,
                        batch=shape["batch"], heads=shape["heads"],
                        seq_len=shape["seq_len"], head_dim=shape["head_dim"],
                        inject_nan=True)
        elif kind == "zero_base_freq":
            for scheme in data["schemes"]:
                if scheme["type"] != "alibi":
                    scheme["base_freq"] = 0.0
            _write_clean_shards(seed, shard_dir, shape)
        elif kind == "zero_quant_scale":
            # A perfectly constant bias forces a degenerate scale factor;
            # the numeric guard rejects a non-positive scale explicitly.
            write_shard(shard_dir, seed=seed, shard_index=0,
                        batch=shape["batch"], heads=shape["heads"],
                        seq_len=shape["seq_len"], head_dim=shape["head_dim"])
        else:
            raise ValueError(f"unknown injection kind: {kind}")

        injected_cfg = AppConfig(data)
        pipeline = Pipeline(
            injected_cfg, inject_dir, shard_dir=shard_dir,
            reference_path=os.path.join(work_dir, "reference",
                                        "reference_metrics.yaml"),
            stop_flag=StopFlag())
        if kind == "zero_quant_scale":
            return _force_zero_quant_scale(pipeline, injected_cfg, shard_dir)
        report = pipeline.run(write_report=False)
        return {"kind": kind, "triggered": False, "report_scheme_count":
                len(report["schemes"])}
    except WorkbenchError as exc:
        return {"kind": kind, "triggered": True, "error": exc.to_dict()}


def _write_clean_shards(seed, shard_dir, shape):
    for idx in range(3):
        write_shard(shard_dir, seed=seed, shard_index=idx,
                    batch=shape["batch"], heads=shape["heads"],
                    seq_len=shape["seq_len"], head_dim=shape["head_dim"])


def _force_zero_quant_scale(pipeline, config, shard_dir):
    from .checkpoints import build_record
    from .encodings import pad_head_dim
    from .numeric import QuantizedBias
    import numpy as np

    config_record, _ = pipeline.run_config_stage()
    scheme = config.schemes[0]
    meta = pipeline.metas[0]
    shard = __import__(
        "posbias.core.shards", fromlist=["load_shard"]).load_shard(
            shard_dir, meta["shard_index"])
    padded_dim, padding = pad_head_dim(
        config.head_dim, rule=config.padding_rule)
    try:
        QuantizedBias(np.zeros((meta["seq_len"], meta["seq_len"]),
                               dtype=np.int32), 0.0, scheme=scheme["id"])
    except WorkbenchError as exc:
        return {"kind": "zero_quant_scale", "triggered": True,
                "error": exc.to_dict()}
    raise AssertionError("zero quant scale guard did not trigger")
