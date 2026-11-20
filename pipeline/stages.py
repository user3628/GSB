"""Stage implementations and input-version version fingerprints.

Stage chain: normalize -> dedup -> mix. Each stage declares which input
versions decide its validity; a mismatch invalidates the stage and every
downstream stage (propagated through the artifact digest chain).
"""

import glob
import json
import os

from .dedup import dedup_records
from .errors import E_CONFIG, E_INPUT, PipelineError
from .fsutil import (canonical_json_bytes, digest_bytes, digest_text,
                     dump_json_lines, read_json_file, sha256_hex)
from .mixer import stratified_mix
from .textnorm import normalize_text, simhash64, token_count, tokenize

STAGES = ["normalize", "dedup", "mix"]

RELEVANT_INPUTS = {
    "normalize": ["shards"],
    "dedup": ["upstream", "dedup_params"],
    "mix": ["upstream", "mix_config"],
}

REQUIRED_FIELDS = ("id", "source", "lang", "text")


def data_dir(workspace):
    return os.path.join(workspace, "data")


def shards_dir(workspace):
    return os.path.join(data_dir(workspace), "shards")


def config_path(workspace):
    return os.path.join(data_dir(workspace), "mix_config.json")


def artifacts_dir(workspace):
    return os.path.join(workspace, "artifacts")


def list_shard_paths(workspace):
    return sorted(glob.glob(os.path.join(shards_dir(workspace), "*.jsonl")))


def load_config(workspace):
    path = config_path(workspace)
    if not os.path.exists(path):
        raise PipelineError(E_CONFIG, "mix config not found: %s" % path)
    config = read_json_file(path)
    for section in ("dedup", "mix"):
        if section not in config:
            raise PipelineError(
                E_CONFIG, "mix config missing '%s' section" % section)
    return config


def compute_versions(workspace):
    """Version fingerprints of everything a stage may depend on."""
    shard_entries = []
    for path in list_shard_paths(workspace):
        with open(path, "rb") as fh:
            shard_entries.append("%s:%s" % (os.path.basename(path),
                                            sha256_hex(fh.read())))
    shards_version = digest_text("\n".join(shard_entries))
    try:
        config = load_config(workspace)
        dedup_version = digest_bytes(canonical_json_bytes(config["dedup"]))
        mix_version = digest_bytes(canonical_json_bytes(config["mix"]))
    except PipelineError:
        dedup_version = mix_version = "missing"
    return {
        "shards": shards_version,
        "dedup_params": dedup_version,
        "mix_config": mix_version,
    }


class StageResult:
    def __init__(self, primary_name, primary_bytes, extra_files=None,
                 summary=None):
        self.primary_name = primary_name
        self.primary_bytes = primary_bytes
        self.extra_files = extra_files or {}
        self.summary = summary or {}

    @property
    def digest(self):
        return digest_bytes(self.primary_bytes)


def _read_jsonl_bytes(data, source_name):
    records = []
    for lineno, line in enumerate(data.decode("utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PipelineError(
                E_INPUT, "%s:%d invalid JSON: %s" % (source_name, lineno, exc))
        missing = [f for f in REQUIRED_FIELDS if f not in obj]
        if missing:
            raise PipelineError(
                E_INPUT, "%s:%d missing fields %s" % (
                    source_name, lineno, ",".join(missing)))
        records.append(obj)
    return records


def _load_artifact_records(workspace, name):
    path = os.path.join(artifacts_dir(workspace), name)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as fh:
        return _read_jsonl_bytes(fh.read(), name)


def run_normalize(workspace):
    records = []
    order = 0
    for path in list_shard_paths(workspace):
        shard = os.path.basename(path)
        with open(path, "rb") as fh:
            for obj in _read_jsonl_bytes(fh.read(), shard):
                norm = normalize_text(str(obj["text"]))
                tokens = tokenize(norm)
                records.append({
                    "order": order,
                    "id": str(obj["id"]),
                    "source": str(obj["source"]),
                    "lang": str(obj["lang"]),
                    "text": str(obj["text"]),
                    "text_norm": norm,
                    "tokens": token_count(norm),
                    "simhash": "%016x" % simhash64(tokens),
                    "shard": shard,
                })
                order += 1
    summary = {"records": len(records),
               "shards": len(list_shard_paths(workspace))}
    return StageResult("normalize.jsonl", dump_json_lines(records),
                       summary=summary)


def run_dedup(workspace, config=None):
    config = config or load_config(workspace)
    params = config["dedup"]
    threshold = int(params.get("hamming_threshold", 3))
    bands = int(params.get("bands", 4))
    band_bits = int(params.get("band_bits", 16))
    records = _load_artifact_records(workspace, "normalize.jsonl") or []
    simhashes = [int(rec["simhash"], 16) for rec in records]
    kept, duplicates = dedup_records(simhashes, threshold=threshold,
                                     bands=bands, band_bits=band_bits)
    kept_records = [records[i] for i in kept]
    report = {
        "params": {"hamming_threshold": threshold, "bands": bands,
                   "band_bits": band_bits},
        "input_records": len(records),
        "kept_records": len(kept_records),
        "dropped_records": len(duplicates),
        "duplicates": [
            {"id": records[i]["id"], "dup_of": records[j]["id"],
             "hamming": dist}
            for i, j, dist in duplicates
        ],
    }
    extra = {"dedup.report.json": canonical_json_bytes(report)}
    summary = {"kept": len(kept_records), "dropped": len(duplicates)}
    return StageResult("dedup.jsonl", dump_json_lines(kept_records),
                       extra_files=extra, summary=summary)


def run_mix(workspace, config=None):
    config = config or load_config(workspace)
    mix_cfg = config["mix"]
    total = int(mix_cfg.get("total_samples", 0))
    seed = mix_cfg.get("seed", 0)
    weights = {str(k): float(v)
               for k, v in mix_cfg.get("weights", {}).items()}
    default_weight = float(mix_cfg.get("default_weight", 0.0))
    records = _load_artifact_records(workspace, "dedup.jsonl") or []
    selected, stats = stratified_mix(records, weights, default_weight,
                                     total, seed)
    mix_records = [{"id": rec["id"], "source": rec["source"],
                    "lang": rec["lang"], "text": rec["text"]}
                   for rec in selected]
    stats = dict(stats)
    stats["seed"] = seed
    extra = {"mix.stats.json": canonical_json_bytes(stats)}
    summary = {"selected": stats["total_selected"],
               "requested": stats["total_requested"]}
    return StageResult("mix.jsonl", dump_json_lines(mix_records),
                       extra_files=extra, summary=summary)


STAGE_FUNCS = {
    "normalize": run_normalize,
    "dedup": run_dedup,
    "mix": run_mix,
}

STAGE_PRIMARY_ARTIFACT = {
    "normalize": "normalize.jsonl",
    "dedup": "dedup.jsonl",
    "mix": "mix.jsonl",
}
