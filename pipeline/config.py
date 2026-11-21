"""YAML 管线配置加载与校验。"""

import copy

import yaml

from pipeline.errors import E_INVALID_CONFIG, PipelineError
from pipeline.hashing import digest_json

DEFAULT_CONFIG = {
    "config_version": 1,
    "normalize": {
        "nfkc": True,
        "fold_whitespace": True,
        "halfwidth_fullwidth_unify": True,
        "lowercase": True,
    },
    "features": {
        "hash_dimension": 256,
        "han_bigrams": True,
        "l2_norm": True,
    },
    "model": {
        "learning_rate": 0.5,
        "epochs": 30,
        "quant_bits": 16,
    },
    "inference": {
        "batch_size": 4,
        "confidence_threshold": 0.5,
    },
    "resources": {
        "max_train_samples": 100000,
        "max_inference_requests": 100000,
        "max_tokens_per_sample": 4096,
    },
}


def _deep_merge(base, override):
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path):
    with open(path, "rb") as handle:
        raw = yaml.safe_load(handle.read().decode("utf-8")) or {}
    if not isinstance(raw, dict):
        raise PipelineError(E_INVALID_CONFIG, "YAML 顶层必须是映射结构", {"path": path})
    return validate_config(_deep_merge(DEFAULT_CONFIG, raw))


def validate_config(config):
    config = copy.deepcopy(config)
    dim = config.get("features", {}).get("hash_dimension")
    if not isinstance(dim, int) or dim <= 0:
        raise PipelineError(E_INVALID_CONFIG, "features.hash_dimension 必须是正整数", {"value": dim})
    epochs = config.get("model", {}).get("epochs")
    if not isinstance(epochs, int) or epochs < 0:
        raise PipelineError(E_INVALID_CONFIG, "model.epochs 必须是非负整数", {"value": epochs})
    lr = config.get("model", {}).get("learning_rate")
    if not isinstance(lr, (int, float)) or isinstance(lr, bool):
        raise PipelineError(E_INVALID_CONFIG, "model.learning_rate 必须是数值", {"value": lr})
    threshold = config.get("inference", {}).get("confidence_threshold")
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
        raise PipelineError(E_INVALID_CONFIG, "inference.confidence_threshold 必须是数值", {"value": threshold})
    batch = config.get("inference", {}).get("batch_size")
    if not isinstance(batch, int) or batch <= 0:
        raise PipelineError(E_INVALID_CONFIG, "inference.batch_size 必须是正整数", {"value": batch})
    bits = config.get("model", {}).get("quant_bits")
    if not isinstance(bits, int) or bits not in (8, 16, 32):
        raise PipelineError(E_INVALID_CONFIG, "model.quant_bits 仅支持 8/16/32", {"value": bits})
    return config


def config_fingerprint(config):
    """配置内容指纹（规范化 JSON 的 SHA-256）。"""
    return digest_json(config)
