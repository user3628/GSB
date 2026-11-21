"""特征哈希、L2 归一化与词项统计。"""

import math

from pipeline.errors import E_EMPTY_SAMPLE, PipelineError
from pipeline.hashing import feature_index
from pipeline.tokenizer import text_to_tokens


def featurize_text(text, config):
    """单条文本 -> (normalized, 稀疏特征 dict{index: value}, token 计数)。

    空文本抛出 E_EMPTY_SAMPLE，由阶段层捕获并写入诊断输出。
    """
    normalized, tokens = text_to_tokens(text, config)
    if not normalized or not tokens:
        raise PipelineError(
            E_EMPTY_SAMPLE,
            "规范化后文本为空，无法生成特征",
            {"normalized": normalized},
        )
    dimension = config["features"]["hash_dimension"]
    counts = {}
    token_counts = {}
    for token in tokens:
        idx = feature_index(token, dimension)
        counts[idx] = counts.get(idx, 0) + 1
        token_counts[token] = token_counts.get(token, 0) + 1
    if config["features"].get("l2_norm", True):
        norm = math.sqrt(sum(value * value for value in counts.values()))
        features = {idx: value / norm for idx, value in counts.items()}
    else:
        features = dict(counts)
    return normalized, dict(sorted(features.items())), token_counts


def featurize_sample(sample, config):
    """带 sample_id 的特征化，返回阶段产物中的单条记录。"""
    sample_id = sample["sample_id"]
    normalized, features, token_counts = featurize_text(sample.get("text", ""), config)
    return {
        "sample_id": sample_id,
        "label": _normalize_label(sample.get("label")),
        "lang": sample.get("lang"),
        "source": sample.get("source"),
        "normalized_text": normalized,
        "features": [
            {"index": idx, "value": value} for idx, value in features.items()
        ],
        "nnz": len(features),
        "token_count": sum(token_counts.values()),
        "token_counts": dict(sorted(token_counts.items())),
    }


def _normalize_label(label):
    if isinstance(label, bool):
        return int(label)
    if isinstance(label, str):
        lowered = label.strip().lower()
        if lowered in ("1", "pos", "positive", "true", "yes", "正例"):
            return 1
        if lowered in ("0", "neg", "negative", "false", "no", "负例"):
            return 0
    return int(label)
