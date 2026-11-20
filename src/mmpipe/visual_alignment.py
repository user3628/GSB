"""前端阶段 2：视觉特征对齐。

图像特征向量 v 经可配置投影矩阵 W 映射到文本嵌入空间：
    p = W @ v （W: embed_dim x feature_dim）
再与对应文本嵌入 e 计算余弦相似度：
    cos = dot(p, e) / (|p| * |e|)

校验：维度不匹配 -> E_SHAPE_MISMATCH；引用文本缺失 -> E_MISSING_INPUT；
出现 NaN/Inf -> E_NUMERIC。
"""

import numpy as np

from .codec import fingerprint
from .errors import (
    E_MISSING_INPUT,
    E_NUMERIC,
    E_SHAPE_MISMATCH,
    PipeError,
    StageFailure,
)


def image_input_fingerprint(record: dict) -> str:
    return fingerprint({
        "id": record.get("id"),
        "feature": record.get("feature"),
        "text_id": record.get("text_id"),
        "operator": record.get("operator"),
    })


def validate_matrix(config: dict) -> np.ndarray:
    embed_dim = int(config["embedding_dim"])
    matrix = np.asarray(config["projection_matrix"], dtype=np.float64)
    if matrix.ndim != 2 or matrix.shape[0] != embed_dim:
        raise PipeError(E_SHAPE_MISMATCH,
                        f"projection matrix shape {matrix.shape} incompatible "
                        f"with embedding_dim={embed_dim}",
                        stage="visual_alignment")
    if not np.all(np.isfinite(matrix)):
        raise PipeError(E_NUMERIC, "projection matrix contains NaN/Inf",
                        stage="visual_alignment")
    return matrix


def _cosine(projected: np.ndarray, embedding: np.ndarray, sample_id: str) -> float:
    if not np.all(np.isfinite(projected)):
        raise PipeError(E_NUMERIC, "projected vector contains NaN/Inf",
                        stage="visual_alignment", sample_id=sample_id)
    p_norm = float(np.linalg.norm(projected))
    e_norm = float(np.linalg.norm(embedding))
    if p_norm == 0.0 or e_norm == 0.0 or not np.isfinite(p_norm) \
            or not np.isfinite(e_norm):
        raise PipeError(E_NUMERIC,
                        f"zero/non-finite norm (projected={p_norm!r}, text={e_norm!r})",
                        stage="visual_alignment", sample_id=sample_id)
    cos = float(np.dot(projected, embedding) / (p_norm * e_norm))
    if not np.isfinite(cos):
        raise PipeError(E_NUMERIC, f"cosine similarity is non-finite: {cos!r}",
                        stage="visual_alignment", sample_id=sample_id)
    return max(-1.0, min(1.0, cos))


def align_unit(record: dict, text_product: dict, config: dict) -> dict:
    """不可拆分样本单元：单条图像描述 -> 投影与相似度记录。"""
    sample_id = str(record.get("id")) if record.get("id") is not None else "<unknown>"
    matrix = validate_matrix(config)
    feature_dim = matrix.shape[1]
    feature = record.get("feature")
    if feature is None:
        raise PipeError(E_MISSING_INPUT, "feature vector is missing",
                        stage="visual_alignment", sample_id=sample_id)
    vec = np.asarray(feature, dtype=np.float64)
    if vec.ndim != 1 or vec.shape[0] != feature_dim:
        raise PipeError(
            E_SHAPE_MISMATCH,
            f"feature dim {vec.shape[0]} != projection input dim {feature_dim}",
            stage="visual_alignment", sample_id=sample_id)
    if not np.all(np.isfinite(vec)):
        raise PipeError(E_NUMERIC, "feature vector contains NaN/Inf",
                        stage="visual_alignment", sample_id=sample_id)
    text_id = record.get("text_id")
    embeddings = text_product["embeddings"]
    if text_id is None or str(text_id) not in embeddings:
        raise PipeError(E_MISSING_INPUT,
                        f"referenced text_id {text_id!r} does not exist",
                        stage="visual_alignment", sample_id=sample_id)
    projected = matrix @ vec
    similarity = _cosine(projected,
                         np.asarray(embeddings[str(text_id)], dtype=np.float64),
                         sample_id)
    return {
        "id": sample_id,
        "text_id": str(text_id),
        "operator": record.get("operator"),
        "similarity": similarity,
        "projected": [float(x) for x in projected],
        "input_fingerprint": image_input_fingerprint(record),
    }


def build_product(items: list[dict], config: dict) -> dict:
    return {
        "stage": "visual_alignment",
        "items": items,
        "summary": {
            "record_count": len(items),
            "feature_dim": len(config["projection_matrix"][0]),
            "projection_fingerprint": fingerprint(config["projection_matrix"]),
            "item_fingerprints": sorted(
                (it["id"], it["input_fingerprint"]) for it in items),
        },
    }


def run(records: list[dict], text_product: dict, config: dict) -> dict:
    validate_matrix(config)
    items: list[dict] = []
    errors: list[dict] = []
    seen: set[str] = set()
    for record in sorted(records, key=lambda r: str(r.get("id"))):
        sample_id = str(record.get("id")) if record.get("id") is not None else "<unknown>"
        if sample_id in seen:
            errors.append(PipeError(E_MISSING_INPUT,
                                    f"duplicate image id {sample_id!r}",
                                    stage="visual_alignment",
                                    sample_id=sample_id).to_dict())
            continue
        seen.add(sample_id)
        try:
            items.append(align_unit(record, text_product, config))
        except PipeError as exc:
            errors.append(exc.to_dict())
    if errors:
        raise StageFailure("visual_alignment", errors)
    return build_product(items, config)
