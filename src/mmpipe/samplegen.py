"""固定随机种子的本地确定性样本生成器。

产出：
- texts JSONL：id / text / lang
- images JSON：id / feature / text_id / operator
- generation_params.json：全部生成参数（复现依据）
- pipeline_config.json：投影矩阵、融合权重、阈值与算子置信度

不访问任何外部服务，不依赖真实模型或 GPU。
"""

import os

import numpy as np

from .codec import canonical_dumps, fingerprint
from .text_encoding import hash_embedding

SEED = 20251120
EMBED_DIM = 64
FEATURE_DIM = 64

TEXT_RECORDS = [
    {"id": "t001", "lang": "en",
     "text": "The autonomous vehicle detects a pedestrian at the crosswalk."},
    {"id": "t002", "lang": "zh",
     "text": "智能摄像头在雨夜识别出路口的交通信号灯。"},
    {"id": "t003", "lang": "zh-en",
     "text": "多模态 Multi-Modal 模型融合 image 与 text 信号。"},
    {"id": "t004", "lang": "en",
     "text": "Warehouse robot sorts parcels on the conveyor belt."},
]


def _project(matrix: np.ndarray, feature: np.ndarray) -> np.ndarray:
    projected = matrix @ feature
    return projected / np.linalg.norm(projected)


def _tune_feature(matrix: np.ndarray, target_embedding: np.ndarray,
                  rng: np.random.RandomState, target_label: str,
                  noise_scale: float) -> np.ndarray:
    """以目标嵌入为导向构造特征；conflict 时反向，保证标签可预期。"""
    direction = rng.normal(0.0, 1.0, size=FEATURE_DIM)
    if target_label == "conflict":
        direction = -direction
    pseudo_inverse = np.linalg.pinv(matrix)
    aligned = pseudo_inverse @ target_embedding
    feature = aligned + noise_scale * direction
    if target_label == "conflict":
        feature = -aligned + noise_scale * direction
    return feature / np.linalg.norm(feature)


def generate(output_dir: str, seed: int = SEED) -> dict:
    rng = np.random.RandomState(seed)
    os.makedirs(output_dir, exist_ok=True)
    matrix = rng.normal(0.0, 1.0, size=(EMBED_DIM, FEATURE_DIM))
    matrix /= np.sqrt(FEATURE_DIM)

    embeddings = {rec["id"]: hash_embedding(rec["text"], EMBED_DIM)[0]
                  for rec in TEXT_RECORDS}

    image_specs = [
        ("img001", "t001", "op_rgb", "consistent", 0.05),
        ("img002", "t002", "op_radar", "weak", 0.35),
        ("img003", "t003", "op_lidar", "consistent", 0.08),
        ("img004", "t003", "op_rgb", "conflict", 0.02),
    ]
    images: list[dict] = []
    for image_id, text_id, operator, target_label, noise in image_specs:
        feature = _tune_feature(matrix, embeddings[text_id], rng,
                                target_label, noise)
        images.append({
            "id": image_id,
            "text_id": text_id,
            "operator": operator,
            "target_label": target_label,
            "feature": [float(x) for x in feature],
        })

    config = {
        "schema_version": "mmpipe-config/v1",
        "seed": seed,
        "embedding_dim": EMBED_DIM,
        "feature_dim": FEATURE_DIM,
        "hash_algorithm": "blake2b-128-signed-feature-hashing",
        "text_normalization": ["NFKC", "whitespace-fold", "lowercase"],
        "projection_matrix": [[float(x) for x in row] for row in matrix],
        "fusion_weights": {"similarity": 0.6, "operator": 0.4},
        "fusion_thresholds": {"strong": 0.72, "weak": 0.45},
        "operator_confidence": {
            "op_rgb": 0.92,
            "op_radar": 0.78,
            "op_lidar": 0.85,
        },
        "thread_pool_workers": 4,
    }

    params = {
        "generator": "mmpipe.samplegen",
        "seed": seed,
        "numpy_random": "numpy.random.RandomState(seed).normal",
        "text_count": len(TEXT_RECORDS),
        "image_count": len(images),
        "embedding_dim": EMBED_DIM,
        "feature_dim": FEATURE_DIM,
        "image_specs": [
            {"id": s[0], "text_id": s[1], "operator": s[2],
             "target_label": s[3], "noise_scale": s[4]}
            for s in image_specs
        ],
        "languages": sorted({rec["lang"] for rec in TEXT_RECORDS}),
        "config_fingerprint": fingerprint(config),
    }

    texts_path = os.path.join(output_dir, "texts.jsonl")
    with open(texts_path, "w", encoding="utf-8") as handle:
        for rec in TEXT_RECORDS:
            handle.write(canonical_dumps(rec).decode("utf-8") + "\n")
    images_path = os.path.join(output_dir, "images.json")
    atomic = canonical_dumps(images)
    with open(images_path, "wb") as handle:
        handle.write(atomic)
    config_path = os.path.join(output_dir, "pipeline_config.json")
    with open(config_path, "wb") as handle:
        handle.write(canonical_dumps(config))
    params_path = os.path.join(output_dir, "generation_params.json")
    with open(params_path, "wb") as handle:
        handle.write(canonical_dumps(params))
    return {"texts_path": texts_path, "images_path": images_path,
            "config_path": config_path, "params_path": params_path,
            "params": params}
