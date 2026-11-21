"""固定随机种子的确定性样本生成器。

生成 3 个训练 JSONL 分片与 2 个推理 JSON 分片，覆盖中文、英文与中英混排，
并保证同一 seed 逐字节复现。推理分片引用的模型版本号由管线指纹规则推导，
保证与一次全量训练得到的当前有效模型版本一致。
"""

import os
import random

from pipeline.config import DEFAULT_CONFIG, validate_config
from pipeline.shards import write_json_array, write_jsonl
from pipeline.stages import compute_model_version, stage2_input_fingerprint

POS_WORDS_ZH = ["优惠", "便宜", "喜欢", "推荐", "好用", "开心", "划算", "满意"]
NEG_WORDS_ZH = ["失望", "糟糕", "退货", "难用", "投诉", "生气", "破损", "垃圾"]
POS_WORDS_EN = ["good", "great", "love", "recommend", "cheap", "nice", "happy", "excellent"]
NEG_WORDS_EN = ["bad", "terrible", "hate", "broken", "angry", "poor", "awful", "refund"]
NEUTRAL_ZH = ["包装", "快递", "商品", "订单", "客服"]
NEUTRAL_EN = ["package", "delivery", "product", "order", "service"]


def _build_text(rng, label):
    if label == 1:
        zh, en = POS_WORDS_ZH, POS_WORDS_EN
    else:
        zh, en = NEG_WORDS_ZH, NEG_WORDS_EN
    # 同一条文本重复 3 次类别词，配合 L2 归一化形成更强的类别信号。
    pattern = rng.randrange(4)
    if pattern == 0:
        return (
            rng.choice(NEUTRAL_ZH)
            + rng.choice(zh)
            + rng.choice(zh)
            + rng.choice(zh)
        )
    if pattern == 1:
        return " ".join(
            [rng.choice(NEUTRAL_EN), rng.choice(en), rng.choice(en), rng.choice(en)]
        )
    if pattern == 2:
        return (
            rng.choice(NEUTRAL_ZH)
            + rng.choice(zh)
            + " "
            + rng.choice(en)
            + " "
            + rng.choice(en)
        )
    return " ".join(
        [rng.choice(NEUTRAL_EN), rng.choice(en), rng.choice(en)]
    ) + rng.choice(zh)


def generate_training_shards(train_dir, seed=20261122, shard_count=3, per_shard=12):
    rng = random.Random(seed)
    os.makedirs(train_dir, exist_ok=True)
    paths = []
    sample_id = 0
    for shard_index in range(shard_count):
        records = []
        for _ in range(per_shard):
            label = rng.randrange(2)
            text = _build_text(rng, label)
            records.append(
                {
                    "sample_id": f"train-{sample_id:04d}",
                    "text": text,
                    "label": label,
                    "lang": "zh" if rng.random() < 0.5 else "en",
                    "source": f"shard-{shard_index + 1}",
                }
            )
            sample_id += 1
        # 每个训练分片固定包含一条空样本，用于 E_EMPTY_SAMPLE 诊断演示。
        records.append(
            {
                "sample_id": f"train-empty-{shard_index + 1:02d}",
                "text": "  　",
                "label": 0,
                "lang": "zh",
                "source": f"shard-{shard_index + 1}",
            }
        )
        path = os.path.join(train_dir, f"train-shard-{shard_index + 1:02d}.jsonl")
        write_jsonl(path, records)
        paths.append(path)
    return paths


def _expected_model_version(config, workspace):
    """按管线指纹规则预推模型版本，使推理请求引用有效版本。"""
    from pipeline.pipeline import Pipeline

    pipe = Pipeline(workspace)
    stage1_artifact, _, _, _ = _stage1_for_version(pipe, config)
    input_fp = stage2_input_fingerprint(stage1_artifact, config)
    return compute_model_version(input_fp)


def _stage1_for_version(pipe, config):
    from pipeline.stages import run_stage1

    return run_stage1(pipe.ws, config)


def generate_inference_shards(infer_dir, config, workspace, seed=992211, shard_count=2, per_shard=10):
    rng = random.Random(seed)
    os.makedirs(infer_dir, exist_ok=True)
    model_version = _expected_model_version(config, workspace)
    paths = []
    request_id = 0
    for shard_index in range(shard_count):
        requests = []
        for _ in range(per_shard):
            label = rng.randrange(2)
            text = _build_text(rng, label)
            requests.append(
                {
                    "request_id": f"req-{request_id:04d}",
                    "text": text,
                    "ref_model_version": model_version,
                    "expected_min": 0.0,
                    "expected_max": 1.0,
                }
            )
            request_id += 1
        if shard_index == 1:
            requests.append(
                {
                    "request_id": "req-empty-01",
                    "text": "",
                    "ref_model_version": model_version,
                    "expected_min": 0.0,
                    "expected_max": 1.0,
                }
            )
        path = os.path.join(infer_dir, f"infer-shard-{shard_index + 1:02d}.json")
        write_json_array(path, requests)
        paths.append(path)
    return paths, model_version


def write_default_config(config_path, overrides=None):
    import yaml

    config = validate_config(
        {
            **DEFAULT_CONFIG,
            **(overrides or {}),
        }
    )
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    text = yaml.safe_dump(config, sort_keys=False, allow_unicode=True)
    with open(config_path, "wb") as handle:
        handle.write(text.encode("utf-8"))
    return config


def generate_workspace(workspace_root, seed=20261122, config_overrides=None):
    """一次性生成完整演示工作区的数据与配置。"""
    from pipeline.workspace import Workspace

    ws = Workspace(workspace_root).ensure_dirs()
    config = write_default_config(ws.config_path, config_overrides)
    train_paths = generate_training_shards(ws.train_dir, seed=seed)
    infer_paths, model_version = generate_inference_shards(ws.infer_dir, config, workspace_root, seed=seed + 7)
    return {
        "workspace": ws,
        "config": config,
        "train_paths": train_paths,
        "infer_paths": infer_paths,
        "expected_model_version": model_version,
    }
