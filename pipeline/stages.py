"""四个管线阶段的纯计算逻辑与阶段输入指纹。

每个阶段返回 (artifact_dict, input_fingerprint, artifact_summary)，
落盘、检查点发布与失效判断由 :mod:`pipeline.pipeline` 统一编排。
"""

import math
import os

from pipeline.atomicio import atomic_write_json, read_json
from pipeline.errors import (
    E_BUDGET_EXCEEDED,
    E_EMPTY_SAMPLE,
    E_MODEL_VERSION_MISMATCH,
    PipelineError,
)
from pipeline.featurizer import featurize_sample, featurize_text
from pipeline.hashing import digest_json, sha256_bytes, short_digest
from pipeline.model import dequantize_weights, fit_model, score_vector, sigmoid
from pipeline.shards import (
    directory_fingerprint,
    read_json_array,
    read_jsonl,
)

TOP_TERMS_IN_STATS = 50


def stage_config_fingerprint(config, stage):
    """仅纳入该阶段实际消费的配置字段，避免无关参数变化导致版本漂移。"""
    if stage == "stage1_features":
        subset = {
            "normalize": config["normalize"],
            "features": config["features"],
            "resources": config["resources"],
        }
    elif stage == "stage2_model":
        subset = {"features": config["features"], "model": config["model"]}
    elif stage == "stage3_inference":
        subset = {
            "normalize": config["normalize"],
            "features": config["features"],
            "inference": config["inference"],
            "resources": config["resources"],
        }
    else:
        subset = {"inference": config["inference"]}
    return digest_json({"stage_config": stage, "config": subset})


def train_input_fingerprint(ws):
    return directory_fingerprint(ws.train_dir, ".jsonl")


def inference_input_fingerprint(ws):
    return directory_fingerprint(ws.infer_dir, ".json")


def stage2_input_fingerprint(stage1_artifact, config):
    payload = {
        "input_artifact_digest": stage1_artifact["artifact_summary"]["digest"],
        "config_fingerprint": stage_config_fingerprint(config, "stage2_model"),
        "stage": "stage2_model",
    }
    return digest_json(payload)


def stage3_input_fingerprint(ws, stage2_artifact, config):
    infer_fp, files = inference_input_fingerprint(ws)
    payload = {
        "inference_fingerprint": infer_fp,
        "inference_files": files,
        "model_version": stage2_artifact["model_version"],
        "config_fingerprint": stage_config_fingerprint(config, "stage3_inference"),
        "stage": "stage3_inference",
    }
    return digest_json(payload)


def stage4_input_fingerprint(stage3_artifact, config):
    payload = {
        "input_artifact_digest": stage3_artifact["artifact_summary"]["digest"],
        "config_fingerprint": stage_config_fingerprint(config, "stage4_summary"),
        "stage": "stage4_summary",
    }
    return digest_json(payload)


def compute_model_version(stage2_input_fp):
    return "mv_" + short_digest(stage2_input_fp, 16)


def _summary(name, payload, extra=None):
    digest = digest_json(payload)
    summary = {"artifact_name": name, "digest": digest}
    if extra:
        summary.update(extra)
    return digest, summary


def run_stage1(ws, config):
    input_fp, files = train_input_fingerprint(ws)
    samples = []
    for path in files:
        samples.extend(read_jsonl(os.path.join(ws.train_dir, path)))
    budget = config["resources"]["max_train_samples"]
    if len(samples) > budget:
        raise PipelineError(
            E_BUDGET_EXCEEDED,
            "训练样本数超出资源预算",
            {"count": len(samples), "budget": budget},
        )
    samples.sort(key=lambda item: item["sample_id"])

    features = []
    diagnostics = []
    term_totals = {}
    for sample in samples:
        try:
            record = featurize_sample(sample, config)
        except PipelineError as exc:
            if exc.code != E_EMPTY_SAMPLE:
                raise
            diagnostics.append(
                {
                    "sample_id": sample["sample_id"],
                    "error": E_EMPTY_SAMPLE,
                    "message": exc.message,
                    "details": exc.details,
                }
            )
            continue
        if record["token_count"] > config["resources"]["max_tokens_per_sample"]:
            raise PipelineError(
                E_BUDGET_EXCEEDED,
                "单样本词项数超出资源预算",
                {"sample_id": record["sample_id"], "token_count": record["token_count"]},
            )
        for token, count in record["token_counts"].items():
            term_totals[token] = term_totals.get(token, 0) + count
        features.append(record)

    term_stats = [
        {"token": token, "count": count}
        for token, count in sorted(
            term_totals.items(), key=lambda item: (-item[1], item[0])
        )[:TOP_TERMS_IN_STATS]
    ]
    artifact = {
        "stage": "stage1_features",
        "input_fingerprint": input_fp,
        "config_fingerprint": stage_config_fingerprint(config, "stage1_features"),
        "hash_dimension": config["features"]["hash_dimension"],
        "sample_count": len(samples),
        "features": features,
        "diagnostics": diagnostics,
        "term_stats": term_stats,
    }
    digest, summary = _summary(
        "stage1_features.json",
        artifact,
        {
            "sample_count": len(samples),
            "feature_records": len(features),
            "diagnostics_count": len(diagnostics),
            "term_kinds": len(term_totals),
        },
    )
    artifact["artifact_summary"] = summary
    return artifact, input_fp, summary, digest


def run_stage2(stage1_artifact, config):
    input_fp = stage2_input_fingerprint(stage1_artifact, config)
    model = fit_model(stage1_artifact["features"], config)
    model_version = compute_model_version(input_fp)
    artifact = {
        "stage": "stage2_model",
        "input_fingerprint": input_fp,
        "config_fingerprint": stage_config_fingerprint(config, "stage2_model"),
        "model_version": model_version,
        **model,
    }
    final_loss = model["loss_history"][-1]["loss"] if model["loss_history"] else None
    digest, summary = _summary(
        "stage2_model.json",
        artifact,
        {
            "model_version": model_version,
            "epochs": model["epochs"],
            "final_loss": final_loss,
            "param_count": len(model["quantized_weights"]),
            "quant_bits": model["quant_bits"],
        },
    )
    artifact["artifact_summary"] = summary
    return artifact, input_fp, summary, digest


def run_stage3(ws, stage2_artifact, config):
    input_fp = stage3_input_fingerprint(ws, stage2_artifact, config)
    weights = dequantize_weights(stage2_artifact)
    dimension = stage2_artifact["dimension"]
    current_version = stage2_artifact["model_version"]
    threshold = float(config["inference"]["confidence_threshold"])
    batch_size = int(config["inference"]["batch_size"])

    requests = []
    _, files = inference_input_fingerprint(ws)
    for name in files:
        requests.extend(read_json_array(os.path.join(ws.infer_dir, name)))
    budget = config["resources"]["max_inference_requests"]
    if len(requests) > budget:
        raise PipelineError(
            E_BUDGET_EXCEEDED,
            "推理请求数超出资源预算",
            {"count": len(requests), "budget": budget},
        )
    requests.sort(key=lambda item: item["request_id"])

    mismatches = []
    for request in requests:
        referenced = request.get("ref_model_version")
        if referenced != current_version:
            mismatches.append(
                {
                    "request_id": request["request_id"],
                    "referenced_version": referenced,
                    "current_version": current_version,
                }
            )
    if mismatches:
        raise PipelineError(
            E_MODEL_VERSION_MISMATCH,
            "存在引用模型版本与当前有效版本不一致的请求，禁止替代打分",
            {"mismatches": mismatches},
        )

    results = []
    diagnostics = []
    for batch_start in range(0, len(requests), batch_size):
        batch = requests[batch_start : batch_start + batch_size]
        for request in batch:
            try:
                _, features, _ = featurize_text(request.get("text", ""), config)
            except PipelineError as exc:
                if exc.code != E_EMPTY_SAMPLE:
                    raise
                diagnostics.append(
                    {
                        "request_id": request["request_id"],
                        "error": E_EMPTY_SAMPLE,
                        "message": exc.message,
                    }
                )
                continue
            score = score_vector(
                [{"index": idx, "value": value} for idx, value in features.items()],
                weights,
                dimension,
            )
            probability = sigmoid(score)
            margin = abs(probability - threshold)
            decision = "accept" if probability >= threshold else "reject"
            expected_min = request.get("expected_min", -math.inf)
            expected_max = request.get("expected_max", math.inf)
            in_expected_range = bool(expected_min <= probability <= expected_max)
            results.append(
                {
                    "request_id": request["request_id"],
                    "score": score,
                    "probability": probability,
                    "decision": decision,
                    "confidence_margin": margin,
                    "model_version": current_version,
                    "in_expected_range": in_expected_range,
                }
            )

    artifact = {
        "stage": "stage3_inference",
        "input_fingerprint": input_fp,
        "config_fingerprint": stage_config_fingerprint(config, "stage3_inference"),
        "model_version": current_version,
        "confidence_threshold": threshold,
        "batch_size": batch_size,
        "request_count": len(requests),
        "results": results,
        "diagnostics": diagnostics,
    }
    digest, summary = _summary(
        "stage3_inference.json",
        artifact,
        {
            "model_version": current_version,
            "request_count": len(requests),
            "result_count": len(results),
            "accept_count": sum(1 for item in results if item["decision"] == "accept"),
            "reject_count": sum(1 for item in results if item["decision"] == "reject"),
            "diagnostics_count": len(diagnostics),
        },
    )
    artifact["artifact_summary"] = summary
    return artifact, input_fp, summary, digest


def run_stage4(stage3_artifact, config):
    input_fp = stage4_input_fingerprint(stage3_artifact, config)
    results = stage3_artifact["results"]
    probabilities = [item["probability"] for item in results]
    in_range = [item for item in results if item["in_expected_range"]]
    artifact = {
        "stage": "stage4_summary",
        "input_fingerprint": input_fp,
        "config_fingerprint": stage_config_fingerprint(config, "stage4_summary"),
        "model_version": stage3_artifact["model_version"],
        "total_requests": stage3_artifact["request_count"],
        "scored_requests": len(results),
        "accepted": sum(1 for item in results if item["decision"] == "accept"),
        "rejected": sum(1 for item in results if item["decision"] == "reject"),
        "expected_range_hits": len(in_range),
        "expected_range_hit_rate": (len(in_range) / len(results)) if results else None,
        "min_probability": min(probabilities) if probabilities else None,
        "max_probability": max(probabilities) if probabilities else None,
        "diagnostics_count": len(stage3_artifact["diagnostics"]),
    }
    digest, summary = _summary(
        "stage4_summary.json",
        artifact,
        {
            "model_version": artifact["model_version"],
            "total_requests": artifact["total_requests"],
            "accepted": artifact["accepted"],
            "rejected": artifact["rejected"],
        },
    )
    artifact["artifact_summary"] = summary
    return artifact, input_fp, summary, digest


def load_artifact(path):
    artifact, _ = read_json(path)
    return artifact


def write_artifact(ws, stage, artifact):
    path = ws.artifact_path(stage)
    atomic_write_json(path, artifact)
    with open(path, "rb") as handle:
        return sha256_bytes(handle.read())
