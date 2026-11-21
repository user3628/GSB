"""确定性线性打分器：固定顺序全批量梯度下降 + 逐维定点量化。"""

import math

import numpy as np

from pipeline.errors import E_INVALID_TRAIN, E_NUMERIC, PipelineError


def _validate_hyperparams(config):
    model_cfg = config["model"]
    epochs = model_cfg["epochs"]
    lr = model_cfg["learning_rate"]
    if epochs == 0:
        raise PipelineError(
            E_INVALID_TRAIN,
            "迭代轮数为 0，无法拟合模型",
            {"epoch": 0, "reason": "epochs==0"},
        )
    if lr < 0:
        raise PipelineError(
            E_INVALID_TRAIN,
            "学习率为负值，拒绝训练",
            {"epoch": 0, "reason": "learning_rate<0", "learning_rate": lr},
        )


def _build_matrix(feature_records, dimension):
    """按 sample_id 固定排序构造稠密设计矩阵（含 bias 列）与标签向量。"""
    ordered = sorted(feature_records, key=lambda item: item["sample_id"])
    sample_order = [item["sample_id"] for item in ordered]
    matrix = np.zeros((len(ordered), dimension + 1), dtype=np.float64)
    labels = np.zeros(len(ordered), dtype=np.float64)
    for row, item in enumerate(ordered):
        labels[row] = item["label"]
        matrix[row, dimension] = 1.0  # 偏置项固定为 1
        for pair in item["features"]:
            matrix[row, pair["index"]] = pair["value"]
    return matrix, labels, sample_order


def _sigmoid(values):
    return 1.0 / (1.0 + np.exp(-values))


def fit_model(feature_records, config):
    """执行确定性训练，返回模型产物字典。"""
    _validate_hyperparams(config)
    dimension = config["features"]["hash_dimension"]
    matrix, labels, sample_order = _build_matrix(feature_records, dimension)
    params = np.zeros(dimension + 1, dtype=np.float64)
    lr = float(config["model"]["learning_rate"])
    epochs = int(config["model"]["epochs"])
    history = []

    for epoch in range(1, epochs + 1):
        scores = matrix.dot(params)
        probabilities = _sigmoid(scores)
        eps = 1e-12
        loss = float(
            -np.mean(
                labels * np.log(probabilities + eps)
                + (1.0 - labels) * np.log(1.0 - probabilities + eps)
            )
        )
        if math.isnan(loss) or math.isinf(loss):
            raise PipelineError(
                E_NUMERIC,
                "训练损失非有限值",
                {"epoch": epoch, "reason": "loss_not_finite", "loss": loss},
            )
        gradient = matrix.T.dot(probabilities - labels) / matrix.shape[0]
        if not np.all(np.isfinite(gradient)):
            raise PipelineError(
                E_NUMERIC,
                "梯度出现 NaN/Inf",
                {"epoch": epoch, "reason": "gradient_not_finite"},
            )
        new_params = params - lr * gradient
        delta_norm = float(np.linalg.norm(new_params - params))
        history.append(
            {
                "epoch": epoch,
                "loss": loss,
                "param_delta_norm": delta_norm,
                "sample_order": list(sample_order),
            }
        )
        params = new_params
        if not np.all(np.isfinite(params)):
            raise PipelineError(
                E_NUMERIC,
                "参数出现 NaN/Inf",
                {"epoch": epoch, "reason": "params_not_finite"},
            )

    quantized = quantize_per_dimension(params, int(config["model"]["quant_bits"]))
    return {
        "dimension": dimension,
        "learning_rate": lr,
        "epochs": epochs,
        "sample_order": list(sample_order),
        "loss_history": history,
        **quantized,
    }


def quantize_per_dimension(params, quant_bits):
    """逐维定点量化：q = round(w / scale)，每维独立 2 的幂缩放因子。"""
    max_q = 2 ** (quant_bits - 1) - 1
    scales = []
    qvalues = []
    for value in params:
        value = float(value)
        if value == 0.0:
            exponent = 0
            q = 0
        else:
            exponent = int(math.floor(math.log2(abs(value)))) - int(
                math.log2(max_q)
            )
            scale = 2.0**exponent
            q = int(round(value / scale))
            # 理论上不会越界，这里做确定性夹紧保护
            q = max(-max_q - 1, min(max_q, q))
        scales.append(exponent)
        qvalues.append(q)
    return {
        "quant_bits": quant_bits,
        "scale_exponents": scales,
        "quantized_weights": qvalues,
    }


def dequantize_weights(model_artifact):
    """把定点参数还原为 float64 向量。"""
    exponents = model_artifact["scale_exponents"]
    qvalues = model_artifact["quantized_weights"]
    return np.array(
        [q * (2.0**exp) for q, exp in zip(qvalues, exponents)], dtype=np.float64
    )


def score_vector(features, weights, dimension):
    """对单条稀疏特征打分，features 为 [{"index","value"}]。"""
    score = float(weights[dimension])  # bias
    for pair in features:
        score += pair["value"] * float(weights[pair["index"]])
    return score


def sigmoid(value):
    return float(_sigmoid(np.array(value, dtype=np.float64)))
