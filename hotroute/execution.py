from __future__ import annotations

import hashlib
from typing import Any

import numpy as np

from .errors import BusinessError, E_VOCAB_MISSING
from .graph import graph_signature, graph_vocab_refs
from .utils import canonical_json, sha256_text


def _seed_for(parts: list[Any]) -> np.random.SeedSequence:
    digest = hashlib.sha256(canonical_json(parts).encode("utf-8")).digest()
    words = [int.from_bytes(digest[index : index + 4], "little") for index in range(0, 16, 4)]
    return np.random.SeedSequence(words)


def _generate_tensor(
    spec: dict[str, Any],
    seed: int,
    model_id: str,
    purpose: str,
) -> np.ndarray:
    rng = np.random.default_rng(
        _seed_for(
            [
                "hotroute-tensor-v1",
                seed,
                model_id,
                purpose,
                spec["shape"],
                spec["dtype"],
            ]
        )
    )
    dtype = np.dtype(spec["dtype"])
    if dtype.kind == "f":
        values = rng.uniform(-1.0, 1.0, size=tuple(spec["shape"]))
    elif dtype.kind in {"i", "u"}:
        values = rng.integers(-8, 9, size=tuple(spec["shape"]), dtype=dtype)
    else:
        raise BusinessError("E_GRAPH_INVALID", "unsupported deterministic tensor dtype", {"dtype": spec["dtype"]})
    return np.asarray(values, dtype=dtype)


def _quantize_dequantize(array: np.ndarray, bits: int) -> tuple[np.ndarray, dict[str, Any]]:
    if bits >= 32:
        return array, {
            "applied": False,
            "quant_bits": bits,
            "scheme": "full_precision",
        }
    qmax = (1 << (bits - 1)) - 1
    work = np.asarray(array, dtype=np.float64)
    max_abs = float(np.max(np.abs(work))) if work.size else 0.0
    scale = max_abs / qmax if max_abs > 0 else 1.0
    quantized = np.rint(work / scale)
    quantized = np.clip(quantized, -qmax - 1, qmax)
    dequantized = quantized * scale
    error = np.abs(dequantized - work)
    error_bytes = np.ascontiguousarray(error.astype(np.float64)).tobytes(order="C")
    return np.asarray(dequantized, dtype=array.dtype), {
        "applied": True,
        "quant_bits": bits,
        "scheme": "symmetric_signed_round_nearest",
        "qmin": -qmax - 1,
        "qmax": qmax,
        "scale": float(scale),
        "max_abs_error": float(np.max(error)) if error.size else 0.0,
        "mean_abs_error": float(np.mean(error)) if error.size else 0.0,
        "sum_abs_error": float(np.sum(error)) if error.size else 0.0,
        "error_sha256": hashlib.sha256(error_bytes).hexdigest(),
    }


def _tensor_summary(array: np.ndarray) -> dict[str, Any]:
    contiguous = np.ascontiguousarray(array)
    flat = contiguous.reshape(-1)
    sample_count = min(8, int(flat.size))
    sample = [flat[index].item() for index in range(sample_count)]
    stats: dict[str, Any] = {}
    if flat.size:
        if contiguous.dtype.kind == "f":
            stats = {
                "min": float(np.min(contiguous)),
                "max": float(np.max(contiguous)),
                "mean": float(np.mean(contiguous, dtype=np.float64)),
            }
        else:
            stats = {
                "min": int(np.min(contiguous)),
                "max": int(np.max(contiguous)),
                "sum": int(np.sum(contiguous, dtype=np.int64)),
            }
    return {
        "shape": [int(dim) for dim in contiguous.shape],
        "dtype": str(contiguous.dtype),
        "content_sha256": hashlib.sha256(contiguous.tobytes(order="C")).hexdigest(),
        "sample": sample,
        "stats": stats,
    }


def _apply_op(
    node: dict[str, Any],
    current: np.ndarray,
    parameters: list[np.ndarray],
) -> np.ndarray:
    op = node["op"]
    output_spec = node["outputs"][0]
    output_dtype = np.dtype(output_spec["dtype"])
    if op == "identity":
        result = current
    elif op == "relu":
        result = np.maximum(current, np.asarray(0, dtype=current.dtype))
    elif op == "tanh":
        result = np.tanh(current.astype(np.float64, copy=False)).astype(current.dtype)
    elif op == "scale":
        result = (current.astype(np.float64, copy=False) * 0.75).astype(current.dtype)
    elif op == "add":
        result = np.add(current, parameters[0]).astype(current.dtype)
    elif op == "linear":
        result = np.matmul(current, parameters[0]).astype(current.dtype)
    elif op == "layernorm":
        work = current.astype(np.float64, copy=False)
        mean = np.mean(work, axis=-1, keepdims=True, dtype=np.float64)
        variance = np.var(work, axis=-1, keepdims=True, dtype=np.float64)
        result = ((work - mean) / np.sqrt(variance + 1e-5)).astype(current.dtype)
    elif op == "softmax":
        work = current.astype(np.float64, copy=False)
        shifted = work - np.max(work, axis=-1, keepdims=True)
        exponent = np.exp(shifted)
        result = (exponent / np.sum(exponent, axis=-1, keepdims=True)).astype(current.dtype)
    elif op == "reshape":
        result = np.reshape(current, tuple(output_spec["shape"]))
    else:
        raise BusinessError(
            "E_GRAPH_INVALID",
            "operator was validated but has no executor",
            {"node_id": node["id"], "field": "op", "value": op},
        )
    return np.asarray(result, dtype=output_dtype)


def execute_graph(
    graph: dict[str, Any],
    model_id: str,
    seed: int,
    known_vocabs: list[str],
) -> dict[str, Any]:
    refs = graph_vocab_refs(graph)
    known = set(known_vocabs)
    missing = [ref for ref in refs if ref not in known]
    if missing:
        raise BusinessError(
            E_VOCAB_MISSING,
            "graph references unknown feature vocabulary versions",
            {
                "model_id": model_id,
                "missing_vocab_versions": missing,
                "referenced_vocab_versions": refs,
                "partial_inference_performed": False,
            },
        )

    input_spec = graph["inputs"][0]
    current = _generate_tensor(input_spec, seed, model_id, "graph-input")
    input_summary = _tensor_summary(current)
    node_records: list[dict[str, Any]] = []
    node_outputs: dict[str, np.ndarray] = {}

    for index, node in enumerate(graph["nodes"]):
        parameters = [
            _generate_tensor(spec, seed, model_id, f"node:{node['id']}:input:{param_index}")
            for param_index, spec in enumerate(node["inputs"][1:], start=1)
        ]
        raw_output = _apply_op(node, current, parameters)
        quantized_output, quant_error = _quantize_dequantize(raw_output, node["quant_bits"])
        finalized_output = np.asarray(quantized_output, dtype=np.dtype(node["outputs"][0]["dtype"]))
        output_summary = _tensor_summary(finalized_output)
        node_records.append(
            {
                "index": index,
                "id": node["id"],
                "op": node["op"],
                "quant_bits": node["quant_bits"],
                "output": output_summary,
                "rounding_error": quant_error,
            }
        )
        node_outputs[node["id"]] = finalized_output
        current = finalized_output

    outputs = {
        output_id: _tensor_summary(node_outputs[output_id])
        for output_id in graph["outputs"]
    }
    result_payload = {
        "model_id": model_id,
        "graph_signature": graph_signature(graph),
        "seed": seed,
        "input": input_summary,
        "outputs": outputs,
        "nodes": node_records,
    }
    result_payload["result_digest"] = sha256_text(canonical_json(result_payload))
    return result_payload

