from __future__ import annotations

import pytest

from hotroute import samples
from hotroute.errors import BusinessError, E_VOCAB_MISSING
from hotroute.execution import execute_graph
from hotroute.graph import validate_graph


def test_repeated_execution_is_bitwise_identical():
    graph = validate_graph(samples.v2_quant_graph())
    first = execute_graph(graph, samples.MODEL_ID, seed=77, known_vocabs=samples.KNOWN_VOCABS)
    second = execute_graph(graph, samples.MODEL_ID, seed=77, known_vocabs=samples.KNOWN_VOCABS)
    assert first["result_digest"] == second["result_digest"]
    assert first["outputs"] == second["outputs"]
    output = first["outputs"]["act"]
    assert output["shape"] == [2, 6]
    assert output["dtype"] == "float32"
    assert len(output["content_sha256"]) == 64


def test_quantization_reports_per_node_rounding_error():
    graph = validate_graph(samples.v2_quant_graph())
    result = execute_graph(graph, samples.MODEL_ID, seed=5, known_vocabs=samples.KNOWN_VOCABS)
    quantized = [node for node in result["nodes"] if node["quant_bits"] < 32]
    assert len(quantized) == 3
    for node in quantized:
        assert node["rounding_error"]["applied"] is True
        assert node["rounding_error"]["max_abs_error"] >= 0
        assert len(node["rounding_error"]["error_sha256"]) == 64


def test_full_precision_nodes_mark_quantization_not_applied():
    graph = validate_graph(samples.v1_graph())
    result = execute_graph(graph, samples.MODEL_ID, seed=5, known_vocabs=samples.KNOWN_VOCABS)
    for node in result["nodes"]:
        assert node["rounding_error"]["applied"] is False


def test_unknown_vocab_fails_before_any_execution():
    graph = validate_graph(samples.v1_graph())
    graph["vocab_refs"].append("missing-tokenizer-v9")
    with pytest.raises(BusinessError) as caught:
        execute_graph(graph, samples.MODEL_ID, seed=1, known_vocabs=samples.KNOWN_VOCABS)
    assert caught.value.code == E_VOCAB_MISSING
    assert caught.value.context["missing_vocab_versions"] == ["missing-tokenizer-v9"]
    assert caught.value.context["partial_inference_performed"] is False

