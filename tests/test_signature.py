from __future__ import annotations

import copy

from hotroute import samples
from hotroute.graph import graph_signature, validate_graph
from hotroute.signature import (
    DIFF_DTYPE,
    DIFF_OP,
    DIFF_PREPOST,
    DIFF_QUANT,
    DIFF_SHAPE,
    DIFF_VOCAB,
    diff_graphs,
)


def test_quant_only_diff_is_in_place_migratable():
    old = validate_graph(samples.v1_graph())
    new = validate_graph(samples.v2_quant_graph())
    diff = diff_graphs(old, new)
    assert diff["mask"] == DIFF_QUANT == 4
    assert diff["names"] == ["quant_bits_changed"]
    assert diff["in_place_migratable"] is True
    assert diff["requires_buffer_rebuild"] is False
    assert diff["mode"] == "in_place_quant"


def test_shape_change_requires_rebuild():
    old = validate_graph(samples.v2_quant_graph())
    new = validate_graph(samples.v3_shape_graph())
    diff = diff_graphs(old, new)
    assert diff["mask"] == DIFF_SHAPE
    assert diff["in_place_migratable"] is False
    assert diff["requires_buffer_rebuild"] is True
    assert diff["mode"] == "rebuild_required"


def test_dtype_prepost_op_and_vocab_categories():
    old = validate_graph(samples.v3_shape_graph())
    dtype_new = validate_graph(samples.v4_dtype_graph())
    assert diff_graphs(old, dtype_new)["mask"] & DIFF_DTYPE

    prepost_graph = copy.deepcopy(old)
    prepost_graph["nodes"][0]["preprocess"].append("rewrite:preproc-v1")
    assert diff_graphs(old, prepost_graph)["mask"] & DIFF_PREPOST

    op_graph = copy.deepcopy(old)
    op_graph["nodes"][2]["op"] = "tanh"
    assert diff_graphs(old, op_graph)["mask"] & DIFF_OP

    vocab_graph = copy.deepcopy(old)
    vocab_graph["vocab_refs"].append("tok-v2")
    assert diff_graphs(old, vocab_graph)["mask"] & DIFF_VOCAB


def test_signature_depends_on_node_order():
    first = validate_graph(samples.v1_graph())
    second = copy.deepcopy(first)
    second["nodes"][1], second["nodes"][2] = second["nodes"][2], second["nodes"][1]
    assert graph_signature(first) != graph_signature(second)


def test_signature_is_deterministic_for_equal_graphs():
    first = validate_graph(samples.v1_graph())
    second = validate_graph(copy.deepcopy(samples.v1_graph()))
    assert graph_signature(first) == graph_signature(second)

