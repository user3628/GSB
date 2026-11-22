from __future__ import annotations

from typing import Any

from .graph import graph_signature, graph_vocab_refs


DIFF_SHAPE = 1 << 0
DIFF_DTYPE = 1 << 1
DIFF_QUANT = 1 << 2
DIFF_OP = 1 << 3
DIFF_PREPOST = 1 << 4
DIFF_VOCAB = 1 << 5

DIFF_NAMES = {
    DIFF_SHAPE: "shape_changed",
    DIFF_DTYPE: "dtype_changed",
    DIFF_QUANT: "quant_bits_changed",
    DIFF_OP: "op_added_removed_or_changed",
    DIFF_PREPOST: "prepost_changed",
    DIFF_VOCAB: "vocab_refs_changed",
}


def _node_io_signature(node: dict[str, Any]) -> tuple[Any, Any]:
    input_shapes = [tuple(spec["shape"]) for spec in node["inputs"]]
    output_shapes = [tuple(spec["shape"]) for spec in node["outputs"]]
    return input_shapes, output_shapes


def _node_dtype_signature(node: dict[str, Any]) -> tuple[Any, Any]:
    input_dtypes = [spec["dtype"] for spec in node["inputs"]]
    output_dtypes = [spec["dtype"] for spec in node["outputs"]]
    return input_dtypes, output_dtypes


def diff_graphs(
    old_graph: dict[str, Any] | None,
    new_graph: dict[str, Any],
) -> dict[str, Any]:
    if old_graph is None:
        return {
            "mask": 0,
            "names": [],
            "in_place_migratable": False,
            "requires_buffer_rebuild": True,
            "mode": "initial",
            "old_signature": None,
            "new_signature": graph_signature(new_graph),
        }

    mask = 0
    old_nodes = old_graph["nodes"]
    new_nodes = new_graph["nodes"]

    old_op_identity = [(node["id"], node["op"]) for node in old_nodes]
    new_op_identity = [(node["id"], node["op"]) for node in new_nodes]
    if old_op_identity != new_op_identity:
        mask |= DIFF_OP

    old_input_shapes = [tuple(spec["shape"]) for spec in old_graph["inputs"]]
    new_input_shapes = [tuple(spec["shape"]) for spec in new_graph["inputs"]]
    if old_input_shapes != new_input_shapes:
        mask |= DIFF_SHAPE
    old_input_dtypes = [spec["dtype"] for spec in old_graph["inputs"]]
    new_input_dtypes = [spec["dtype"] for spec in new_graph["inputs"]]
    if old_input_dtypes != new_input_dtypes:
        mask |= DIFF_DTYPE

    for index in range(max(len(old_nodes), len(new_nodes))):
        if index >= len(old_nodes) or index >= len(new_nodes):
            continue
        old_node = old_nodes[index]
        new_node = new_nodes[index]
        if _node_io_signature(old_node) != _node_io_signature(new_node):
            mask |= DIFF_SHAPE
        if _node_dtype_signature(old_node) != _node_dtype_signature(new_node):
            mask |= DIFF_DTYPE
        if old_node["quant_bits"] != new_node["quant_bits"]:
            mask |= DIFF_QUANT
        old_steps = (old_node["preprocess"], old_node["postprocess"], old_node.get("vocab_refs", []))
        new_steps = (new_node["preprocess"], new_node["postprocess"], new_node.get("vocab_refs", []))
        if old_steps != new_steps:
            mask |= DIFF_PREPOST

    if old_graph.get("outputs", []) != new_graph.get("outputs", []):
        mask |= DIFF_OP
    if graph_vocab_refs(old_graph) != graph_vocab_refs(new_graph):
        mask |= DIFF_VOCAB

    names = [name for bit, name in DIFF_NAMES.items() if mask & bit]
    in_place = mask == DIFF_QUANT
    if mask == 0:
        mode = "unchanged"
    elif in_place:
        mode = "in_place_quant"
    else:
        mode = "rebuild_required"
    return {
        "mask": mask,
        "names": names,
        "in_place_migratable": in_place,
        "requires_buffer_rebuild": mask != 0 and not in_place,
        "mode": mode,
        "old_signature": graph_signature(old_graph),
        "new_signature": graph_signature(new_graph),
    }

