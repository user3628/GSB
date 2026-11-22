from __future__ import annotations

import copy
import math
from typing import Any

from .errors import BusinessError, E_GRAPH_INVALID
from .utils import canonical_json, require_positive_int, sha256_text


ALLOWED_DTYPES = ("float16", "float32", "float64", "int32", "int64")
FLOAT_DTYPES = ("float16", "float32", "float64")
ALLOWED_QUANT_BITS = (8, 16, 32)
ALLOWED_OPS = (
    "identity",
    "relu",
    "tanh",
    "scale",
    "add",
    "linear",
    "layernorm",
    "softmax",
    "reshape",
)
MAX_TENSOR_ELEMENTS = 20_000_000

OP_INPUT_ARITY = {
    "identity": 1,
    "relu": 1,
    "tanh": 1,
    "scale": 1,
    "add": 2,
    "linear": 2,
    "layernorm": 1,
    "softmax": 1,
    "reshape": 1,
}


def _graph_error(
    message: str,
    node_id: str | None,
    field: str,
    **extra: Any,
) -> BusinessError:
    context = {"node_id": node_id, "field": field}
    context.update(extra)
    return BusinessError(E_GRAPH_INVALID, message, context)


def _normalize_spec(
    raw: Any,
    node_id: str | None,
    field: str,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _graph_error("tensor specification must be an object", node_id, field)
    shape_raw = raw.get("shape")
    if not isinstance(shape_raw, list) or not shape_raw:
        raise _graph_error("shape must be a non-empty list", node_id, f"{field}.shape")
    shape = [
        require_positive_int(dim, f"{field}.shape[{index}]", node_id)
        for index, dim in enumerate(shape_raw)
    ]
    element_count = math.prod(shape)
    if element_count > MAX_TENSOR_ELEMENTS:
        raise _graph_error(
            "tensor exceeds the local deterministic size limit",
            node_id,
            f"{field}.shape",
            element_count=element_count,
            limit=MAX_TENSOR_ELEMENTS,
        )
    dtype = raw.get("dtype")
    if dtype not in ALLOWED_DTYPES:
        raise _graph_error(
            "dtype is not in the allowed set",
            node_id,
            f"{field}.dtype",
            value=dtype,
            allowed=list(ALLOWED_DTYPES),
        )
    result: dict[str, Any] = {"shape": shape, "dtype": dtype}
    if "name" in raw:
        name = raw["name"]
        if not isinstance(name, str) or not name.strip():
            raise _graph_error("tensor name must be a non-empty string", node_id, f"{field}.name")
        result["name"] = name
    return result


def _assert_same_spec(
    left: dict[str, Any],
    right: dict[str, Any],
    node_id: str | None,
    field: str,
) -> None:
    if left["shape"] != right["shape"] or left["dtype"] != right["dtype"]:
        raise _graph_error(
            "adjacent tensors are not compatible in shape and dtype",
            node_id,
            field,
            expected={"shape": left["shape"], "dtype": left["dtype"]},
            actual={"shape": right["shape"], "dtype": right["dtype"]},
        )


def _normalize_step_list(
    raw: Any,
    node_id: str,
    field: str,
) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise _graph_error(f"{field} must be a list", node_id, field)
    steps: list[str] = []
    for index, step in enumerate(raw):
        if not isinstance(step, str) or not step.strip():
            raise _graph_error(
                f"{field} entries must be non-empty strings",
                node_id,
                f"{field}[{index}]",
            )
        steps.append(step)
    return steps


def _normalize_vocab_refs(raw: Any, node_id: str | None) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise _graph_error("vocab_refs must be a list", node_id, "vocab_refs")
    refs: list[str] = []
    for index, ref in enumerate(raw):
        if not isinstance(ref, str) or not ref.strip():
            raise _graph_error(
                "vocab_refs entries must be non-empty strings",
                node_id,
                f"vocab_refs[{index}]",
            )
        refs.append(ref)
    return refs


def _validate_op_semantics(
    node_id: str,
    op: str,
    inputs: list[dict[str, Any]],
    output: dict[str, Any],
) -> None:
    primary = inputs[0]
    if output["dtype"] != primary["dtype"]:
        raise _graph_error(
            "operator output dtype does not match its primary input",
            node_id,
            "outputs[0].dtype",
            expected=primary["dtype"],
            actual=output["dtype"],
        )

    if op in {"identity", "relu", "tanh", "scale", "layernorm", "softmax"}:
        if output["shape"] != primary["shape"]:
            raise _graph_error(
                f"{op} must preserve the primary input shape",
                node_id,
                "outputs[0].shape",
                expected=primary["shape"],
                actual=output["shape"],
            )
        if op not in {"identity", "reshape"} and primary["dtype"] not in FLOAT_DTYPES:
            raise _graph_error(
                f"{op} requires a floating-point tensor",
                node_id,
                "inputs[0].dtype",
                value=primary["dtype"],
            )

    if op == "add":
        _assert_same_spec(primary, inputs[1], node_id, "inputs[1]")
        _assert_same_spec(primary, output, node_id, "outputs[0]")

    if op == "linear":
        if len(primary["shape"]) != 2 or len(inputs[1]["shape"]) != 2:
            raise _graph_error(
                "linear requires rank-2 activation and weight tensors",
                node_id,
                "inputs",
            )
        batch, in_features = primary["shape"]
        weight_in, out_features = inputs[1]["shape"]
        if in_features != weight_in:
            raise _graph_error(
                "linear weight inner dimension does not match activation",
                node_id,
                "inputs[1].shape",
                activation_shape=primary["shape"],
                weight_shape=inputs[1]["shape"],
            )
        if inputs[1]["dtype"] != primary["dtype"]:
            raise _graph_error(
                "linear weight dtype must match the activation dtype",
                node_id,
                "inputs[1].dtype",
            )
        expected = [batch, out_features]
        if output["shape"] != expected:
            raise _graph_error(
                "linear output shape is inconsistent with its inputs",
                node_id,
                "outputs[0].shape",
                expected=expected,
                actual=output["shape"],
            )

    if op == "reshape":
        input_elements = math.prod(primary["shape"])
        output_elements = math.prod(output["shape"])
        if input_elements != output_elements:
            raise _graph_error(
                "reshape cannot change the number of tensor elements",
                node_id,
                "outputs[0].shape",
                input_elements=input_elements,
                output_elements=output_elements,
            )


def validate_graph(raw_graph: Any) -> dict[str, Any]:
    """Validate and normalize a complete graph without touching persistent state."""
    if not isinstance(raw_graph, dict):
        raise _graph_error("graph must be an object", None, "graph")
    raw_inputs = raw_graph.get("inputs")
    if not isinstance(raw_inputs, list) or len(raw_inputs) != 1:
        raise _graph_error(
            "graph.inputs must contain exactly one primary input tensor",
            None,
            "inputs",
        )
    graph_input = _normalize_spec(raw_inputs[0], None, "inputs[0]")

    raw_nodes = raw_graph.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise _graph_error("graph.nodes must be a non-empty list", None, "nodes")

    nodes: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    previous_output: dict[str, Any] | None = None

    for index, raw_node in enumerate(raw_nodes):
        if not isinstance(raw_node, dict):
            raise _graph_error("node must be an object", None, f"nodes[{index}]")
        node_id_raw = raw_node.get("id")
        if not isinstance(node_id_raw, str) or not node_id_raw.strip():
            raise _graph_error(
                "node id must be a non-empty string",
                None,
                f"nodes[{index}].id",
            )
        node_id = node_id_raw.strip()
        if node_id in seen_ids:
            raise _graph_error("node id is duplicated", node_id, "id")
        seen_ids.add(node_id)

        op = raw_node.get("op")
        if op not in ALLOWED_OPS:
            raise _graph_error(
                "operator type is not supported",
                node_id,
                "op",
                value=op,
                allowed=list(ALLOWED_OPS),
            )

        raw_node_inputs = raw_node.get("inputs")
        if not isinstance(raw_node_inputs, list):
            raise _graph_error("node inputs must be a list", node_id, "inputs")
        arity = OP_INPUT_ARITY[op]
        if len(raw_node_inputs) != arity:
            raise _graph_error(
                f"{op} requires exactly {arity} input tensor(s)",
                node_id,
                "inputs",
                expected=arity,
                actual=len(raw_node_inputs),
            )
        inputs = [
            _normalize_spec(spec, node_id, f"inputs[{input_index}]")
            for input_index, spec in enumerate(raw_node_inputs)
        ]

        raw_outputs = raw_node.get("outputs")
        if not isinstance(raw_outputs, list) or len(raw_outputs) != 1:
            raise _graph_error(
                "each node must declare exactly one output tensor",
                node_id,
                "outputs",
            )
        output = _normalize_spec(raw_outputs[0], node_id, "outputs[0]")

        if previous_output is None:
            _assert_same_spec(graph_input, inputs[0], node_id, "inputs[0]")
        else:
            _assert_same_spec(previous_output, inputs[0], node_id, "inputs[0]")

        quant_bits = raw_node.get("quant_bits")
        if (
            isinstance(quant_bits, bool)
            or not isinstance(quant_bits, int)
            or quant_bits not in ALLOWED_QUANT_BITS
        ):
            raise _graph_error(
                "quant_bits is not in the legal enum",
                node_id,
                "quant_bits",
                value=quant_bits,
                allowed=list(ALLOWED_QUANT_BITS),
            )
        if quant_bits < 32 and output["dtype"] not in FLOAT_DTYPES:
            raise _graph_error(
                "quantized nodes must produce a floating-point tensor",
                node_id,
                "outputs[0].dtype",
                value=output["dtype"],
            )

        preprocess = _normalize_step_list(raw_node.get("preprocess"), node_id, "preprocess")
        postprocess = _normalize_step_list(raw_node.get("postprocess"), node_id, "postprocess")
        node_vocab_refs = _normalize_vocab_refs(raw_node.get("vocab_refs"), node_id)
        _validate_op_semantics(node_id, op, inputs, output)

        node = {
            "id": node_id,
            "op": op,
            "inputs": inputs,
            "outputs": [output],
            "quant_bits": quant_bits,
            "preprocess": preprocess,
            "postprocess": postprocess,
        }
        if node_vocab_refs:
            node["vocab_refs"] = node_vocab_refs
        nodes.append(node)
        previous_output = output

    graph_vocab_refs = _normalize_vocab_refs(raw_graph.get("vocab_refs"), None)
    raw_outputs = raw_graph.get("outputs")
    if raw_outputs is None:
        output_ids = [nodes[-1]["id"]]
    else:
        if not isinstance(raw_outputs, list) or not raw_outputs:
            raise _graph_error("graph.outputs must be a non-empty list", None, "outputs")
        output_ids = []
        for output_index, output_id in enumerate(raw_outputs):
            if not isinstance(output_id, str) or output_id not in seen_ids:
                raise _graph_error(
                    "graph output must reference an existing node id",
                    output_id if isinstance(output_id, str) else None,
                    f"outputs[{output_index}]",
                )
            output_ids.append(output_id)

    all_refs: list[str] = []
    for ref in graph_vocab_refs:
        if ref not in all_refs:
            all_refs.append(ref)
    for node in nodes:
        for ref in node.get("vocab_refs", []):
            if ref not in all_refs:
                all_refs.append(ref)

    graph = {
        "inputs": [graph_input],
        "nodes": nodes,
        "outputs": output_ids,
        "vocab_refs": all_refs,
    }
    return graph


def graph_signature(graph: dict[str, Any]) -> str:
    """Signature covers ordered graph structure, shapes, dtypes, quant and step IDs."""
    payload = {
        "inputs": graph["inputs"],
        "nodes": graph["nodes"],
        "outputs": graph["outputs"],
        "vocab_refs": graph.get("vocab_refs", []),
    }
    return sha256_text(canonical_json(payload))


def graph_vocab_refs(graph: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    for ref in graph.get("vocab_refs", []):
        if ref not in refs:
            refs.append(ref)
    for node in graph.get("nodes", []):
        for ref in node.get("vocab_refs", []):
            if ref not in refs:
                refs.append(ref)
    return refs


def preprocess_steps(graph: dict[str, Any]) -> list[str]:
    steps: list[str] = []
    for node in graph["nodes"]:
        for step in node["preprocess"]:
            steps.append(f"{node['id']}:pre:{step}")
        for step in node["postprocess"]:
            steps.append(f"{node['id']}:post:{step}")
    return steps


def copy_graph(graph: dict[str, Any]) -> dict[str, Any]:
    return copy.deepcopy(graph)

