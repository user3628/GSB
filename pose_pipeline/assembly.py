"""Stage 3: skeleton assembly and geometric consistency filtering."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from typing import Any

from .config import PipelineConfig
from .constants import ARTIFACT_ROUND_DIGITS, ASSEMBLY, SCHEMA_VERSION
from .errors import pose_invalid
from .util import digest_json, finalize_artifact, round_float


def assemble_poses(
    decode_artifact: dict[str, Any],
    *,
    shard_id: str,
    keypoint_names: tuple[str, ...],
    skeleton: tuple[tuple[str, str], ...],
    width: int,
    height: int,
    coordinate_convention: str,
    config: PipelineConfig,
    input_fingerprint: str,
) -> dict[str, Any]:
    known = set(keypoint_names)
    unknown_edges: list[dict[str, Any]] = []
    for left, right in skeleton:
        if left not in known or right not in known:
            unknown_edges.append(
                {
                    "connection": [left, right],
                    "unknown_keypoints": [name for name in (left, right) if name not in known],
                }
            )
    if unknown_edges:
        raise pose_invalid(
            "skeleton contains unknown keypoint names",
            shard_id=shard_id,
            details={"unknown_skeleton_connections": unknown_edges},
        )
    if not isinstance(decode_artifact.get("candidates"), list):
        raise pose_invalid(
            "decode artifact has no explicit candidate list",
            shard_id=shard_id,
            details={"reason": "malformed_decode_artifact"},
        )

    nodes: list[dict[str, Any]] = []
    by_keypoint: dict[str, list[int]] = defaultdict(list)
    for record in decode_artifact.get("keypoints", []):
        keypoint = str(record.get("keypoint"))
        if keypoint not in known:
            raise pose_invalid(
                "candidate keypoint is not declared by the manifest",
                shard_id=shard_id,
                details={"unknown_keypoint": keypoint},
            )
        for candidate in record.get("candidates", []):
            index = len(nodes)
            nodes.append(dict(candidate, keypoint=keypoint))
            by_keypoint[keypoint].append(index)

    adjacency: dict[int, set[int]] = {index: set() for index in range(len(nodes))}
    diagonal = math.hypot(max(1, width - 1), max(1, height - 1))
    max_edge = config.association_max_edge_fraction * diagonal
    for left, right in skeleton:
        for left_index in by_keypoint.get(left, []):
            for right_index in by_keypoint.get(right, []):
                if _distance(nodes[left_index], nodes[right_index]) <= max_edge:
                    adjacency[left_index].add(right_index)
                    adjacency[right_index].add(left_index)

    components: list[list[int]] = []
    visited: set[int] = set()
    for start in range(len(nodes)):
        if start in visited:
            continue
        queue: deque[int] = deque([start])
        visited.add(start)
        component: list[int] = []
        while queue:
            current = queue.popleft()
            component.append(current)
            for neighbor in sorted(adjacency[current]):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)
        component.sort()
        components.append(component)

    components.sort(
        key=lambda component: (
            min(float(nodes[index]["pixel_y"]) for index in component),
            min(float(nodes[index]["pixel_x"]) for index in component),
            [str(nodes[index]["keypoint"]) for index in component],
        )
    )

    kept_instances: list[dict[str, Any]] = []
    excluded_instances: list[dict[str, Any]] = []
    for component_index, component in enumerate(components, start=1):
        counts: dict[str, int] = defaultdict(int)
        for index in component:
            counts[str(nodes[index]["keypoint"])] += 1
        duplicate_keypoints = sorted(name for name, count in counts.items() if count > 1)
        if duplicate_keypoints:
            raise pose_invalid(
                "assembly produced a multi-instance conflict: component contains duplicate keypoints",
                shard_id=shard_id,
                details={
                    "reason": "multi_instance_conflict",
                    "component_index": component_index,
                    "duplicate_keypoints": duplicate_keypoints,
                    "connections": [list(edge) for edge in skeleton],
                },
            )
        keypoints = {
            str(nodes[index]["keypoint"]): _public_candidate(nodes[index], config)
            for index in component
        }
        valid_count = len(keypoints)
        if valid_count < config.min_valid_keypoints:
            raise pose_invalid(
                "assembled instance has fewer valid keypoints than configured minimum",
                shard_id=shard_id,
                details={
                    "reason": "low_valid_keypoints",
                    "component_index": component_index,
                    "valid_keypoints": sorted(keypoints),
                    "valid_keypoint_count": valid_count,
                    "minimum": config.min_valid_keypoints,
                },
            )
        violations = _geometry_violations(
            keypoints,
            skeleton=skeleton,
            config=config,
        )
        instance_id = f"pose_{component_index:04d}"
        if violations:
            excluded_instances.append(
                {
                    "instance_id": instance_id,
                    "keypoints": sorted(keypoints),
                    "violations": violations,
                }
            )
            continue
        instance = {
            "instance_id": instance_id,
            "valid_keypoint_count": valid_count,
            "keypoints": dict(sorted(keypoints.items())),
            "confidence": round_float(
                sum(float(value["response"]) for value in keypoints.values())
                / max(1, valid_count),
                ARTIFACT_ROUND_DIGITS,
            ),
            "candidate_indices": component,
        }
        kept_instances.append(instance)

    _check_instance_separation(kept_instances, shard_id, width, height, config)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "stage": ASSEMBLY,
        "shard_id": shard_id,
        "input_fingerprint": input_fingerprint,
        "config_version": config.stage_version(ASSEMBLY),
        "upstream_digest": decode_artifact["artifact_digest"],
        "image": dict(decode_artifact["image"]),
        "coordinate_convention": coordinate_convention,
        "skeleton": [list(edge) for edge in skeleton],
        "association_max_edge_px": round_float(max_edge, ARTIFACT_ROUND_DIGITS),
        "instances": kept_instances,
        "excluded_instances": excluded_instances,
        "violations": [
            {
                "instance_id": item["instance_id"],
                **violation,
            }
            for item in excluded_instances
            for violation in item["violations"]
        ],
        "instance_count": len(kept_instances),
    }
    return finalize_artifact(payload)


def _public_candidate(candidate: dict[str, Any], config: PipelineConfig) -> dict[str, Any]:
    return {
        "pixel_x": round_float(float(candidate["pixel_x"]), ARTIFACT_ROUND_DIGITS),
        "pixel_y": round_float(float(candidate["pixel_y"]), ARTIFACT_ROUND_DIGITS),
        "normalized_x": round_float(float(candidate["normalized_x"]), ARTIFACT_ROUND_DIGITS),
        "normalized_y": round_float(float(candidate["normalized_y"]), ARTIFACT_ROUND_DIGITS),
        "response": round_float(float(candidate["response"]), ARTIFACT_ROUND_DIGITS),
    }


def _geometry_violations(
    keypoints: dict[str, dict[str, Any]],
    *,
    skeleton: tuple[tuple[str, str], ...],
    config: PipelineConfig,
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    lengths: dict[tuple[str, str], float] = {}
    for left, right in skeleton:
        if left not in keypoints or right not in keypoints:
            continue
        length = _distance(keypoints[left], keypoints[right])
        lengths[(left, right)] = length
        if length <= config.min_edge_length:
            violations.append(
                {
                    "type": "edge_length",
                    "connection": [left, right],
                    "observed_length_px": round_float(length, ARTIFACT_ROUND_DIGITS),
                    "deviation_px": round_float(config.min_edge_length - length, ARTIFACT_ROUND_DIGITS),
                }
            )

    neighbors: dict[str, set[str]] = defaultdict(set)
    for left, right in skeleton:
        if left in keypoints and right in keypoints:
            neighbors[left].add(right)
            neighbors[right].add(left)
    for joint in sorted(neighbors):
        adjacent = sorted(neighbors[joint])
        for first_index in range(len(adjacent)):
            for second_index in range(first_index + 1, len(adjacent)):
                first = adjacent[first_index]
                second = adjacent[second_index]
                angle = _joint_angle(keypoints[joint], keypoints[first], keypoints[second])
                deviation = 0.0
                if angle < config.min_joint_angle:
                    deviation = config.min_joint_angle - angle
                elif angle > config.max_joint_angle:
                    deviation = angle - config.max_joint_angle
                if deviation > 0:
                    violations.append(
                        {
                            "type": "joint_angle",
                            "joint": joint,
                            "connection": [[joint, first], [joint, second]],
                            "observed_angle_degrees": round_float(angle, ARTIFACT_ROUND_DIGITS),
                            "allowed_interval": [config.min_joint_angle, config.max_joint_angle],
                            "deviation_degrees": round_float(deviation, ARTIFACT_ROUND_DIGITS),
                        }
                    )

    length_lookup: dict[frozenset[str], float] = {
        frozenset((left, right)): length for (left, right), length in lengths.items()
    }
    for first_edge, second_edge in config.symmetry_edge_pairs:
        first_key = frozenset(first_edge)
        second_key = frozenset(second_edge)
        if first_key not in length_lookup or second_key not in length_lookup:
            continue
        first_length = length_lookup[first_key]
        second_length = length_lookup[second_key]
        denominator = max(first_length, second_length, config.min_edge_length)
        ratio_deviation = abs(first_length - second_length) / denominator
        if ratio_deviation > config.symmetry_ratio_tolerance:
            violations.append(
                {
                    "type": "symmetry_ratio",
                    "connection": [list(first_edge), list(second_edge)],
                    "lengths_px": [
                        round_float(first_length, ARTIFACT_ROUND_DIGITS),
                        round_float(second_length, ARTIFACT_ROUND_DIGITS),
                    ],
                    "ratio_deviation": round_float(ratio_deviation, ARTIFACT_ROUND_DIGITS),
                    "allowed_ratio_deviation": config.symmetry_ratio_tolerance,
                }
            )
    violations.sort(key=lambda item: digest_json(item))
    return violations


def _check_instance_separation(
    instances: list[dict[str, Any]],
    shard_id: str,
    width: int,
    height: int,
    config: PipelineConfig,
) -> None:
    if len(instances) < 2:
        return
    diagonal = math.hypot(max(1, width - 1), max(1, height - 1))
    threshold = config.multi_instance_min_separation_fraction * diagonal
    centroids = {
        instance["instance_id"]: (
            sum(float(point["pixel_x"]) for point in instance["keypoints"].values())
            / len(instance["keypoints"]),
            sum(float(point["pixel_y"]) for point in instance["keypoints"].values())
            / len(instance["keypoints"]),
        )
        for instance in instances
    }
    ordered = sorted(centroids)
    for first_index, first in enumerate(ordered):
        for second in ordered[first_index + 1 :]:
            distance = math.dist(centroids[first], centroids[second])
            if distance < threshold:
                raise pose_invalid(
                    "assembly produced a multi-instance conflict: instance centres overlap",
                    shard_id=shard_id,
                    details={
                        "reason": "multi_instance_conflict",
                        "instances": [first, second],
                        "centroid_distance_px": round_float(distance, ARTIFACT_ROUND_DIGITS),
                        "minimum_distance_px": round_float(threshold, ARTIFACT_ROUND_DIGITS),
                    },
                )


def _distance(first: dict[str, Any], second: dict[str, Any]) -> float:
    return math.hypot(
        float(first["pixel_x"]) - float(second["pixel_x"]),
        float(first["pixel_y"]) - float(second["pixel_y"]),
    )


def _joint_angle(joint: dict[str, Any], first: dict[str, Any], second: dict[str, Any]) -> float:
    ax = float(first["pixel_x"]) - float(joint["pixel_x"])
    ay = float(first["pixel_y"]) - float(joint["pixel_y"])
    bx = float(second["pixel_x"]) - float(joint["pixel_x"])
    by = float(second["pixel_y"]) - float(joint["pixel_y"])
    denominator = math.hypot(ax, ay) * math.hypot(bx, by)
    if denominator <= 0:
        return 0.0
    cosine = max(-1.0, min(1.0, (ax * bx + ay * by) / denominator))
    return math.degrees(math.acos(cosine))
