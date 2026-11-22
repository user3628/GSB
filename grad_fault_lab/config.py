from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import AppError, ErrorCode


@dataclass(slots=True, frozen=True)
class NodeConfig:
    node_id: str
    port: int
    refused_port: int
    sample_weights: tuple[int, ...]


@dataclass(slots=True, frozen=True)
class RoundPolicy:
    on_exhaustion: str = "shorten"
    min_participants: int = 2

    def validate(self) -> None:
        if self.on_exhaustion not in {"shorten", "abort"}:
            raise ValueError("on_exhaustion must be 'shorten' or 'abort'")
        if self.min_participants < 1:
            raise ValueError("min_participants must be at least 1")


@dataclass(slots=True, frozen=True)
class LabConfig:
    host: str
    node_port_base: int
    injector_port: int
    validator_port: int
    database_path: Path
    artifact_dir: Path
    seed: int
    tensor_shape: tuple[int, ...]
    shards_per_node: int
    nodes: tuple[NodeConfig, ...]
    round_policies: dict[int, RoundPolicy] = field(default_factory=dict)
    default_round_policy: RoundPolicy = RoundPolicy()
    socket_timeout_seconds: float = 0.25
    hang_seconds: float = 30.0
    request_timeout_seconds: float = 8.0

    def __post_init__(self) -> None:
        if self.host != "127.0.0.1":
            raise ValueError("The lab only permits the IPv4 loopback address 127.0.0.1")
        if self.shards_per_node < 2:
            raise ValueError("shards_per_node must be at least 2 for ordering faults")
        if not self.tensor_shape or any(dim <= 0 for dim in self.tensor_shape):
            raise ValueError("tensor_shape must contain positive dimensions")
        if len({node.node_id for node in self.nodes}) != len(self.nodes):
            raise ValueError("node_id values must be unique")
        if self.default_round_policy.on_exhaustion not in {"shorten", "abort"}:
            raise ValueError("default_round_policy.on_exhaustion is invalid")

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(node.node_id for node in self.nodes)

    def node(self, node_id: str) -> NodeConfig:
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise AppError(
            ErrorCode.INJECTION_SPEC_INVALID,
            "Target node is not registered",
            {"target_node": node_id, "registered_nodes": list(self.node_ids)},
        )

    def policy_for_round(self, round_no: int) -> RoundPolicy:
        return self.round_policies.get(round_no, self.default_round_policy)

    @classmethod
    def load(cls, path: str | Path) -> "LabConfig":
        config_path = Path(path).resolve()
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unable to load config {config_path}: {exc}") from exc

        nodes = tuple(
            NodeConfig(
                node_id=str(item["node_id"]),
                port=int(item["port"]),
                refused_port=int(item["refused_port"]),
                sample_weights=tuple(int(value) for value in item["sample_weights"]),
            )
            for item in payload["nodes"]
        )
        raw_policies = payload.get("round_policies", {})
        policies = {
            int(round_no): RoundPolicy(
                on_exhaustion=str(value.get("on_exhaustion", "shorten")),
                min_participants=int(value.get("min_participants", 2)),
            )
            for round_no, value in raw_policies.items()
        }
        for policy in policies.values():
            policy.validate()
        default_raw = payload.get("default_round_policy", {})
        default_policy = RoundPolicy(
            on_exhaustion=str(default_raw.get("on_exhaustion", "shorten")),
            min_participants=int(default_raw.get("min_participants", 2)),
        )
        default_policy.validate()

        base_dir = config_path.parent
        config = cls(
            host=str(payload.get("host", "127.0.0.1")),
            node_port_base=int(payload.get("node_port_base", 18101)),
            injector_port=int(payload["injector_port"]),
            validator_port=int(payload["validator_port"]),
            database_path=(base_dir / payload["database"]).resolve(),
            artifact_dir=(base_dir / payload["artifact_dir"]).resolve(),
            seed=int(payload["seed"]),
            tensor_shape=tuple(int(value) for value in payload["tensor_shape"]),
            shards_per_node=int(payload.get("shards_per_node", 2)),
            nodes=nodes,
            round_policies=policies,
            default_round_policy=default_policy,
            socket_timeout_seconds=float(payload.get("socket_timeout_seconds", 0.25)),
            hang_seconds=float(payload.get("hang_seconds", 30.0)),
            request_timeout_seconds=float(payload.get("request_timeout_seconds", 8.0)),
        )
        if config.shards_per_node > min(len(node.sample_weights) for node in config.nodes):
            raise ValueError("Every node must have a sample weight for every shard")
        return config

    def public_dict(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "injector_port": self.injector_port,
            "validator_port": self.validator_port,
            "database_path": str(self.database_path),
            "artifact_dir": str(self.artifact_dir),
            "seed": self.seed,
            "tensor_shape": list(self.tensor_shape),
            "shards_per_node": self.shards_per_node,
            "nodes": [
                {"node_id": node.node_id, "port": node.port, "refused_port": node.refused_port}
                for node in self.nodes
            ],
            "round_policies": {
                str(round_no): {
                    "on_exhaustion": policy.on_exhaustion,
                    "min_participants": policy.min_participants,
                }
                for round_no, policy in self.round_policies.items()
            },
        }

