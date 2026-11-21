"""Snapshot loading and whole-replacement/rollback validation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import config_fingerprint
from .errors import (
    E_INDEX_NOT_READY,
    E_SNAPSHOT_MISMATCH,
    E_SNAPSHOT_REGRESSION,
    error,
)
from .util import digest_obj, read_json


def _validate_representation_integrity(representation: dict[str, Any], summary: dict[str, Any]) -> None:
    summary_items = summary.get("items", [])
    representation_items = representation.get("items", [])
    if len(summary_items) != len(representation_items):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Representation item count does not match its summary",
            stage="index",
            details={"summary_count": len(summary_items), "representation_count": len(representation_items)},
        )
    for expected, entry in zip(summary_items, representation_items):
        if expected.get("item_id") != entry.get("item_id"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Representation item order or identity differs from its summary",
                stage="index",
                item_id=entry.get("item_id"),
                details={"expected": expected.get("item_id"), "actual": entry.get("item_id")},
            )
        item_id = entry["item_id"]
        entry_digest_source = {key: value for key, value in entry.items() if key != "item_digest"}
        if digest_obj(entry_digest_source) != expected.get("item_digest"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Representation entry digest mismatch",
                stage="index",
                item_id=item_id,
                details={},
            )
        if digest_obj(entry.get("visual_q")) != expected.get("visual_q_sha256"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Representation visual vector digest mismatch",
                stage="index",
                item_id=item_id,
                details={"route": "visual"},
            )
        if digest_obj(entry.get("text_q")) != expected.get("text_q_sha256"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Representation text vector digest mismatch",
                stage="index",
                item_id=item_id,
                details={"route": "text"},
            )
    quantization = representation.get("quantization", {})
    if quantization.get("granularity") == "per_dim":
        parameter_material = quantization
    else:
        parameter_material = [
            {
                "item_id": entry["item_id"],
                "visual": entry.get("visual_quantization"),
                "text": entry.get("text_quantization"),
            }
            for entry in representation_items
        ]
    parameter_digest = digest_obj(parameter_material)
    if parameter_digest != summary.get("quantization", {}).get("parameter_digest"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Quantization parameter digest mismatch",
            stage="index",
            details={"actual": parameter_digest},
        )
    if parameter_digest != representation.get("quantization_parameter_digest"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Representation quantization parameter digest mismatch",
            stage="index",
            details={"actual": parameter_digest},
        )


def _validate_index_integrity(index: dict[str, Any], summary: dict[str, Any]) -> None:
    summary_dense = summary.get("dense", [])
    index_dense = index.get("dense", [])
    if len(summary_dense) != len(index_dense):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Dense index item count does not match its summary",
            stage="index",
            details={"summary_count": len(summary_dense), "index_count": len(index_dense)},
        )
    for expected, entry in zip(summary_dense, index_dense):
        item_id = entry.get("item_id")
        if item_id != expected.get("item_id"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Dense index item order or identity mismatch",
                stage="index",
                item_id=item_id,
                details={"expected": expected.get("item_id")},
            )
        if digest_obj(entry.get("visual_q")) != expected.get("visual_q_sha256"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Dense index visual vector digest mismatch",
                stage="index",
                item_id=item_id,
                details={"route": "visual"},
            )
        if digest_obj(entry.get("text_q")) != expected.get("text_q_sha256"):
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Dense index text vector digest mismatch",
                stage="index",
                item_id=item_id,
                details={"route": "text"},
            )
    postings_digest = digest_obj(
        {
            "text": index.get("text_inverted"),
            "visual": index.get("visual_inverted"),
            "text_latent": index.get("text_latent_inverted"),
            "items": index.get("item_postings"),
        }
    )
    if postings_digest != summary.get("postings_digest"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Inverted index postings digest mismatch",
            stage="index",
            details={"actual": postings_digest},
        )


@dataclass
class Snapshot:
    representation: dict[str, Any]
    reference: dict[str, Any]
    index: dict[str, Any]
    config: dict[str, Any]
    representation_summary: dict[str, Any]
    index_summary: dict[str, Any]
    state: dict[str, Any]

    @property
    def representation_fingerprint(self) -> str:
        return self.state["representation_fingerprint"]

    @property
    def index_fingerprint(self) -> str:
        return self.state["index_fingerprint"]

    @property
    def snapshot_sequence(self) -> int:
        return int(self.state["snapshot_sequence"])


def _required(path: Path, *, stage: str) -> None:
    if not path.is_file():
        raise error(
            E_INDEX_NOT_READY,
            "Index snapshot is not ready",
            stage=stage,
            details={"missing": path.name},
        )


def load_snapshot(workspace: Any, *, require_active_config: bool = True) -> Snapshot:
    paths = {
        "representation": workspace.representation_path,
        "reference": workspace.reference_path,
        "index": workspace.index_path,
        "config": workspace.config_snapshot_path,
        "representation_summary": workspace.representation_summary_path,
        "index_summary": workspace.index_summary_path,
        "state": workspace.snapshot_state_path,
    }
    for label, path in paths.items():
        _required(path, stage="index")
    representation = read_json(paths["representation"])
    reference = read_json(paths["reference"])
    index = read_json(paths["index"])
    config = read_json(paths["config"])
    representation_summary = read_json(paths["representation_summary"])
    index_summary = read_json(paths["index_summary"])
    state = read_json(paths["state"])

    state_seq = int(state.get("snapshot_sequence", -1))
    if int(representation.get("snapshot_sequence", -2)) < state_seq:
        raise error(
            E_SNAPSHOT_REGRESSION,
            "Representation snapshot sequence is older than the workspace high-water mark",
            stage="index",
            details={
                "representation_sequence": representation.get("snapshot_sequence"),
                "high_water_sequence": state_seq,
            },
        )
    if int(index.get("snapshot_sequence", -2)) < state_seq:
        raise error(
            E_SNAPSHOT_REGRESSION,
            "Index snapshot sequence is older than the workspace high-water mark",
            stage="index",
            details={
                "index_sequence": index.get("snapshot_sequence"),
                "high_water_sequence": state_seq,
            },
        )
    build_state_path = workspace.build_state_path
    if build_state_path.exists():
        build_state = read_json(build_state_path)
        if int(build_state.get("snapshot_sequence", -2)) > state_seq:
            raise error(
                E_SNAPSHOT_REGRESSION,
                "Published build state is newer than the active snapshot",
                stage="index",
                details={
                    "build_state_sequence": build_state.get("snapshot_sequence"),
                    "active_sequence": state_seq,
                },
            )
    if (
        int(representation.get("snapshot_sequence", -2)) != state_seq
        or int(index.get("snapshot_sequence", -2)) != state_seq
    ):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Representation and index are from different snapshots",
            stage="index",
            details={
                "state_sequence": state_seq,
                "representation_sequence": representation.get("snapshot_sequence"),
                "index_sequence": index.get("snapshot_sequence"),
            },
        )

    actual_representation_digest = digest_obj(representation_summary)
    actual_index_digest = digest_obj(index_summary)
    if actual_representation_digest != state.get("representation_fingerprint"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Representation summary does not match the published fingerprint",
            stage="index",
            details={
                "expected": state.get("representation_fingerprint"),
                "actual": actual_representation_digest,
            },
        )
    if actual_index_digest != state.get("index_fingerprint"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Index summary does not match the published fingerprint",
            stage="index",
            details={"expected": state.get("index_fingerprint"), "actual": actual_index_digest},
        )
    if index.get("representation_fingerprint") != state.get("representation_fingerprint"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Index references a different representation snapshot",
            stage="index",
            details={
                "index_representation": index.get("representation_fingerprint"),
                "state_representation": state.get("representation_fingerprint"),
            },
        )
    _validate_representation_integrity(representation, representation_summary)
    _validate_index_integrity(index, index_summary)
    snapshot_config_fingerprint = config_fingerprint(config)
    if config.get("name") is None or snapshot_config_fingerprint != state.get("config_fingerprint"):
        raise error(
            E_SNAPSHOT_MISMATCH,
            "Snapshot configuration fingerprint mismatch",
            stage="index",
            details={
                "config_fingerprint": snapshot_config_fingerprint,
                "state_config_fingerprint": state.get("config_fingerprint"),
            },
        )
    if require_active_config:
        active = workspace.active_config()
        if config_fingerprint(active) != snapshot_config_fingerprint:
            raise error(
                E_SNAPSHOT_MISMATCH,
                "Active configuration differs from the published index configuration",
                stage="index",
                details={
                    "active_config_fingerprint": config_fingerprint(active),
                    "snapshot_config_fingerprint": snapshot_config_fingerprint,
                },
            )
    return Snapshot(
        representation=representation,
        reference=reference,
        index=index,
        config=config,
        representation_summary=representation_summary,
        index_summary=index_summary,
        state=state,
    )

