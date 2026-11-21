"""Deterministic staged build, incremental reuse, checkpoints, and publication."""

from __future__ import annotations

import contextlib
import fcntl
import math
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .checkpoint import STAGES, CheckpointStore
from .config import (
    config_fingerprint,
    feature_fingerprint,
    index_fingerprint,
    projection_quantization_fingerprint,
    quantization_fingerprint,
)
from .errors import E_INVALID_STAGE, E_NUMERIC, E_SNAPSHOT_MISMATCH, error
from .features import (
    extract_text_features,
    extract_visual_features,
    feature_source_fingerprint,
    normalize_caption,
    tokenize,
)
from .quantization import (
    build_corpus_quantization,
    build_per_vector_spec,
    decimal_cosine,
    decimal_text,
    decimal_to_float,
    decimal_vector,
    dequantize_vector,
    project_and_normalize,
    quantize_vector,
    similarity_error_bound_decimal,
    vector_error_envelope,
)
from .snapshot import load_snapshot
from .util import digest_obj, read_json, sha256_file, write_atomic, write_json_atomic


def _vector_strings(values: Iterable[float]) -> list[str]:
    return decimal_vector(values)


def _vector_from_strings(values: Iterable[str]) -> np.ndarray:
    return np.asarray([float(value) for value in values], dtype=np.float64)


def _latent_tokens(quantized: list[int], count: int) -> list[str]:
    order = sorted(range(len(quantized)), key=lambda index: (-abs(quantized[index]), index))
    tokens: list[str] = []
    for index in order:
        if len(tokens) >= count:
            break
        value = int(quantized[index])
        if value == 0:
            continue
        tokens.append(f"v{index:02d}:{'p' if value > 0 else 'n'}")
    return tokens


class Pipeline:
    def __init__(self, workspace: Any) -> None:
        self.workspace = workspace

    @contextlib.contextmanager
    def _publish_lock(self):
        self.workspace.locks_dir.mkdir(parents=True, exist_ok=True)
        with open(self.workspace.publish_lock_path, "a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _load_previous_build_state(self) -> dict[str, Any] | None:
        if not self.workspace.build_state_path.exists():
            return None
        try:
            return read_json(self.workspace.build_state_path)
        except Exception:
            return None

    def _version_base(
        self,
        *,
        config: dict[str, Any],
        manifest_sha: str,
        feature_corpus_digest: str,
    ) -> str:
        return digest_obj(
            {
                "config_fingerprint": config_fingerprint(config),
                "feature_fingerprint": feature_fingerprint(config),
                "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
                "index_config_fingerprint": index_fingerprint(config),
                "manifest_sha256": manifest_sha,
                "feature_corpus_digest": feature_corpus_digest,
            }
        )

    def _compute_features(
        self,
        *,
        config: dict[str, Any],
        items: list[dict[str, Any]],
        reuse_cache: bool,
    ) -> dict[str, Any]:
        feature_fp = feature_fingerprint(config)
        previous_payload: dict[str, Any] | None = None
        if reuse_cache and self.workspace.features_path.exists():
            try:
                previous_payload = read_json(self.workspace.features_path)
            except Exception:
                previous_payload = None
        if previous_payload and previous_payload.get("feature_config_fingerprint") != feature_fp:
            previous_payload = None
        previous_items = previous_payload.get("items", {}) if previous_payload else {}
        feature_items: dict[str, Any] = {}
        for item in items:
            item_id = item["item_id"]
            caption_normalized = normalize_caption(item["caption"], item_id=item_id)
            image_path = self.workspace.root / item["image_file"]
            image_sha = sha256_file(image_path)
            source_fp = feature_source_fingerprint(
                feature_config_fingerprint=feature_fp,
                image_sha256=image_sha,
                normalized_caption=caption_normalized,
                source=item["source"],
                lang=item["lang"],
            )
            previous_item = previous_items.get(item_id)
            if previous_item and previous_item.get("source_fingerprint") == source_fp:
                visual_vector = _vector_from_strings(previous_item["visual_vector"])
                text_vector = _vector_from_strings(previous_item["text_vector"])
                visual_summary = previous_item["visual_summary"]
                text_summary = previous_item["text_summary"]
            else:
                visual = extract_visual_features(image_path, config["feature"], item_id=item_id)
                text = extract_text_features(item["caption"], config["feature"], item_id=item_id)
                visual_vector = visual["vector"]
                text_vector = text["vector"]
                visual_summary = visual["summary"]
                text_summary = text["summary"]
            feature_digest = digest_obj(
                {
                    "item_id": item_id,
                    "source_fingerprint": source_fp,
                    "visual_vector": _vector_strings(visual_vector),
                    "text_vector": _vector_strings(text_vector),
                    "visual_summary": visual_summary,
                    "text_summary": text_summary,
                }
            )
            feature_items[item_id] = {
                "item_id": item_id,
                "source": item["source"],
                "lang": item["lang"],
                "image_sha256": image_sha,
                "caption_original": item["caption"],
                "caption_normalized": caption_normalized,
                "source_fingerprint": source_fp,
                "visual_vector": _vector_strings(visual_vector),
                "text_vector": _vector_strings(text_vector),
                "visual_summary": visual_summary,
                "text_summary": text_summary,
                "feature_digest": feature_digest,
            }
        corpus_digest = digest_obj(
            [
                {
                    "item_id": item_id,
                    "source_fingerprint": payload["source_fingerprint"],
                    "feature_digest": payload["feature_digest"],
                }
                for item_id, payload in sorted(feature_items.items())
            ]
        )
        payload = {
            "format": "shardseek-features-v1",
            "feature_config_fingerprint": feature_fp,
            "items": feature_items,
            "feature_corpus_digest": corpus_digest,
        }
        write_json_atomic(self.workspace.features_path, payload, pretty=True)
        return payload

    def _load_reused_representation(self, previous: dict[str, Any], feature_corpus_digest: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
        if previous.get("feature_corpus_digest") != feature_corpus_digest:
            return None
        try:
            representation = read_json(self.workspace.representation_path)
            reference = read_json(self.workspace.reference_path)
            summary = read_json(self.workspace.representation_summary_path)
        except Exception:
            return None
        if digest_obj(summary) != previous.get("representation_fingerprint"):
            return None
        if representation.get("representation_fingerprint") != previous.get("representation_fingerprint"):
            return None
        return representation, reference, summary

    def _build_representation(
        self,
        *,
        config: dict[str, Any],
        features: dict[str, Any],
        planned_snapshot_sequence: int,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        items = sorted(features["items"].values(), key=lambda item: item["item_id"])
        visual_refs: list[np.ndarray] = []
        text_refs: list[np.ndarray] = []
        for item in items:
            visual_refs.append(
                project_and_normalize(
                    _vector_from_strings(item["visual_vector"]),
                    config["projection"]["visual"],
                    route="visual",
                    item_id=item["item_id"],
                )
            )
            text_refs.append(
                project_and_normalize(
                    _vector_from_strings(item["text_vector"]),
                    config["projection"]["text"],
                    route="text",
                    item_id=item["item_id"],
                )
            )
        quant_block = build_corpus_quantization(
            visual_refs,
            text_refs,
            bits=int(config["quantization"]["bits"]),
            granularity=config["quantization"]["granularity"],
        )
        representation_items: list[dict[str, Any]] = []
        reference_items: dict[str, Any] = {}
        summary_items: list[dict[str, Any]] = []
        parameter_material: list[Any] = []
        for item, visual_ref, text_ref in zip(items, visual_refs, text_refs):
            item_id = item["item_id"]
            if quant_block["granularity"] == "per_dim":
                visual_spec = quant_block["visual"]
                text_spec = quant_block["text"]
            else:
                visual_spec = build_per_vector_spec(
                    visual_ref,
                    bits=int(config["quantization"]["bits"]),
                    modality="visual",
                )
                text_spec = build_per_vector_spec(
                    text_ref,
                    bits=int(config["quantization"]["bits"]),
                    modality="text",
                )
            visual_q = quantize_vector(visual_ref, visual_spec, item_id=item_id)
            text_q = quantize_vector(text_ref, text_spec, item_id=item_id)
            visual_error = vector_error_envelope(
                visual_ref,
                dequantize_vector(visual_q, visual_spec),
                visual_spec,
            )
            text_error = vector_error_envelope(
                text_ref,
                dequantize_vector(text_q, text_spec),
                text_spec,
            )
            entry: dict[str, Any] = {
                "item_id": item_id,
                "source": item["source"],
                "lang": item["lang"],
                "image_sha256": item["image_sha256"],
                "caption_normalized": item["caption_normalized"],
                "visual_q": visual_q,
                "text_q": text_q,
                "visual_error_l2_bound": visual_error,
                "text_error_l2_bound": text_error,
            }
            if quant_block["granularity"] == "per_vector":
                entry["visual_quantization"] = visual_spec
                entry["text_quantization"] = text_spec
                parameter_material.append({"item_id": item_id, "visual": visual_spec, "text": text_spec})
            item_digest = digest_obj(entry)
            entry["item_digest"] = item_digest
            representation_items.append(entry)
            reference_items[item_id] = {
                "visual": _vector_strings(visual_ref),
                "text": _vector_strings(text_ref),
            }
            summary_items.append(
                {
                    "item_id": item_id,
                    "source": item["source"],
                    "image_sha256": item["image_sha256"],
                    "caption_sha256": digest_obj(item["caption_normalized"]),
                    "visual_q_sha256": digest_obj(visual_q),
                    "text_q_sha256": digest_obj(text_q),
                    "item_digest": item_digest,
                }
            )
        if quant_block["granularity"] == "per_dim":
            parameter_material = quant_block
        parameter_digest = digest_obj(parameter_material)
        summary = {
            "format": "shardseek-representation-summary-v1",
            "feature_fingerprint": feature_fingerprint(config),
            "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
            "feature_corpus_digest": features["feature_corpus_digest"],
            "quantization": {
                "bits": int(config["quantization"]["bits"]),
                "granularity": config["quantization"]["granularity"],
                "parameter_digest": parameter_digest,
            },
            "items": summary_items,
        }
        representation_fingerprint = digest_obj(summary)
        representation = {
            "format": "shardseek-representations-v1",
            "snapshot_sequence": planned_snapshot_sequence,
            "config_fingerprint": config_fingerprint(config),
            "feature_fingerprint": feature_fingerprint(config),
            "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
            "feature_corpus_digest": features["feature_corpus_digest"],
            "quantization": quant_block,
            "quantization_parameter_digest": parameter_digest,
            "representation_fingerprint": representation_fingerprint,
            "items": representation_items,
        }
        reference = {
            "format": "shardseek-reference-vectors-v1",
            "purpose": "diagnostic_only_not_used_for_ranking",
            "config_fingerprint": config_fingerprint(config),
            "representation_fingerprint": representation_fingerprint,
            "vectors": reference_items,
        }
        return representation, reference, summary, quant_block

    def _build_index(
        self,
        *,
        config: dict[str, Any],
        representation: dict[str, Any],
        representation_summary: dict[str, Any],
        features: dict[str, Any],
        planned_snapshot_sequence: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        representation_fingerprint = digest_obj(representation_summary)
        token_count = int(config["index"]["visual_token_count"])
        dense_entries: list[dict[str, Any]] = []
        text_postings: dict[str, list[str]] = {}
        visual_token_postings: dict[str, list[str]] = {}
        text_latent_postings: dict[str, list[str]] = {}
        item_postings: dict[str, Any] = {}
        for entry in sorted(representation["items"], key=lambda value: value["item_id"]):
            item_id = entry["item_id"]
            feature_item = features["items"][item_id]
            caption_tokens = tokenize(feature_item["caption_normalized"])
            visual_tokens = _latent_tokens(entry["visual_q"], token_count)
            text_latent = _latent_tokens(entry["text_q"], token_count)
            dense_entry = {
                "item_id": item_id,
                "source": entry["source"],
                "visual_q": entry["visual_q"],
                "text_q": entry["text_q"],
                "visual_error_l2_bound": entry["visual_error_l2_bound"],
                "text_error_l2_bound": entry["text_error_l2_bound"],
            }
            if "visual_quantization" in entry:
                dense_entry["visual_quantization"] = entry["visual_quantization"]
                dense_entry["text_quantization"] = entry["text_quantization"]
            dense_entries.append(dense_entry)
            for token in sorted(set(caption_tokens)):
                text_postings.setdefault(token, []).append(item_id)
            for token in sorted(set(visual_tokens)):
                visual_token_postings.setdefault(token, []).append(item_id)
            for token in sorted(set(text_latent)):
                text_latent_postings.setdefault(token, []).append(item_id)
            item_postings[item_id] = {
                "caption_tokens": sorted(set(caption_tokens)),
                "visual_tokens": sorted(set(visual_tokens)),
                "text_latent_tokens": sorted(set(text_latent)),
            }
        for postings in (text_postings, visual_token_postings, text_latent_postings):
            for token in postings:
                postings[token] = sorted(set(postings[token]))
        postings_digest = digest_obj(
            {
                "text": text_postings,
                "visual": visual_token_postings,
                "text_latent": text_latent_postings,
                "items": item_postings,
            }
        )
        summary = {
            "format": "shardseek-index-summary-v1",
            "config_fingerprint": config_fingerprint(config),
            "index_config_fingerprint": index_fingerprint(config),
            "representation_fingerprint": representation_fingerprint,
            "ranking": config["ranking"],
            "index_config": config["index"],
            "postings_digest": postings_digest,
            "item_order": [entry["item_id"] for entry in dense_entries],
            "dense": [
                {
                    "item_id": entry["item_id"],
                    "visual_q_sha256": digest_obj(entry["visual_q"]),
                    "text_q_sha256": digest_obj(entry["text_q"]),
                }
                for entry in dense_entries
            ],
        }
        index_fingerprint_digest = digest_obj(summary)
        index = {
            "format": "shardseek-index-v1",
            "snapshot_sequence": planned_snapshot_sequence,
            "config_fingerprint": config_fingerprint(config),
            "index_config_fingerprint": index_fingerprint(config),
            "representation_fingerprint": representation_fingerprint,
            "index_fingerprint": index_fingerprint_digest,
            "ranking": config["ranking"],
            "dense": dense_entries,
            "text_inverted": text_postings,
            "visual_inverted": visual_token_postings,
            "text_latent_inverted": text_latent_postings,
            "item_postings": item_postings,
        }
        return index, summary

    def _stage_artifacts(
        self,
        *,
        config: dict[str, Any],
        features: dict[str, Any],
        representation: dict[str, Any],
        reference: dict[str, Any],
        index: dict[str, Any],
        item_id: str,
    ) -> dict[str, Any]:
        feature_item = features["items"][item_id]
        rep_entry = next(entry for entry in representation["items"] if entry["item_id"] == item_id)
        ref_entry = reference["vectors"][item_id]
        quant_block = representation["quantization"]
        if quant_block["granularity"] == "per_dim":
            visual_spec = quant_block["visual"]
            text_spec = quant_block["text"]
        else:
            visual_spec = rep_entry["visual_quantization"]
            text_spec = rep_entry["text_quantization"]
        ref_cosine = decimal_cosine(ref_entry["text"], ref_entry["visual"])
        visual_dequant = dequantize_vector(rep_entry["visual_q"], visual_spec)
        text_dequant = dequantize_vector(rep_entry["text_q"], text_spec)
        dequant_cosine = decimal_cosine(
            decimal_vector(text_dequant),
            decimal_vector(visual_dequant),
        )
        actual_error = abs(dequant_cosine - ref_cosine)
        bound = similarity_error_bound_decimal(
            float(rep_entry["text_error_l2_bound"]),
            float(rep_entry["visual_error_l2_bound"]),
            ref_cosine,
        )
        if actual_error > bound:
            raise error(
                E_NUMERIC,
                "Observed quantization similarity error exceeds the declared bound",
                stage="recall_verify",
                item_id=item_id,
                details={
                    "reference_cosine": decimal_text(ref_cosine),
                    "dequantized_cosine": decimal_text(dequant_cosine),
                    "actual_error": decimal_text(actual_error),
                    "declared_bound": decimal_text(bound),
                },
            )
        recall = {
            "item_id": item_id,
            "reference_cosine": decimal_text(ref_cosine),
            "dequantized_cosine": decimal_text(dequant_cosine),
            "actual_error": decimal_text(actual_error),
            "declared_bound": decimal_text(bound),
            "within_bound": True,
        }
        postings = index["item_postings"][item_id]
        return {
            "feature_extract": {
                "input_fingerprint": feature_item["source_fingerprint"],
                "artifact": {"feature_digest": feature_item["feature_digest"]},
            },
            "project_quantize": {
                "input_fingerprint": digest_obj(
                    {
                        "feature_digest": feature_item["feature_digest"],
                        "projection_quantization": projection_quantization_fingerprint(config),
                    }
                ),
                "artifact": {
                    "item_digest": rep_entry["item_digest"],
                    "visual_q_sha256": digest_obj(rep_entry["visual_q"]),
                    "text_q_sha256": digest_obj(rep_entry["text_q"]),
                },
            },
            "index_build": {
                "input_fingerprint": digest_obj(
                    {
                        "item_digest": rep_entry["item_digest"],
                        "index_config_fingerprint": index_fingerprint(config),
                    }
                ),
                "artifact": {"postings_digest": digest_obj(postings), "postings": postings},
            },
            "recall_verify": {
                "input_fingerprint": digest_obj(
                    {
                        "postings_digest": digest_obj(postings),
                        "quantization_fingerprint": quantization_fingerprint(config),
                    }
                ),
                "artifact": recall,
            },
        }

    def build(
        self,
        *,
        mode: str = "incremental",
        stop_after_shards: int | None = None,
        config_name: str | Path | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if mode not in ("full", "incremental", "resume"):
            raise error(E_NUMERIC, "Unknown build mode", stage="build", details={"mode": mode})
        workspace = self.workspace
        workspace.init()
        if config_name is not None:
            workspace.set_config(config_name)
        config = workspace.active_config()
        imported = workspace.load_import()
        items = workspace.enabled_items()
        manifest_sha = imported["manifest_sha256"]
        workspace.clear_stop()
        force_new = mode == "full"
        reuse_cache = not force_new
        previous = None if force_new else self._load_previous_build_state()

        features = self._compute_features(config=config, items=items, reuse_cache=reuse_cache)
        feature_corpus_digest = features["feature_corpus_digest"]
        version_base = self._version_base(
            config=config,
            manifest_sha=manifest_sha,
            feature_corpus_digest=feature_corpus_digest,
        )
        checkpoint_store: CheckpointStore = workspace.checkpoint_store()
        existing_session = checkpoint_store.load_session()
        if (
            existing_session
            and existing_session.get("version_base") == version_base
            and not existing_session.get("completed")
            and not force_new
        ):
            start_stage = int(existing_session["start_stage"])
            snapshot_sequence = int(existing_session["snapshot_sequence"])
            earliest_stage = STAGES[start_stage - 1]
            resuming = True
        else:
            resuming = False
            if mode == "resume":
                raise error(
                    E_INVALID_STAGE,
                    "No resumable build session matches the current inputs and configuration",
                    stage="build",
                    details={"version_base": version_base},
                )
            if mode == "full" or previous is None:
                start_stage = 1
                earliest_stage = STAGES[0]
            else:
                previous_features = previous.get("item_feature_digests", {})
                current_features = {
                    item_id: payload["feature_digest"] for item_id, payload in features["items"].items()
                }
                if (
                    previous.get("feature_fingerprint") != feature_fingerprint(config)
                    or previous_features != current_features
                    or previous.get("manifest_sha256") != manifest_sha
                ):
                    start_stage = 1
                    earliest_stage = STAGES[0]
                elif previous.get("projection_quantization_fingerprint") != projection_quantization_fingerprint(config):
                    start_stage = 2
                    earliest_stage = STAGES[1]
                elif previous.get("index_config_fingerprint") != index_fingerprint(config):
                    start_stage = 3
                    earliest_stage = STAGES[2]
                elif previous.get("config_fingerprint") != config_fingerprint(config):
                    start_stage = 3
                    earliest_stage = STAGES[2]
                else:
                    snapshot = load_snapshot(workspace, require_active_config=False)
                    return {
                        "ok": True,
                        "changed": False,
                        "reused": True,
                        "earliest_stage": None,
                        "reused_stages": list(STAGES),
                        "snapshot_sequence": snapshot.snapshot_sequence,
                        "representation_fingerprint": snapshot.representation_fingerprint,
                        "index_fingerprint": snapshot.index_fingerprint,
                        "checkpoints": checkpoint_store.status(),
                    }
            if workspace.snapshot_state_path.exists():
                snapshot_sequence = int(read_json(workspace.snapshot_state_path).get("snapshot_sequence", 0)) + 1
            else:
                snapshot_sequence = 1

        reused_stages = list(STAGES[: start_stage - 1])
        reuse_representation = start_stage > 2 and previous is not None
        if reuse_representation:
            reused_rep = self._load_reused_representation(previous, feature_corpus_digest)
        else:
            reused_rep = None
        if reused_rep is None:
            representation, reference, representation_summary, _ = self._build_representation(
                config=config,
                features=features,
                planned_snapshot_sequence=snapshot_sequence,
            )
        else:
            representation, reference, representation_summary = reused_rep
            representation["snapshot_sequence"] = snapshot_sequence
            representation["config_fingerprint"] = config_fingerprint(config)
            reference["config_fingerprint"] = config_fingerprint(config)
            reference["representation_fingerprint"] = digest_obj(representation_summary)
            representation["representation_fingerprint"] = reference["representation_fingerprint"]

        if start_stage > 3 and previous is not None and previous.get("representation_fingerprint") == digest_obj(representation_summary):
            try:
                index = read_json(workspace.index_path)
                index_summary = read_json(workspace.index_summary_path)
                if digest_obj(index_summary) != previous.get("index_fingerprint"):
                    index = None
            except Exception:
                index = None
                index_summary = None
        else:
            index = None
            index_summary = None
        if index is None:
            index, index_summary = self._build_index(
                config=config,
                representation=representation,
                representation_summary=representation_summary,
                features=features,
                planned_snapshot_sequence=snapshot_sequence,
            )
        else:
            index["snapshot_sequence"] = snapshot_sequence

        representation_fingerprint = digest_obj(representation_summary)
        index_config_summary = digest_obj(index_summary)
        session = {
            "format": "shardseek-build-session-v1",
            "session_id": digest_obj(
                {
                    "version_base": version_base,
                    "start_stage": start_stage,
                    "snapshot_sequence": snapshot_sequence,
                }
            ),
            "version_base": version_base,
            "version_fingerprint": digest_obj(
                {
                    "version_base": version_base,
                    "representation_fingerprint": representation_fingerprint,
                    "index_fingerprint": index_config_summary,
                }
            ),
            "config_fingerprint": config_fingerprint(config),
            "feature_fingerprint": feature_fingerprint(config),
            "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
            "index_config_fingerprint": index_fingerprint(config),
            "quantization_fingerprint": quantization_fingerprint(config),
            "manifest_sha256": manifest_sha,
            "feature_corpus_digest": feature_corpus_digest,
            "representation_fingerprint": representation_fingerprint,
            "index_fingerprint": index_config_summary,
            "start_stage": start_stage,
            "snapshot_sequence": snapshot_sequence,
            "ordered_shards": [item["item_id"] for item in items],
            "expected_pairs": [
                [item["item_id"], stage_index]
                for item in items
                for stage_index in range(start_stage, 5)
            ],
            "completed": False,
        }
        checkpoint_store.initialize_session(session, force_new=force_new)
        published_records = checkpoint_store.read_records()
        expected_pairs = session["expected_pairs"]
        expected_by_shard: dict[str, list[int]] = {item["item_id"]: [] for item in items}
        for shard_id, stage_index in expected_pairs:
            expected_by_shard[shard_id].append(int(stage_index))
        published_stages = {(record["shard_id"], int(record["stage_index"])) for record in published_records}
        completed_shards = sum(
            1
            for item in items
            if all((item["item_id"], stage_index) in published_stages for stage_index in expected_by_shard[item["item_id"]])
        )
        stopped = False
        for item in items:
            item_id = item["item_id"]
            artifacts = self._stage_artifacts(
                config=config,
                features=features,
                representation=representation,
                reference=reference,
                index=index,
                item_id=item_id,
            )
            published_any = False
            for stage_index in expected_by_shard[item_id]:
                if (item_id, stage_index) in published_stages:
                    continue
                stage = STAGES[stage_index - 1]
                stage_data = artifacts[stage]
                checkpoint_store.publish(
                    {
                        "shard_id": item_id,
                        "stage": stage,
                        "stage_index": stage_index,
                        "input_fingerprint": stage_data["input_fingerprint"],
                        "config_version": config_fingerprint(config),
                        "quantization_fingerprint": quantization_fingerprint(config),
                        "artifact_digest": digest_obj(stage_data["artifact"]),
                    }
                )
                published_stages.add((item_id, stage_index))
                published_any = True
            if published_any and all(
                (item_id, stage_index) in published_stages for stage_index in expected_by_shard[item_id]
            ):
                completed_shards += 1
            if stop_after_shards is not None and completed_shards >= stop_after_shards:
                stopped = completed_shards < len(items)
                break
            if workspace.stop_requested() and completed_shards < len(items):
                stopped = True
                break

        checkpoint_status_before_publish = checkpoint_store.status()
        if checkpoint_status_before_publish["record_count"] < checkpoint_status_before_publish["expected_count"]:
            stopped = True
        if stopped:
            return {
                "ok": True,
                "changed": True,
                "stopped": True,
                "code": "E_BUILD_STOPPED",
                "earliest_stage": earliest_stage,
                "reused_stages": reused_stages,
                "session_id": session["session_id"],
                "snapshot_sequence": snapshot_sequence,
                "completed_shards": completed_shards,
                "total_shards": len(items),
                "checkpoints": checkpoint_store.status(),
                "message": "Stopped after completing the current indivisible shard.",
            }

        with self._publish_lock():
            if workspace.snapshot_state_path.exists():
                high_water = int(read_json(workspace.snapshot_state_path).get("snapshot_sequence", 0))
                if high_water >= snapshot_sequence:
                    raise error(
                        E_SNAPSHOT_MISMATCH,
                        "Another publisher advanced the snapshot sequence",
                        stage="publish",
                        details={"high_water": high_water, "planned": snapshot_sequence},
                    )
            write_json_atomic(workspace.config_snapshot_path, config, pretty=True)
            write_json_atomic(workspace.representation_summary_path, representation_summary)
            write_json_atomic(workspace.index_summary_path, index_summary)
            write_json_atomic(workspace.representation_path, representation, pretty=True)
            write_json_atomic(workspace.reference_path, reference, pretty=True)
            write_json_atomic(workspace.index_path, index, pretty=True)
            state = {
                "format": "shardseek-snapshot-state-v1",
                "snapshot_sequence": snapshot_sequence,
                "session_id": session["session_id"],
                "config_fingerprint": config_fingerprint(config),
                "quantization_fingerprint": quantization_fingerprint(config),
                "representation_fingerprint": representation_fingerprint,
                "index_fingerprint": index_config_summary,
                "published_checkpoint_seq": checkpoint_status_before_publish["last_seq"],
            }
            write_json_atomic(workspace.snapshot_state_path, state)
            build_state = {
                "format": "shardseek-build-state-v1",
                "published": True,
                "snapshot_sequence": snapshot_sequence,
                "session_id": session["session_id"],
                "version_fingerprint": session["version_fingerprint"],
                "config_fingerprint": config_fingerprint(config),
                "feature_fingerprint": feature_fingerprint(config),
                "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
                "index_config_fingerprint": index_fingerprint(config),
                "quantization_fingerprint": quantization_fingerprint(config),
                "manifest_sha256": manifest_sha,
                "feature_corpus_digest": feature_corpus_digest,
                "representation_fingerprint": representation_fingerprint,
                "index_fingerprint": index_config_summary,
                "item_feature_digests": {
                    item_id: payload["feature_digest"] for item_id, payload in features["items"].items()
                },
                "earliest_stage": earliest_stage,
                "reused_stages": reused_stages,
                "checkpoint_last_seq": checkpoint_status_before_publish["last_seq"],
            }
            write_json_atomic(workspace.build_state_path, build_state, pretty=True)
        checkpoint_store.mark_session_complete()
        return {
            "ok": True,
            "changed": True,
            "stopped": False,
            "earliest_stage": earliest_stage,
            "reused_stages": reused_stages,
            "session_id": session["session_id"],
            "version_fingerprint": session["version_fingerprint"],
            "snapshot_sequence": snapshot_sequence,
            "representation_fingerprint": representation_fingerprint,
            "index_fingerprint": index_config_summary,
            "quantization_fingerprint": quantization_fingerprint(config),
            "item_count": len(items),
            "checkpoints": checkpoint_store.status(),
            "artifact_sha256": {
                "representation_summary": representation_fingerprint,
                "index_summary": index_config_summary,
                "representation_file": sha256_file(workspace.representation_path),
                "index_file": sha256_file(workspace.index_path),
            },
        }

