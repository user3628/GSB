from __future__ import annotations

import hashlib
import math
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import atomic_write_json, digest_obj, read_json
from .errors import EngineError
from .layout import (
    layout_version,
    normalize_layout,
    planning_config_fingerprint,
    vector_config_fingerprint,
)
from .planner import membership_map, plan_index
from .records import (
    RecordScan,
    merge_record_dataset,
    record_summaries,
    scan_records,
    write_batch_record,
)
from .vectors import (
    build_retrieval_vector,
    normalize_text,
    validate_override,
    vector_digest,
    vector_source_key,
)
from .workspace import Workspace


def _attempt(value: Any, field: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise EngineError("E_INVALID_INPUT", f"{field} must be an integer", "request_validation") from exc
    if isinstance(value, bool) or result < 0:
        raise EngineError("E_INVALID_INPUT", f"{field} must be a non-negative integer", "request_validation")
    return result


def _annotate(error: EngineError, *, batch_seq: int | None, batch_id: str | None) -> EngineError:
    if error.batch_seq is None:
        error.batch_seq = batch_seq
    if error.batch_id is None:
        error.batch_id = batch_id
    return error


def _entry_index_map(index: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    if not index:
        return result
    for shard in index.get("shards", []):
        for entry in shard.get("entries", []):
            result[entry["text_id"]] = {**entry, "shard_id": shard["shard_id"]}
    return result


def _diff_shards(old_index: dict[str, Any] | None, new_index: dict[str, Any]) -> tuple[list[str], list[str]]:
    old = membership_map(old_index) if old_index else {}
    new = membership_map(new_index)
    invalidated: list[str] = []
    reused: list[str] = []
    for shard_id in sorted(set(old) | set(new)):
        old_value = old.get(shard_id)
        new_value = new.get(shard_id)
        if old_value is None or new_value is None:
            invalidated.append(shard_id)
            continue
        if (
            old_value["entry_ids"] == new_value["entry_ids"]
            and old_value["membership_digest"] == new_value["membership_digest"]
            and old_value["descriptor_digest"] == new_value["descriptor_digest"]
        ):
            reused.append(shard_id)
        else:
            invalidated.append(shard_id)
    return invalidated, reused


def _make_event(core: dict[str, Any]) -> dict[str, Any]:
    event = dict(core)
    event["event_id"] = digest_obj({"schema": "mmb-invalidation-event-v1", "event": event})
    return event


class Engine:
    def __init__(self, workspace: Workspace | Path | str) -> None:
        self.workspace = workspace if isinstance(workspace, Workspace) else Workspace(workspace)
        self.workspace.ensure()

    def _read_index(self) -> dict[str, Any] | None:
        if not self.workspace.index_path.exists():
            return None
        try:
            value = read_json(self.workspace.index_path)
        except (OSError, ValueError):
            return None
        if not isinstance(value, dict) or value.get("schema_version") != "index-v1":
            return None
        return value

    def _read_vector_cache(self) -> dict[str, Any]:
        if not self.workspace.vectors_path.exists():
            return {"schema_version": "vector-cache-v1", "vectors": {}}
        try:
            value = read_json(self.workspace.vectors_path)
        except (OSError, ValueError):
            return {"schema_version": "vector-cache-v1", "vectors": {}}
        if not isinstance(value, dict) or value.get("schema_version") != "vector-cache-v1":
            return {"schema_version": "vector-cache-v1", "vectors": {}}
        if not isinstance(value.get("vectors"), dict):
            return {"schema_version": "vector-cache-v1", "vectors": {}}
        return value

    def _read_state(self) -> dict[str, Any] | None:
        if not self.workspace.state_path.exists():
            return None
        try:
            envelope = read_json(self.workspace.state_path)
        except (OSError, ValueError):
            return None
        if not isinstance(envelope, dict) or not isinstance(envelope.get("state"), dict):
            return None
        state = envelope["state"]
        if envelope.get("state_sha256") != digest_obj(state):
            return None
        return state

    def _write_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.workspace.state_path, {"state": state, "state_sha256": digest_obj(state)})

    def _state_is_current(self, scan: RecordScan, layout: dict[str, Any], index: dict[str, Any] | None) -> bool:
        state = self._read_state()
        if state is None or index is None:
            return False
        cache = self._read_vector_cache()
        if state.get("vector_cache_fingerprint") != cache.get("vector_stage_fingerprint"):
            return False
        if state.get("last_batch_seq") != scan.latest_seq:
            return False
        if state.get("record_ledger_digest") != scan.ledger_digest:
            return False
        if state.get("config_version") != layout_version(layout):
            return False
        if state.get("index_digest") != digest_obj(index):
            return False
        if state.get("dataset_digest") != index.get("dataset_digest"):
            return False
        return True

    def _cached_vector(self, cached: Any, source_key: str, dimension: int) -> tuple[np.ndarray, str] | None:
        if not isinstance(cached, dict) or cached.get("source_key") != source_key:
            return None
        values = cached.get("vector")
        if not isinstance(values, list) or len(values) != dimension:
            return None
        try:
            vector = np.asarray(values, dtype=np.float64)
        except (TypeError, ValueError):
            return None
        if not bool(np.all(np.isfinite(vector))):
            return None
        digest = vector_digest(vector)
        if digest != cached.get("digest"):
            return None
        return vector, digest

    def _build_entries(
        self,
        layout: dict[str, Any],
        images: dict[str, dict[str, Any]],
        texts: dict[str, dict[str, Any]],
        *,
        cache: dict[str, Any],
        overrides: dict[str, Any] | None,
        batch_seq: int | None,
        batch_id: str | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
        overrides = dict(overrides or {})
        for text_id, row in texts.items():
            if "vector_override" in row and text_id not in overrides:
                overrides[text_id] = row["vector_override"]
        unknown = sorted(
            {
                text["image_id"]
                for text in texts.values()
                if text["image_id"] not in images
            }
        )
        if unknown:
            conflicts = [
                {"text_id": text_id, "image_id": texts[text_id]["image_id"]}
                for text_id in sorted(texts)
                if texts[text_id]["image_id"] in unknown
            ]
            raise _annotate(
                EngineError(
                    "E_UNKNOWN_REF",
                    "text entries reference image_ids that do not exist",
                    "vector_build",
                    entries=conflicts,
                    conflicts=conflicts,
                    details={"unknown_image_ids": unknown},
                ),
                batch_seq=batch_seq,
                batch_id=batch_id,
            )

        unknown_overrides = sorted(set(overrides).difference(texts))
        if unknown_overrides:
            raise _annotate(
                EngineError(
                    "E_UNKNOWN_REF",
                    "vector override references an unknown text_id",
                    "vector_build",
                    entries=[{"text_id": text_id} for text_id in unknown_overrides],
                    details={"unknown_text_ids": unknown_overrides},
                ),
                batch_seq=batch_seq,
                batch_id=batch_id,
            )

        dimension = int(layout["vector"]["dimension"])
        cache_vectors = cache.get("vectors", {})
        cache_out: dict[str, Any] = {}
        prepared: list[dict[str, Any]] = []
        reused_vectors = 0
        ordered_ids = sorted(texts)
        for ordinal, text_id in enumerate(ordered_ids):
            text_row = texts[text_id]
            image_id = text_row["image_id"]
            image_row = images[image_id]
            override = overrides.get(text_id)
            if override is not None:
                try:
                    validate_override(override, layout, text_id=text_id, image_id=image_id)
                except EngineError as exc:
                    raise _annotate(exc, batch_seq=batch_seq, batch_id=batch_id)
            image_bytes = self.workspace.image_bytes(image_row["file"])
            image_sha = hashlib.sha256(image_bytes).hexdigest() if image_bytes is not None else "MISSING"
            try:
                normalized = normalize_text(text_row["text"])
            except EngineError as exc:
                raise _annotate(exc, batch_seq=batch_seq, batch_id=batch_id)
            source_key = vector_source_key(
                layout=layout,
                text_id=text_id,
                image_id=image_id,
                raw_text=text_row["text"],
                normalized_text=normalized,
                image_sha256=image_sha,
                image_language=image_row.get("language", "und"),
                override=override,
            )
            cached = self._cached_vector(cache_vectors.get(text_id), source_key, dimension)
            if cached is None:
                try:
                    vector, digest, normalized, source_key = build_retrieval_vector(
                        text_id=text_id,
                        image_id=image_id,
                        image_bytes=image_bytes,
                        text=text_row["text"],
                        layout=layout,
                        image_language=image_row.get("language", "und"),
                        override=override,
                    )
                except EngineError as exc:
                    raise _annotate(exc, batch_seq=batch_seq, batch_id=batch_id)
            else:
                vector, digest = cached
                reused_vectors += 1

            prepared.append(
                {
                    "text_id": text_id,
                    "image_id": image_id,
                    "target_shard": text_row["target_shard"],
                    "ordinal": ordinal,
                    "vector": vector,
                    "vector_digest": digest,
                    "source_key": source_key,
                }
            )
            cache_out[text_id] = {
                "source_key": source_key,
                "dimension": dimension,
                "digest": digest,
                "vector": [float(value) for value in vector.tolist()],
            }

        vector_sources = [
            {
                "text_id": entry["text_id"],
                "image_id": entry["image_id"],
                "source_key": entry["source_key"],
            }
            for entry in prepared
        ]
        vector_fp = digest_obj(
            {
                "schema": "mmb-vector-stage-v1",
                "vector_config_fingerprint": vector_config_fingerprint(layout),
                "sources": vector_sources,
            }
        )
        planning_fp = digest_obj(
            {
                "schema": "mmb-planning-stage-v1",
                "planning_config_fingerprint": planning_config_fingerprint(layout),
                "vector_stage_fingerprint": vector_fp,
            }
        )
        ownership_fp = digest_obj(
            {
                "schema": "mmb-ownership-stage-v1",
                "planning_stage_fingerprint": planning_fp,
                "entries": [
                    {
                        "text_id": entry["text_id"],
                        "image_id": entry["image_id"],
                        "target_shard": entry["target_shard"],
                        "vector_digest": entry["vector_digest"],
                    }
                    for entry in prepared
                ],
            }
        )
        fingerprints = {
            "layout_config_version": layout_version(layout),
            "vector_build": vector_fp,
            "backfill_planning": planning_fp,
            "shard_ownership": ownership_fp,
            "vector_config_fingerprint": vector_config_fingerprint(layout),
            "planning_config_fingerprint": planning_config_fingerprint(layout),
        }
        cache_payload = {
            "schema_version": "vector-cache-v1",
            "vector_config_fingerprint": fingerprints["vector_config_fingerprint"],
            "vector_stage_fingerprint": vector_fp,
            "vectors": cache_out,
        }
        entry_sources = {entry["text_id"]: entry["source_key"] for entry in prepared}
        return prepared, fingerprints, {
            "cache": cache_payload,
            "entry_sources": entry_sources,
            "reused_vectors": reused_vectors,
        }

    def _write_success_state(
        self,
        *,
        scan: RecordScan,
        index: dict[str, Any],
        cache: dict[str, Any],
        entry_sources: dict[str, str],
    ) -> None:
        atomic_write_json(self.workspace.vectors_path, cache)
        atomic_write_json(self.workspace.index_path, index)
        state = {
            "schema_version": "active-state-v1",
            "last_batch_seq": scan.latest_seq,
            "record_ledger_digest": scan.ledger_digest,
            "record_digests": scan.record_digests,
            "batch_ids": [record["batch_id"] for record in scan.records],
            "config_version": index["config_version"],
            "version_fingerprints": index["version_fingerprints"],
            "index_digest": digest_obj(index),
            "vector_cache_fingerprint": cache["vector_stage_fingerprint"],
            "dataset_digest": index["dataset_digest"],
            "entry_count": index["entry_count"],
            "entry_sources": entry_sources,
        }
        self._write_state(state)

    def _read_ledger(self) -> dict[str, Any]:
        if not self.workspace.invalidations_path.exists():
            return {"schema_version": "invalidation-ledger-v1", "events": []}
        try:
            value = read_json(self.workspace.invalidations_path)
        except (OSError, ValueError):
            return {"schema_version": "invalidation-ledger-v1", "events": []}
        if not isinstance(value, dict) or not isinstance(value.get("events"), list):
            return {"schema_version": "invalidation-ledger-v1", "events": []}
        return value

    def _read_layout_events(self) -> list[dict[str, Any]]:
        if not self.workspace.layout_events_path.exists():
            return []
        try:
            value = read_json(self.workspace.layout_events_path)
        except (OSError, ValueError):
            return []
        if not isinstance(value, dict) or not isinstance(value.get("events"), list):
            return []
        return [item for item in value["events"] if isinstance(item, dict)]

    def _rebuild_invalidation_ledger(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        events: list[dict[str, Any]] = []
        for event in self._read_layout_events():
            if isinstance(event.get("event_id"), str):
                events.append(event)
        for record in records:
            event = record.get("invalidation")
            if isinstance(event, dict) and isinstance(event.get("event_id"), str):
                events.append(event)
        deduped: dict[str, dict[str, Any]] = {}
        for event in events:
            deduped[event["event_id"]] = event
        ordered = sorted(
            deduped.values(),
            key=lambda item: (
                0 if item.get("source") == "layout" else 1,
                int(item.get("batch_seq") or 0),
                item["event_id"],
            ),
        )
        payload = {"schema_version": "invalidation-ledger-v1", "events": ordered}
        atomic_write_json(self.workspace.invalidations_path, payload)
        return payload

    def _append_layout_event(self, event: dict[str, Any]) -> None:
        events = self._read_layout_events()
        if all(item.get("event_id") != event["event_id"] for item in events):
            events.append(event)
        atomic_write_json(self.workspace.layout_events_path, {"schema_version": "layout-events-v1", "events": events})

    def _invalidation_for_change(
        self,
        *,
        old_index: dict[str, Any] | None,
        old_sources: dict[str, str],
        new_index: dict[str, Any],
        new_sources: dict[str, str],
        old_layout: dict[str, Any] | None,
        new_layout: dict[str, Any],
        source: str,
        batch_seq: int | None = None,
        reason_override: str | None = None,
    ) -> dict[str, Any] | None:
        old_entries = _entry_index_map(old_index)
        new_entries = _entry_index_map(new_index)
        common_ids = sorted(set(old_entries).intersection(new_entries))
        vector_changed_ids = [text_id for text_id in common_ids if old_sources.get(text_id) != new_sources.get(text_id)]
        planning_changed_ids = [
            text_id
            for text_id in common_ids
            if old_entries[text_id].get("requested_shard") != new_entries[text_id].get("requested_shard")
        ]
        removed_ids = sorted(set(old_entries).difference(new_entries))
        old_fp = old_index.get("version_fingerprints", {}) if old_index else {}
        new_fp = new_index.get("version_fingerprints", {})
        vector_config_changed = bool(old_index) and old_fp.get("vector_config_fingerprint") != new_fp.get("vector_config_fingerprint")
        planning_config_changed = bool(old_index) and old_fp.get("planning_config_fingerprint") != new_fp.get("planning_config_fingerprint")

        earliest: str | None = None
        reason = reason_override
        if vector_config_changed:
            earliest = "vector_build"
            reason = reason or "vector_or_fusion_config_changed"
        elif vector_changed_ids:
            earliest = "vector_build"
            reason = reason or "sample_content_changed"
        elif planning_config_changed:
            earliest = "backfill_planning"
            reason = reason or "layout_capacity_or_topology_changed"
        elif planning_changed_ids:
            earliest = "backfill_planning"
            reason = reason or "sample_target_shard_changed"
        elif removed_ids:
            earliest = "shard_ownership"
            reason = reason or "sample_removed"
        if earliest is None:
            return None

        invalidated, reused = _diff_shards(old_index, new_index)
        if earliest == "backfill_planning" and old_layout is not None:
            old_layers = {item["layer"]: item for item in old_layout["layers"]}
            new_layers = {item["layer"]: item for item in new_layout["layers"]}
            changed_layers = {
                layer
                for layer in set(old_layers) | set(new_layers)
                if old_layers.get(layer) != new_layers.get(layer)
            }
            if changed_layers:
                layer_shards = {
                    shard["shard_id"]
                    for shard in new_index.get("shards", [])
                    if shard["layer"] in changed_layers
                }
                layer_shards.update(
                    shard["shard_id"]
                    for shard in (old_index or {}).get("shards", [])
                    if shard["layer"] in changed_layers
                )
                invalidated = sorted(set(invalidated) | layer_shards)
                reused = [shard_id for shard_id in reused if shard_id not in layer_shards]
        affected = sorted(set(vector_changed_ids) | set(planning_changed_ids) | set(removed_ids))
        core = {
            "source": source,
            "stage": earliest,
            "reason": reason,
            "old_layout_config_version": old_fp.get("layout_config_version"),
            "new_layout_config_version": new_fp.get("layout_config_version"),
            "old_fingerprints": {
                "vector_build": old_fp.get("vector_build"),
                "backfill_planning": old_fp.get("backfill_planning"),
                "shard_ownership": old_fp.get("shard_ownership"),
            },
            "new_fingerprints": {
                "vector_build": new_fp.get("vector_build"),
                "backfill_planning": new_fp.get("backfill_planning"),
                "shard_ownership": new_fp.get("shard_ownership"),
            },
            "invalidated_shards": invalidated,
            "reused_shards": reused,
            "affected_entries": affected,
            "removed_entries": removed_ids,
            "new_entries": sorted(set(new_entries).difference(old_entries)),
        }
        if source == "batch":
            core["batch_seq"] = batch_seq
        return _make_event(core)

    def _recover_locked(self, scan: RecordScan, *, clear_stop: bool = False) -> dict[str, Any]:
        if clear_stop:
            self.workspace.clear_stop()
        layout = normalize_layout(self.workspace.load_layout())
        if not scan.records:
            input_images, input_texts = {}, {}
        else:
            input_images, input_texts = merge_record_dataset(scan.records)
        cache = self._read_vector_cache()
        entries, fingerprints, build_state = self._build_entries(
            layout,
            input_images,
            input_texts,
            cache=cache,
            overrides=None,
            batch_seq=None,
            batch_id=None,
        )
        index = plan_index(entries, layout, fingerprints)
        self._write_success_state(
            scan=scan,
            index=index,
            cache=build_state["cache"],
            entry_sources=build_state["entry_sources"],
        )
        ledger = self._rebuild_invalidation_ledger(scan.records)
        return {
            "ok": True,
            "recovered": True,
            "reused_existing_index": False,
            "last_batch_seq": scan.latest_seq,
            "skipped_committed_batches": [record["batch_seq"] for record in scan.records],
            "ignored_records": scan.ignored,
            "index": index,
            "invalidations": ledger,
        }

    def _ensure_current_locked(self, scan: RecordScan) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, str]]:
        layout = normalize_layout(self.workspace.load_layout())
        index = self._read_index()
        if self._state_is_current(scan, layout, index):
            state = self._read_state()
            assert state is not None and index is not None
            return layout, index, self._read_vector_cache(), state.get("entry_sources", {})
        recovered = self._recover_locked(scan)
        return (
            layout,
            recovered["index"],
            self._read_vector_cache(),
            self._read_state().get("entry_sources", {}),
        )

    def install_layout(self, raw_layout: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_layout(raw_layout)
        with self.workspace.lock():
            scan = scan_records(self.workspace)
            had_layout = self.workspace.layout_path.exists()
            if had_layout:
                try:
                    old_layout = normalize_layout(read_json(self.workspace.layout_path))
                    old_index = self._read_index()
                    old_state = self._read_state()
                    input_images, input_texts = merge_record_dataset(scan.records) if scan.records else ({}, {})
                    if old_index is None and scan.records:
                        old_layout, old_index, _old_cache, old_sources = self._ensure_current_locked(scan)
                        old_state = self._read_state()
                    else:
                        old_sources = old_state.get("entry_sources", {}) if old_state else {}
                except EngineError:
                    old_layout, old_index, old_sources = None, None, {}
                    input_images, input_texts = merge_record_dataset(scan.records) if scan.records else ({}, {})
            else:
                old_layout = None
                old_index = None
                old_sources = {}
                input_images, input_texts = merge_record_dataset(scan.records) if scan.records else ({}, {})

            cache = self._read_vector_cache()
            try:
                entries, fingerprints, build_state = self._build_entries(
                    normalized,
                    input_images,
                    input_texts,
                    cache=cache,
                    overrides=None,
                    batch_seq=scan.latest_seq if scan.latest_seq else None,
                    batch_id=None,
                )
                new_index = plan_index(entries, normalized, fingerprints)
            except EngineError as exc:
                error = _annotate(exc, batch_seq=scan.latest_seq, batch_id=None)
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise

            event = None
            if old_layout is not None:
                event = self._invalidation_for_change(
                    old_index=old_index,
                    old_sources=old_sources,
                    new_index=new_index,
                    new_sources=build_state["entry_sources"],
                    old_layout=old_layout,
                    new_layout=normalized,
                    source="layout",
                    reason_override="layout_config_changed",
                )
            self.workspace.save_layout(normalized)
            if event is not None:
                self._append_layout_event(event)
            self._write_success_state(
                scan=scan,
                index=new_index,
                cache=build_state["cache"],
                entry_sources=build_state["entry_sources"],
            )
            ledger = self._rebuild_invalidation_ledger(scan.records)
            return {
                "ok": True,
                "layout": normalized,
                "layout_config_version": fingerprints["layout_config_version"],
                "invalidated": event,
                "index": new_index,
                "invalidations": ledger,
            }

    def import_images(self, rows: list[dict[str, Any]], source_dir: Path | None = None) -> dict[str, Any]:
        with self.workspace.lock():
            normalized = self.workspace.normalize_image_rows(rows)
            copied = self.workspace.stage_image_files(normalized, source_dir)
            current_texts = self.workspace.load_texts()
            self.workspace.save_catalog(normalized, current_texts)
            return {"ok": True, "image_count": len(normalized), "copied_files": copied, "images": normalized}

    def import_texts(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        with self.workspace.lock():
            normalized = self.workspace.normalize_text_rows(rows)
            current_images = self.workspace.load_images()
            self.workspace.save_catalog(current_images, normalized)
            return {"ok": True, "text_count": len(normalized), "texts": normalized}

    def import_inputs(
        self,
        image_rows: list[dict[str, Any]],
        text_rows: list[dict[str, Any]],
        image_source_dir: Path | None = None,
    ) -> dict[str, Any]:
        with self.workspace.lock():
            images = self.workspace.normalize_image_rows(image_rows)
            texts = self.workspace.normalize_text_rows(text_rows)
            copied = self.workspace.stage_image_files(images, image_source_dir)
            self.workspace.save_catalog(images, texts)
            return {"ok": True, "image_count": len(images), "text_count": len(texts), "copied_files": copied}

    def _select_batch_texts(
        self,
        *,
        text_ids: list[str],
        order: str,
        catalog_texts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not isinstance(text_ids, list) or not text_ids or any(not isinstance(item, str) or not item for item in text_ids):
            raise EngineError("E_INVALID_INPUT", "text_ids must be a non-empty array of ids", "request_validation")
        if len(set(text_ids)) != len(text_ids):
            raise EngineError("E_INVALID_INPUT", "batch contains duplicate text_ids", "request_validation")
        if order not in ("arrival", "id"):
            raise EngineError("E_INVALID_INPUT", "order must be 'arrival' or 'id'", "request_validation")
        by_id = {row["text_id"]: row for row in catalog_texts}
        missing = sorted(set(text_ids).difference(by_id))
        if missing:
            raise EngineError(
                "E_UNKNOWN_REF",
                "requested text_ids do not exist in the imported catalog",
                "request_validation",
                entries=[{"text_id": text_id} for text_id in missing],
                details={"unknown_text_ids": missing},
            )
        selected = [dict(by_id[text_id]) for text_id in text_ids]
        if order == "arrival":
            selected.sort(key=lambda row: (int(row.get("catalog_ordinal", 0)), row["text_id"]))
        else:
            selected.sort(key=lambda row: row["text_id"])
        return selected

    def submit_batch(
        self,
        *,
        batch_id: str,
        text_ids: list[str],
        expected_sequence: int,
        order: str = "id",
        vector_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(batch_id, str) or not batch_id:
            raise EngineError("E_INVALID_INPUT", "batch_id must be a non-empty string", "request_validation")
        expected = _attempt(expected_sequence, "expected_sequence")
        with self.workspace.lock():
            if self.workspace.stop_requested():
                error = EngineError(
                    "E_STOPPED",
                    "a graceful stop has been requested; no new batch was committed",
                    "batch_gate",
                    batch_seq=expected + 1,
                    batch_id=batch_id,
                )
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise error
            scan = scan_records(self.workspace)
            self._ensure_current_locked(scan)
            if expected != scan.latest_seq:
                error = EngineError(
                    "E_STALE_CHECKPOINT",
                    "expected_sequence does not match the committed ledger head",
                    "batch_gate",
                    batch_seq=expected + 1,
                    batch_id=batch_id,
                    details={"expected_sequence": expected, "current_sequence": scan.latest_seq},
                )
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise error
            if any(record["batch_id"] == batch_id for record in scan.records):
                error = EngineError(
                    "E_STALE_CHECKPOINT",
                    "batch_id has already been committed",
                    "batch_gate",
                    batch_seq=expected + 1,
                    batch_id=batch_id,
                    details={"current_sequence": scan.latest_seq, "duplicate_batch_id": batch_id},
                )
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise error

            layout, old_index, cache, old_sources = self._ensure_current_locked(scan)
            catalog_texts = self.workspace.load_texts()
            try:
                selected = self._select_batch_texts(text_ids=text_ids, order=order, catalog_texts=catalog_texts)
            except EngineError as exc:
                raise _annotate(exc, batch_seq=scan.latest_seq + 1, batch_id=batch_id)

            overrides = dict(vector_overrides or {})
            foreign_overrides = sorted(set(overrides).difference(row["text_id"] for row in selected))
            if foreign_overrides:
                raise _annotate(
                    EngineError(
                        "E_INVALID_INPUT",
                        "vector overrides may only target text_ids in the submitted batch",
                        "vector_build",
                        entries=[{"text_id": text_id} for text_id in foreign_overrides],
                        details={"unknown_text_ids": foreign_overrides},
                    ),
                    batch_seq=scan.latest_seq + 1,
                    batch_id=batch_id,
                )
            for row in selected:
                if row["text_id"] in overrides:
                    row["vector_override"] = overrides[row["text_id"]]

            committed_images, committed_texts = merge_record_dataset(scan.records) if scan.records else ({}, {})
            catalog_images = self.workspace.load_images()
            proposed_images = dict(committed_images)
            for row in catalog_images:
                proposed_images[row["image_id"]] = dict(row)
            proposed_texts = dict(committed_texts)
            for row in selected:
                proposed_texts[row["text_id"]] = dict(row)

            try:
                entries, fingerprints, build_state = self._build_entries(
                    layout,
                    proposed_images,
                    proposed_texts,
                    cache=cache,
                    overrides=overrides,
                    batch_seq=scan.latest_seq + 1,
                    batch_id=batch_id,
                )
                new_index = plan_index(entries, layout, fingerprints)
            except EngineError as exc:
                error = _annotate(exc, batch_seq=scan.latest_seq + 1, batch_id=batch_id)
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id, "committed_sequence": scan.latest_seq}
                raise

            event = self._invalidation_for_change(
                old_index=old_index,
                old_sources=old_sources,
                new_index=new_index,
                new_sources=build_state["entry_sources"],
                old_layout=layout,
                new_layout=layout,
                source="batch",
                batch_seq=scan.latest_seq + 1,
            )
            referenced_images = {row["image_id"]: proposed_images[row["image_id"]] for row in selected}
            batch_images = [referenced_images[image_id] for image_id in sorted(referenced_images)]
            batch_texts = [dict(row) for row in selected]
            input_content_fingerprint = digest_obj(
                {"schema": "mmb-batch-input-v1", "images": batch_images, "texts": batch_texts}
            )
            assignment_summary = {
                shard_id: {
                    "entry_count": value["entry_count"],
                    "entry_ids": value["entry_ids"],
                    "membership_digest": value["membership_digest"],
                }
                for shard_id, value in new_index["ownership_summary"].items()
            }
            prev_digest = scan.records[-1]["_record_digest"] if scan.records else None
            payload = {
                "record_version": 1,
                "batch_seq": scan.latest_seq + 1,
                "batch_id": batch_id,
                "order": order,
                "submitted_text_ids": [row["text_id"] for row in batch_texts],
                "images": batch_images,
                "texts": batch_texts,
                "input_content_fingerprint": input_content_fingerprint,
                "dataset_content_fingerprint": fingerprints["shard_ownership"],
                "layout_config_version": fingerprints["layout_config_version"],
                "version_fingerprints": fingerprints,
                "ownership_fingerprint": fingerprints["shard_ownership"],
                "dataset_digest": new_index["dataset_digest"],
                "entry_count": new_index["entry_count"],
                "assignment_summary": assignment_summary,
                "invalidation": event,
                "prev_record_digest": prev_digest,
            }
            record_sha = write_batch_record(self.workspace, payload)
            new_scan = scan_records(self.workspace)
            if new_scan.latest_seq != payload["batch_seq"] or new_scan.records[-1]["_record_digest"] != record_sha:
                raise EngineError(
                    "E_STALE_CHECKPOINT",
                    "the record ledger could not observe the newly committed record",
                    "batch_commit",
                    batch_seq=payload["batch_seq"],
                    batch_id=batch_id,
                )
            self._write_success_state(
                scan=new_scan,
                index=new_index,
                cache=build_state["cache"],
                entry_sources=build_state["entry_sources"],
            )
            ledger = self._rebuild_invalidation_ledger(new_scan.records)
            return {
                "ok": True,
                "batch_seq": payload["batch_seq"],
                "batch_id": batch_id,
                "order": order,
                "record_digest": record_sha,
                "input_content_fingerprint": input_content_fingerprint,
                "layout_config_version": fingerprints["layout_config_version"],
                "version_fingerprints": fingerprints,
                "assignment_summary": assignment_summary,
                "invalidated": event,
                "stages": {
                    "vector_build": {
                        "entry_count": len(entries),
                        "vector_cache_fingerprint": fingerprints["vector_build"],
                        "reused_vectors": build_state.get("reused_vectors", 0),
                    },
                    "backfill_planning": {
                        "entry_count": len(entries),
                        "planning_fingerprint": fingerprints["backfill_planning"],
                        "ownership_fingerprint": fingerprints["shard_ownership"],
                        "shard_count": len(new_index["shards"]),
                        "split_count": len(new_index["split_history"]),
                        "route_count": len(new_index["routes"]),
                    },
                },
                "invalidations": ledger,
                "index": new_index,
            }

    def backfill_all(
        self,
        *,
        text_ids: list[str],
        expected_sequence: int,
        batch_size: int,
        batch_prefix: str = "batch",
        order: str = "id",
    ) -> dict[str, Any]:
        expected = _attempt(expected_sequence, "expected_sequence")
        batch_size = _attempt(batch_size, "batch_size")
        if batch_size <= 0:
            raise EngineError("E_INVALID_INPUT", "batch_size must be positive", "request_validation")
        if not isinstance(text_ids, list) or not text_ids or any(not isinstance(item, str) or not item for item in text_ids):
            raise EngineError("E_INVALID_INPUT", "text_ids must be a non-empty array of ids", "request_validation")
        if len(set(text_ids)) != len(text_ids):
            raise EngineError("E_INVALID_INPUT", "batch contains duplicate text_ids", "request_validation")
        if order not in ("arrival", "id"):
            raise EngineError("E_INVALID_INPUT", "order must be 'arrival' or 'id'", "request_validation")
        catalog = self.workspace.load_texts()
        if order == "arrival":
            ordered = [row["text_id"] for row in sorted(catalog, key=lambda row: (row.get("catalog_ordinal", 0), row["text_id"]))]
        else:
            ordered = sorted(row["text_id"] for row in catalog)
        selected = [text_id for text_id in ordered if text_id in set(text_ids)]
        missing = sorted(set(text_ids).difference(ordered))
        if missing:
            raise EngineError(
                "E_UNKNOWN_REF",
                "requested text_ids do not exist in catalog",
                "request_validation",
                entries=[{"text_id": text_id} for text_id in missing],
                details={"unknown_text_ids": missing},
            )
        attempts: list[dict[str, Any]] = []
        current = expected
        for index, start in enumerate(range(0, len(selected), batch_size), start=1):
            if self.workspace.stop_requested():
                attempts.append(
                    {
                        "ok": False,
                        "stopped": True,
                        "next_sequence": current,
                        "reason": "stop marker observed between atomic batches",
                    }
                )
                break
            chunk = selected[start : start + batch_size]
            result = self.submit_batch(
                batch_id=f"{batch_prefix}-{index:04d}",
                text_ids=chunk,
                expected_sequence=current,
                order=order,
            )
            attempts.append(
                {
                    "ok": True,
                    "batch_seq": result["batch_seq"],
                    "batch_id": result["batch_id"],
                    "text_ids": chunk,
                }
            )
            current = result["batch_seq"]
        return {
            "ok": all(item.get("ok") for item in attempts) if attempts else True,
            "order": order,
            "committed_batch_count": sum(1 for item in attempts if item.get("ok")),
            "last_sequence": current,
            "attempts": attempts,
            "stopped": self.workspace.stop_requested(),
        }

    def recover(self, *, clear_stop: bool = True) -> dict[str, Any]:
        with self.workspace.lock():
            if clear_stop:
                self.workspace.clear_stop()
            scan = scan_records(self.workspace)
            try:
                layout = normalize_layout(self.workspace.load_layout())
                index = self._read_index()
                if self._state_is_current(scan, layout, index):
                    ledger = self._rebuild_invalidation_ledger(scan.records)
                    return {
                        "ok": True,
                        "recovered": False,
                        "reused_existing_index": True,
                        "last_batch_seq": scan.latest_seq,
                        "skipped_committed_batches": [record["batch_seq"] for record in scan.records],
                        "ignored_records": scan.ignored,
                        "index": index,
                        "invalidations": ledger,
                    }
            except EngineError as exc:
                error = _annotate(exc, batch_seq=scan.latest_seq, batch_id=None)
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise
            try:
                return self._recover_locked(scan)
            except EngineError as exc:
                error = _annotate(exc, batch_seq=scan.latest_seq, batch_id=None)
                diagnostic_id = self.workspace.record_diagnostic(error)
                error.details = {**error.details, "diagnostic_id": diagnostic_id}
                raise

    def status(self) -> dict[str, Any]:
        scan = scan_records(self.workspace)
        index = self._read_index()
        state = self._read_state()
        ledger = self._read_ledger()
        state_consistent = False
        if self.workspace.layout_path.exists() and index is not None:
            try:
                layout = normalize_layout(self.workspace.load_layout())
                state_consistent = self._state_is_current(scan, layout, index)
            except EngineError:
                state_consistent = False
        return {
            "ok": True,
            "workspace": str(self.workspace.root),
            "initialized": self.workspace.layout_path.exists(),
            "last_batch_seq": scan.latest_seq,
            "batch_count": len(scan.records),
            "state_consistent": state_consistent,
            "stopped": self.workspace.stop_requested(),
            "stop_request": self.workspace.read_stop(),
            "ignored_record_count": len(scan.ignored),
            "config_version": layout_version(normalize_layout(self.workspace.load_layout())) if self.workspace.layout_path.exists() else None,
            "state": state,
            "index_summary": {
                "entry_count": index.get("entry_count", 0) if index else 0,
                "shard_count": len(index.get("shards", [])) if index else 0,
                "split_count": len(index.get("split_history", [])) if index else 0,
                "dataset_digest": index.get("dataset_digest") if index else None,
            },
            "invalidation_count": len(ledger.get("events", [])),
        }

    def index(self) -> dict[str, Any]:
        index = self._read_index()
        if index is None:
            raise EngineError("E_INVALID_LAYOUT", "no materialized index exists", "index_query")
        return index

    def batches(self) -> dict[str, Any]:
        scan = scan_records(self.workspace)
        return {
            "ok": True,
            "latest_sequence": scan.latest_seq,
            "batches": record_summaries(scan.records),
            "ignored_records": scan.ignored,
        }

    def invalidations(self) -> dict[str, Any]:
        return {"ok": True, **self._read_ledger()}

    def vectors(self) -> dict[str, Any]:
        cache = self._read_vector_cache()
        return {"ok": True, **cache}

    def diagnostics(self) -> dict[str, Any]:
        return {"ok": True, "diagnostics": self.workspace.diagnostics()}
