from __future__ import annotations

import json
import os
import time
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import numpy as np

from .errors import (
    BusinessError,
    E_BAD_REQUEST,
    E_INTERNAL,
    E_MODEL_NOT_FOUND,
    E_REQUEST_BINDING_MISMATCH,
    E_REQUEST_INTERRUPTED,
    E_SNAPSHOT_INVALID,
    E_SNAPSHOT_STALE,
    E_STALE_BASE,
    E_STATE_EMPTY,
    E_VERSION_CONFLICT,
    E_VERSION_NOT_FOUND,
    E_VOCAB_MISSING,
)
from .execution import execute_graph
from .graph import (
    graph_signature,
    graph_vocab_refs,
    preprocess_steps,
    validate_graph,
)
from .signature import DIFF_DTYPE, DIFF_PREPOST, DIFF_QUANT, DIFF_SHAPE, DIFF_VOCAB, diff_graphs
from .storage import connect_database, immediate_transaction, initialize_database, read_transaction
from .utils import (
    RATIO_SCALE,
    canonical_json,
    decimal_text,
    normalize_ratio,
    parse_generated_at,
    require_positive_int,
    sha256_json,
    utc_now,
)


ARTIFACT_KINDS = ("route", "preprocess_cache", "inflight_index", "gray_plan")
INTENT_SNAPSHOT = "snapshot_replace"
INTENT_INCREMENT = "increment_apply"
INTENT_GRAY = "gray_decision"
INTENT_BINDING = "request_binding"
INTENT_INFERENCE = "inference"


def _require_string(value: Any, field: str, code: str = E_BAD_REQUEST) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BusinessError(code, f"{field} must be a non-empty string", {"field": field})
    return value.strip()


def _json_loads(value: str) -> Any:
    return json.loads(value)


class Engine:
    """Hot-switch engine backed by one SQLite database and deterministic NumPy graphs."""

    def __init__(self, db_path: str, recover: bool = True) -> None:
        self.db_path = str(db_path)
        initialize_database(self.db_path)
        self.last_recovery_report: dict[str, Any] | None = None
        if recover:
            self.last_recovery_report = self.recover()

    # ------------------------------------------------------------------ setup
    def _connect(self):
        return connect_database(self.db_path)

    @staticmethod
    def _read_state(connection) -> dict[str, Any] | None:
        row = connection.execute("SELECT * FROM state WHERE id = 1").fetchone()
        if row is None:
            return None
        return {
            "snapshot_seq": int(row["snapshot_seq"]),
            "state_token": row["state_token"],
            "generated_at": row["generated_at"],
            "active_models": _json_loads(row["active_models_json"]),
            "known_vocabs": _json_loads(row["known_vocabs_json"]),
            "revision": int(row["revision"]),
            "updated_at": row["updated_at"],
        }

    def _current_state(self) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            return self._read_state(connection)
        finally:
            connection.close()

    def _require_current_state(self) -> dict[str, Any]:
        state = self._current_state()
        if state is None:
            raise BusinessError(
                E_STATE_EMPTY,
                "no snapshot has been committed yet",
                {"operation": "read_state"},
            )
        return state

    @staticmethod
    def _active_signature_map(
        active_models: dict[str, Any],
        model_records: dict[tuple[str, str], dict[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for model_id, config in sorted(active_models.items()):
            version = config["version"]
            record = model_records.get((model_id, version))
            result[model_id] = {
                "version": version,
                "signature": record["signature"] if record else None,
            }
        return result

    @staticmethod
    def _make_state_token(
        kind: str,
        snapshot_seq: int,
        active_models: dict[str, Any],
        signature_map: dict[str, Any],
        known_vocabs: list[str],
    ) -> str:
        payload = {
            "kind": kind,
            "snapshot_seq": snapshot_seq,
            "active_models": active_models,
            "active_signatures": signature_map,
            "known_vocabs": sorted(known_vocabs),
        }
        prefix = "snapshot" if kind == INTENT_SNAPSHOT else "increment"
        return f"{prefix}:{snapshot_seq}:{sha256_json(payload)[:20]}"

    @staticmethod
    def _write_state(connection, state: dict[str, Any]) -> None:
        connection.execute(
            """
            INSERT INTO state(
                id, snapshot_seq, state_token, generated_at, active_models_json,
                known_vocabs_json, revision, updated_at
            ) VALUES(1, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                snapshot_seq=excluded.snapshot_seq,
                state_token=excluded.state_token,
                generated_at=excluded.generated_at,
                active_models_json=excluded.active_models_json,
                known_vocabs_json=excluded.known_vocabs_json,
                revision=excluded.revision,
                updated_at=excluded.updated_at
            """,
            (
                state["snapshot_seq"],
                state["state_token"],
                state["generated_at"],
                canonical_json(state["active_models"]),
                canonical_json(state["known_vocabs"]),
                state["revision"],
                state["updated_at"],
            ),
        )

    # -------------------------------------------------------------- envelopes
    @staticmethod
    def _normalize_model_record(raw: Any, index: int) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise BusinessError(
                E_BAD_REQUEST,
                "model record must be an object",
                {"field": f"models[{index}]"},
            )
        model_id = _require_string(raw.get("model_id"), f"models[{index}].model_id")
        version = _require_string(raw.get("version"), f"models[{index}].version")
        graph = validate_graph(raw.get("graph"))
        base_version = raw.get("base_version")
        if base_version is not None:
            base_version = _require_string(
                base_version, f"models[{index}].base_version", E_STALE_BASE
            )
        return {
            "model_id": model_id,
            "version": version,
            "graph": graph,
            "signature": graph_signature(graph),
            "base_version": base_version,
        }

    @staticmethod
    def _normalize_active_models(raw: Any) -> dict[str, dict[str, Any]]:
        if not isinstance(raw, dict) or not raw:
            raise BusinessError(
                E_BAD_REQUEST,
                "active_models must be a non-empty object",
                {"field": "active_models"},
            )
        result: dict[str, dict[str, Any]] = {}
        for model_id, raw_config in raw.items():
            if not isinstance(model_id, str) or not model_id.strip():
                raise BusinessError(
                    E_BAD_REQUEST,
                    "active_models keys must be non-empty model IDs",
                    {"field": "active_models"},
                )
            if isinstance(raw_config, str):
                raw_config = {"version": raw_config}
            if not isinstance(raw_config, dict):
                raise BusinessError(
                    E_BAD_REQUEST,
                    "active model configuration must be an object or version string",
                    {"field": f"active_models.{model_id}"},
                )
            version = _require_string(
                raw_config.get("version"),
                f"active_models.{model_id}.version",
            )
            previous_version = raw_config.get("previous_version")
            if previous_version is not None:
                previous_version = _require_string(
                    previous_version,
                    f"active_models.{model_id}.previous_version",
                )
            try:
                ratio = normalize_ratio(raw_config.get("gray_ratio", 0))
            except BusinessError as exc:
                exc.context.setdefault("model_id", model_id)
                exc.context.setdefault("current_ratio", raw_config.get("gray_ratio", 0))
                raise
            result[model_id] = {
                "version": version,
                "previous_version": previous_version,
                "gray_ratio": ratio["normalized"],
                "gray_ratio_ppm": ratio["units"],
                "gray_ratio_rounding_error": ratio["rounding_error"],
            }
        return result

    def _normalize_envelope(self, payload: Any, kind: str) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise BusinessError(E_BAD_REQUEST, "request body must be a JSON object", {"field": "body"})
        try:
            snapshot_seq = require_positive_int(payload.get("snapshot_seq"), "snapshot_seq")
        except BusinessError as exc:
            raise BusinessError(
                E_SNAPSHOT_INVALID,
                "snapshot_seq must be a positive integer",
                {"field": "snapshot_seq", "value": payload.get("snapshot_seq")},
            ) from exc
        generated_at = parse_generated_at(payload.get("generated_at"))
        raw_models = payload.get("models")
        if not isinstance(raw_models, list) or not raw_models:
            raise BusinessError(
                E_BAD_REQUEST,
                "models must be a non-empty list",
                {"field": "models"},
            )
        records = [
            self._normalize_model_record(raw_record, index)
            for index, raw_record in enumerate(raw_models)
        ]
        record_map: dict[tuple[str, str], dict[str, Any]] = {}
        for record in records:
            key = (record["model_id"], record["version"])
            if key in record_map:
                raise BusinessError(
                    E_BAD_REQUEST,
                    "duplicate model_id/version in one submission",
                    {"model_id": record["model_id"], "version": record["version"]},
                )
            record_map[key] = record

        known_raw = payload.get("known_vocabs", [])
        if not isinstance(known_raw, list):
            raise BusinessError(
                E_BAD_REQUEST,
                "known_vocabs must be a list",
                {"field": "known_vocabs"},
            )
        known_vocabs: list[str] = []
        for index, vocab_id in enumerate(known_raw):
            vocab_id = _require_string(vocab_id, f"known_vocabs[{index}]")
            if vocab_id not in known_vocabs:
                known_vocabs.append(vocab_id)

        active_models = self._normalize_active_models(payload.get("active_models"))
        base_version = payload.get("base_version")
        if base_version is not None:
            base_version = _require_string(base_version, "base_version")
        return {
            "kind": kind,
            "snapshot_seq": snapshot_seq,
            "generated_at": generated_at,
            "records": records,
            "record_map": record_map,
            "known_vocabs": known_vocabs,
            "active_models": active_models,
            "base_version": base_version,
        }

    # ----------------------------------------------------------------- intents
    def _start_intent(
        self,
        kind: str,
        snapshot_seq: int | None,
        base_token: str | None,
        details: dict[str, Any],
    ) -> str:
        intent_id = f"intent-{uuid.uuid4().hex}"
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                connection.execute(
                    """
                    INSERT INTO intents(
                        intent_id, kind, status, snapshot_seq, base_token, details_json,
                        created_at, completed_at, recovery_note
                    ) VALUES(?, ?, 'started', ?, ?, ?, ?, NULL, NULL)
                    """,
                    (
                        intent_id,
                        kind,
                        snapshot_seq,
                        base_token,
                        canonical_json(details),
                        utc_now(),
                    ),
                )
        finally:
            connection.close()
        self._maybe_pause_after_intent(intent_id, kind)
        return intent_id

    @staticmethod
    def _finish_intent_connection(connection, intent_id: str, status: str) -> None:
        connection.execute(
            "UPDATE intents SET status = ?, completed_at = ? WHERE intent_id = ?",
            (status, utc_now(), intent_id),
        )

    def _finish_intent(self, intent_id: str, status: str) -> None:
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                self._finish_intent_connection(connection, intent_id, status)
        finally:
            connection.close()

    @staticmethod
    def _maybe_pause_after_intent(intent_id: str, kind: str) -> None:
        target_kind = os.environ.get("HOTROUTE_PAUSE_AFTER_INTENT_KIND")
        if not target_kind or target_kind != kind:
            return
        marker = os.environ.get("HOTROUTE_INTENT_MARKER")
        if marker:
            marker_path = Path(marker)
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.write_text(
                json.dumps({"intent_id": intent_id, "kind": kind, "at": utc_now()}),
                encoding="utf-8",
            )
        pause_seconds = float(os.environ.get("HOTROUTE_PAUSE_SECONDS", "30"))
        time.sleep(max(0.1, pause_seconds))

    # ---------------------------------------------------------- snapshot apply
    def submit_snapshot(self, payload: Any) -> dict[str, Any]:
        envelope = self._normalize_envelope(payload, INTENT_SNAPSHOT)
        current_state = self._current_state()
        base_token = current_state["state_token"] if current_state else None
        intent_id = self._start_intent(
            INTENT_SNAPSHOT,
            envelope["snapshot_seq"],
            base_token,
            {
                "snapshot_seq": envelope["snapshot_seq"],
                "models": sorted(
                    f"{record['model_id']}@{record['version']}" for record in envelope["records"]
                ),
            },
        )
        try:
            result = self._apply_snapshot_transaction(envelope, intent_id)
        except Exception:
            self._finish_intent(intent_id, "aborted")
            raise
        return result

    def _apply_snapshot_transaction(
        self,
        envelope: dict[str, Any],
        intent_id: str,
    ) -> dict[str, Any]:
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                state = self._read_state(connection)
                if state is not None and envelope["snapshot_seq"] <= state["snapshot_seq"]:
                    raise BusinessError(
                        E_SNAPSHOT_STALE,
                        "snapshot sequence must increase monotonically",
                        {
                            "requested_snapshot_seq": envelope["snapshot_seq"],
                            "current_snapshot_seq": state["snapshot_seq"],
                        },
                    )
                record_map = envelope["record_map"]
                for model_id, config in envelope["active_models"].items():
                    if (model_id, config["version"]) not in record_map:
                        raise BusinessError(
                            E_VERSION_NOT_FOUND,
                            "a full snapshot must carry the graph for every active version",
                            {
                                "model_id": model_id,
                                "version": config["version"],
                                "field": "active_models",
                            },
                        )

                old_active = state["active_models"] if state else {}
                old_graphs: dict[tuple[str, str], dict[str, Any]] = {}
                for model_id, config in old_active.items():
                    row = connection.execute(
                        "SELECT graph_json FROM model_versions WHERE model_id = ? AND version = ?",
                        (model_id, config["version"]),
                    ).fetchone()
                    if row is not None:
                        old_graphs[(model_id, config["version"])] = _json_loads(row["graph_json"])

                for model_id, config in envelope["active_models"].items():
                    previous = config.get("previous_version")
                    if previous is None and model_id in old_active:
                        previous = old_active[model_id]["version"]
                    if previous is None:
                        previous = config["version"]
                    config["previous_version"] = previous
                    if config["gray_ratio_ppm"] > 0 and previous != config["version"]:
                        if (model_id, previous) not in record_map:
                            raise BusinessError(
                                E_VERSION_NOT_FOUND,
                                "a gray snapshot must include its previous version graph",
                                {
                                    "model_id": model_id,
                                    "version": previous,
                                    "field": "active_models.previous_version",
                                },
                            )

                known_vocabs = sorted(envelope["known_vocabs"])
                signature_map = {
                    model_id: {
                        "version": config["version"],
                        "signature": record_map[(model_id, config["version"])]["signature"],
                    }
                    for model_id, config in sorted(envelope["active_models"].items())
                }
                state_token = self._make_state_token(
                    INTENT_SNAPSHOT,
                    envelope["snapshot_seq"],
                    envelope["active_models"],
                    signature_map,
                    known_vocabs,
                )

                connection.execute("DELETE FROM model_versions")
                for record in envelope["records"]:
                    connection.execute(
                        """
                        INSERT INTO model_versions(
                            model_id, version, graph_json, graph_signature,
                            source_snapshot_seq, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record["model_id"],
                            record["version"],
                            canonical_json(record["graph"]),
                            record["signature"],
                            envelope["snapshot_seq"],
                            utc_now(),
                        ),
                    )
                connection.execute("DELETE FROM known_vocabs")
                for vocab_id in known_vocabs:
                    connection.execute(
                        "INSERT INTO known_vocabs(vocab_id, snapshot_seq) VALUES(?, ?)",
                        (vocab_id, envelope["snapshot_seq"]),
                    )
                removed_models = sorted(set(old_active) - set(envelope["active_models"]))
                for removed_model in removed_models:
                    connection.execute(
                        "DELETE FROM route_entries WHERE model_id = ?", (removed_model,)
                    )
                    connection.execute(
                        "DELETE FROM derived_artifacts WHERE model_id = ?", (removed_model,)
                    )
                    connection.execute(
                        "DELETE FROM gray_plans WHERE model_id = ?", (removed_model,)
                    )

                manifests: list[dict[str, Any]] = []
                for model_id, config in sorted(envelope["active_models"].items()):
                    old_config = old_active.get(model_id)
                    old_graph = None
                    if old_config:
                        old_graph = old_graphs.get((model_id, old_config["version"]))
                    new_graph = record_map[(model_id, config["version"])]["graph"]
                    manifest = self._apply_switch(
                        connection=connection,
                        model_id=model_id,
                        old_config=old_config,
                        old_graph=old_graph,
                        new_config=config,
                        new_graph=new_graph,
                        snapshot_seq=envelope["snapshot_seq"],
                        state_token=state_token,
                    )
                    manifests.append(manifest)

                new_state = {
                    "snapshot_seq": envelope["snapshot_seq"],
                    "state_token": state_token,
                    "generated_at": envelope["generated_at"],
                    "active_models": envelope["active_models"],
                    "known_vocabs": known_vocabs,
                    "revision": (state["revision"] + 1) if state else 1,
                    "updated_at": utc_now(),
                }
                self._write_state(connection, new_state)
                self._finish_intent_connection(connection, intent_id, "committed")
                response = {
                    "snapshot_seq": envelope["snapshot_seq"],
                    "state_version": state_token,
                    "base_version": state["state_token"] if state else None,
                    "generated_at": envelope["generated_at"],
                    "active_models": envelope["active_models"],
                    "known_vocabs": known_vocabs,
                    "migrations": manifests,
                    "removed_models": removed_models,
                    "atomic_commit": True,
                }
        finally:
            connection.close()
        return response

    # --------------------------------------------------------- increment apply
    def submit_increment(self, payload: Any) -> dict[str, Any]:
        envelope = self._normalize_envelope(payload, INTENT_INCREMENT)
        current_state = self._current_state()
        base_token = envelope["base_version"]
        if base_token is None and current_state and len(envelope["active_models"]) == 1:
            model_id = next(iter(envelope["active_models"]))
            active_version = envelope["active_models"][model_id]["version"]
            for record in envelope["records"]:
                if record["model_id"] == model_id and record["version"] == active_version:
                    base_token = record.get("base_version")
                    break
        intent_id = self._start_intent(
            INTENT_INCREMENT,
            envelope["snapshot_seq"],
            base_token,
            {
                "snapshot_seq": envelope["snapshot_seq"],
                "active_models": {
                    model_id: config["version"]
                    for model_id, config in envelope["active_models"].items()
                },
                "base_version": envelope["base_version"],
            },
        )
        try:
            result = self._apply_increment_transaction(envelope, intent_id)
        except Exception:
            self._finish_intent(intent_id, "aborted")
            raise
        return result

    @staticmethod
    def _assert_increment_base(
        envelope: dict[str, Any],
        state: dict[str, Any],
    ) -> None:
        active_models = state["active_models"]
        affected = sorted(envelope["active_models"])
        top_level_base = envelope["base_version"]
        accepted_global = {
            state["state_token"],
            f"snapshot:{state['snapshot_seq']}",
            f"state:{state['snapshot_seq']}",
        }

        if top_level_base is not None:
            if len(affected) > 1:
                if top_level_base not in accepted_global:
                    raise BusinessError(
                        E_STALE_BASE,
                        "increment base version is stale",
                        {
                            "base_version": top_level_base,
                            "current_state_version": state["state_token"],
                            "current_snapshot_seq": state["snapshot_seq"],
                            "affected_models": affected,
                        },
                    )
            else:
                model_id = affected[0]
                current_version = active_models[model_id]["version"]
                accepted = set(accepted_global)
                accepted.add(current_version)
                accepted.add(f"{model_id}@{current_version}")
                if top_level_base not in accepted:
                    raise BusinessError(
                        E_STALE_BASE,
                        "increment base version is stale",
                        {
                            "base_version": top_level_base,
                            "current_state_version": state["state_token"],
                            "current_model_version": current_version,
                            "model_id": model_id,
                            "affected_models": affected,
                        },
                    )
            return

        if len(affected) != 1:
            raise BusinessError(
                E_STALE_BASE,
                "multi-model increments must carry a global base_version",
                {
                    "current_state_version": state["state_token"],
                    "affected_models": affected,
                },
            )
        model_id = affected[0]
        record = envelope["record_map"].get((model_id, envelope["active_models"][model_id]["version"]))
        per_model_base = record.get("base_version") if record else None
        current_version = active_models[model_id]["version"]
        if per_model_base != current_version:
            raise BusinessError(
                E_STALE_BASE,
                "increment base model version is stale",
                {
                    "base_version": per_model_base,
                    "current_model_version": current_version,
                    "model_id": model_id,
                    "affected_models": affected,
                },
            )

    def _apply_increment_transaction(
        self,
        envelope: dict[str, Any],
        intent_id: str,
    ) -> dict[str, Any]:
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                state = self._read_state(connection)
                if state is None:
                    raise BusinessError(
                        E_STATE_EMPTY,
                        "an increment requires an existing committed snapshot",
                    )
                self._assert_increment_base(envelope, state)
                if envelope["snapshot_seq"] <= state["snapshot_seq"]:
                    raise BusinessError(
                        E_SNAPSHOT_STALE,
                        "increment sequence must increase monotonically",
                        {
                            "requested_snapshot_seq": envelope["snapshot_seq"],
                            "current_snapshot_seq": state["snapshot_seq"],
                        },
                    )

                old_active = state["active_models"]
                old_graphs: dict[tuple[str, str], dict[str, Any]] = {}
                for model_id, config in envelope["active_models"].items():
                    if model_id not in old_active:
                        continue
                    row = connection.execute(
                        "SELECT graph_json FROM model_versions WHERE model_id = ? AND version = ?",
                        (model_id, old_active[model_id]["version"]),
                    ).fetchone()
                    if row is not None:
                        old_graphs[(model_id, old_active[model_id]["version"])] = _json_loads(
                            row["graph_json"]
                        )

                for record in envelope["records"]:
                    existing = connection.execute(
                        """
                        SELECT graph_signature FROM model_versions
                        WHERE model_id = ? AND version = ?
                        """,
                        (record["model_id"], record["version"]),
                    ).fetchone()
                    if existing is not None and existing["graph_signature"] != record["signature"]:
                        raise BusinessError(
                            E_VERSION_CONFLICT,
                            "an increment cannot mutate an already committed model version",
                            {
                                "model_id": record["model_id"],
                                "version": record["version"],
                            },
                        )

                known_vocabs = list(state["known_vocabs"])
                for vocab_id in envelope["known_vocabs"]:
                    if vocab_id not in known_vocabs:
                        known_vocabs.append(vocab_id)

                new_active = json.loads(canonical_json(old_active))
                for model_id, config in envelope["active_models"].items():
                    new_active[model_id] = config

                for record in envelope["records"]:
                    connection.execute(
                        """
                        INSERT INTO model_versions(
                            model_id, version, graph_json, graph_signature,
                            source_snapshot_seq, created_at
                        ) VALUES(?, ?, ?, ?, ?, ?)
                        ON CONFLICT(model_id, version) DO UPDATE SET
                            source_snapshot_seq=excluded.source_snapshot_seq
                        """,
                        (
                            record["model_id"],
                            record["version"],
                            canonical_json(record["graph"]),
                            record["signature"],
                            envelope["snapshot_seq"],
                            utc_now(),
                        ),
                    )
                for vocab_id in known_vocabs:
                    connection.execute(
                        """
                        INSERT INTO known_vocabs(vocab_id, snapshot_seq) VALUES(?, ?)
                        ON CONFLICT(vocab_id) DO UPDATE SET snapshot_seq=excluded.snapshot_seq
                        """,
                        (vocab_id, envelope["snapshot_seq"]),
                    )

                for model_id, config in envelope["active_models"].items():
                    version_row = connection.execute(
                        "SELECT graph_json, graph_signature FROM model_versions WHERE model_id = ? AND version = ?",
                        (model_id, config["version"]),
                    ).fetchone()
                    if version_row is None:
                        raise BusinessError(
                            E_VERSION_NOT_FOUND,
                            "active model version is not available after applying the increment",
                            {"model_id": model_id, "version": config["version"]},
                        )
                    previous = config.get("previous_version")
                    if previous is None and model_id in old_active:
                        previous = old_active[model_id]["version"]
                    if previous is None:
                        previous = config["version"]
                    config["previous_version"] = previous
                    if config["gray_ratio_ppm"] > 0 and previous != config["version"]:
                        previous_row = connection.execute(
                            "SELECT 1 FROM model_versions WHERE model_id = ? AND version = ?",
                            (model_id, previous),
                        ).fetchone()
                        if previous_row is None:
                            raise BusinessError(
                                E_VERSION_NOT_FOUND,
                                "gray traffic requires the previous version graph",
                                {"model_id": model_id, "version": previous},
                            )

                signature_map: dict[str, Any] = {}
                for model_id, config in sorted(new_active.items()):
                    row = connection.execute(
                        "SELECT graph_signature FROM model_versions WHERE model_id = ? AND version = ?",
                        (model_id, config["version"]),
                    ).fetchone()
                    signature_map[model_id] = {
                        "version": config["version"],
                        "signature": row["graph_signature"] if row else None,
                    }
                state_token = self._make_state_token(
                    INTENT_INCREMENT,
                    envelope["snapshot_seq"],
                    new_active,
                    signature_map,
                    known_vocabs,
                )

                manifests: list[dict[str, Any]] = []
                for model_id, config in sorted(envelope["active_models"].items()):
                    old_config = old_active.get(model_id)
                    old_graph = old_graphs.get((model_id, old_config["version"])) if old_config else None
                    new_graph = _json_loads(
                        connection.execute(
                            "SELECT graph_json FROM model_versions WHERE model_id = ? AND version = ?",
                            (model_id, config["version"]),
                        ).fetchone()["graph_json"]
                    )
                    manifests.append(
                        self._apply_switch(
                            connection=connection,
                            model_id=model_id,
                            old_config=old_config,
                            old_graph=old_graph,
                            new_config=config,
                            new_graph=new_graph,
                            snapshot_seq=envelope["snapshot_seq"],
                            state_token=state_token,
                        )
                    )

                new_state = {
                    "snapshot_seq": envelope["snapshot_seq"],
                    "state_token": state_token,
                    "generated_at": envelope["generated_at"],
                    "active_models": new_active,
                    "known_vocabs": known_vocabs,
                    "revision": state["revision"] + 1,
                    "updated_at": utc_now(),
                }
                self._write_state(connection, new_state)
                self._finish_intent_connection(connection, intent_id, "committed")
                response = {
                    "snapshot_seq": envelope["snapshot_seq"],
                    "state_version": state_token,
                    "base_version": state["state_token"],
                    "generated_at": envelope["generated_at"],
                    "active_models": new_active,
                    "known_vocabs": known_vocabs,
                    "migrations": manifests,
                    "atomic_commit": True,
                }
        finally:
            connection.close()
        return response

    # -------------------------------------------------------- artifact switch
    @staticmethod
    def _current_artifact_generation(
        connection,
        model_id: str,
        version: str,
        kind: str,
    ) -> int:
        if kind == "route":
            row = connection.execute(
                "SELECT generation FROM route_entries WHERE model_id = ? AND version = ?",
                (model_id, version),
            ).fetchone()
        else:
            row = connection.execute(
                """
                SELECT generation FROM derived_artifacts
                WHERE model_id = ? AND version = ? AND kind = ?
                """,
                (model_id, version, kind),
            ).fetchone()
        return int(row["generation"]) if row is not None else 0

    @staticmethod
    def _preprocess_cache_payload(
        graph: dict[str, Any],
        model_id: str,
        version: str,
        snapshot_seq: int,
        generation: int,
        migrated_from: str | None,
    ) -> dict[str, Any]:
        layout_payload = {
            "inputs": graph["inputs"],
            "nodes": [
                {
                    "id": node["id"],
                    "op": node["op"],
                    "inputs": node["inputs"],
                    "outputs": node["outputs"],
                    "preprocess": node["preprocess"],
                    "postprocess": node["postprocess"],
                }
                for node in graph["nodes"]
            ],
            "vocab_refs": graph.get("vocab_refs", []),
        }
        layout_key = sha256_json(layout_payload)
        quant_bits = [node["quant_bits"] for node in graph["nodes"]]
        cache_key = sha256_json({"layout_key": layout_key, "quant_bits": quant_bits})
        allocation_bytes = 0
        for node in graph["nodes"]:
            for spec in node["inputs"] + node["outputs"]:
                allocation_bytes += int(np.prod(spec["shape"], dtype=np.int64)) * np.dtype(
                    spec["dtype"]
                ).itemsize
        return {
            "model_id": model_id,
            "version": version,
            "snapshot_seq": snapshot_seq,
            "generation": generation,
            "input": graph["inputs"][0],
            "steps": preprocess_steps(graph),
            "quant_bits": quant_bits,
            "layout_key": layout_key,
            "cache_key": cache_key,
            "allocation_bytes": allocation_bytes,
            "migrated_from": migrated_from,
        }

    @staticmethod
    def _inflight_index_payload(
        graph: dict[str, Any],
        model_id: str,
        version: str,
        snapshot_seq: int,
        generation: int,
        migrated_from: str | None,
        pending_count: int,
    ) -> dict[str, Any]:
        layout = {
            "input": graph["inputs"][0],
            "node_io": [
                {
                    "id": node["id"],
                    "inputs": [{"shape": spec["shape"], "dtype": spec["dtype"]} for spec in node["inputs"]],
                    "outputs": [{"shape": spec["shape"], "dtype": spec["dtype"]} for spec in node["outputs"]],
                }
                for node in graph["nodes"]
            ],
        }
        return {
            "model_id": model_id,
            "version": version,
            "snapshot_seq": snapshot_seq,
            "generation": generation,
            "index_key": sha256_json(layout),
            "binding_policy": "pin_version_on_request_entry",
            "pending_bindings_visible": pending_count,
            "migrated_from": migrated_from,
        }

    @staticmethod
    def _gray_plan_payload(
        model_id: str,
        old_version: str,
        new_version: str,
        snapshot_seq: int,
        generation: int,
        ratio_ppm: int,
        ratio_rounding_error: str,
        accumulator: str,
        compensation: str,
        decision_count: int,
        migrated_from: str | None,
    ) -> dict[str, Any]:
        return {
            "model_id": model_id,
            "old_version": old_version,
            "new_version": new_version,
            "snapshot_seq": snapshot_seq,
            "generation": generation,
            "fixed_point": {"units": ratio_ppm, "scale": RATIO_SCALE},
            "normalized_ratio": decimal_text(Decimal(ratio_ppm) / Decimal(RATIO_SCALE)),
            "rounding_error": ratio_rounding_error,
            "accumulator": accumulator,
            "compensation": compensation,
            "compensated_summation": "kahan_decimal",
            "decision_count": decision_count,
            "migrated_from": migrated_from,
        }

    def _apply_switch(
        self,
        connection,
        model_id: str,
        old_config: dict[str, Any] | None,
        old_graph: dict[str, Any] | None,
        new_config: dict[str, Any],
        new_graph: dict[str, Any],
        snapshot_seq: int,
        state_token: str,
    ) -> dict[str, Any]:
        old_version = old_config["version"] if old_config else None
        new_version = new_config["version"]
        diff = diff_graphs(old_graph, new_graph)
        ratio_ppm = int(new_config["gray_ratio_ppm"])
        ratio_rounding_error = new_config["gray_ratio_rounding_error"]
        old_version_available = False
        if old_version is not None:
            old_version_available = (
                connection.execute(
                    "SELECT 1 FROM model_versions WHERE model_id = ? AND version = ?",
                    (model_id, old_version),
                ).fetchone()
                is not None
            )
        if old_version is not None and old_version_available:
            previous_version = old_version
        elif ratio_ppm > 0 and new_version != (old_version or new_version):
            raise BusinessError(
                E_VERSION_NOT_FOUND,
                "gray traffic requires the previous version graph to remain available",
                {
                    "model_id": model_id,
                    "previous_version": old_version,
                    "new_version": new_version,
                },
            )
        else:
            previous_version = new_version
        new_config["previous_version"] = previous_version

        old_plan = connection.execute(
            "SELECT * FROM gray_plans WHERE model_id = ?", (model_id,)
        ).fetchone()
        old_plan_ratio = int(old_plan["ratio_ppm"]) if old_plan is not None else None
        ratio_same = old_plan is not None and old_plan_ratio == ratio_ppm

        actions: dict[str, str] = {}
        if old_graph is None:
            actions = {kind: "create" for kind in ARTIFACT_KINDS}
        else:
            version_switch = (
                old_version != new_version
                or diff["mask"] != 0
                or old_graph is None
            )
            actions["route"] = "updated" if (version_switch or not ratio_same) else "retained"

            if diff["mask"] == 0 and old_version == new_version:
                actions["preprocess_cache"] = "retained"
                actions["inflight_index"] = "retained"
            elif diff["mask"] == 0:
                actions["preprocess_cache"] = "migrated"
                actions["inflight_index"] = "migrated"
            elif diff["mask"] == DIFF_QUANT:
                actions["preprocess_cache"] = "migrated"
                actions["inflight_index"] = "migrated"
            else:
                preprocess_rebuild_bits = DIFF_SHAPE | DIFF_DTYPE | DIFF_PREPOST | DIFF_VOCAB
                actions["preprocess_cache"] = (
                    "rebuild" if diff["mask"] & preprocess_rebuild_bits else "migrated"
                )
                actions["inflight_index"] = "rebuild"

            if (
                old_version == new_version
                and diff["mask"] == 0
                and ratio_same
                and old_plan is not None
            ):
                actions["gray_plan"] = "retained"
            elif diff["mask"] in (0, DIFF_QUANT) and ratio_same and old_plan is not None:
                actions["gray_plan"] = "migrated"
            else:
                actions["gray_plan"] = "rebuild"

        generations: dict[str, int] = {}
        for kind in ARTIFACT_KINDS:
            current_generation = self._current_artifact_generation(
                connection, model_id, new_version, kind
            )
            if actions[kind] == "retained":
                generations[kind] = current_generation or 1
            else:
                generations[kind] = current_generation + 1

        pending_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM bindings WHERE model_id = ? AND status = 'pending'",
                (model_id,),
            ).fetchone()["count"]
        )
        migrated_from = None if actions["preprocess_cache"] == "create" else old_version
        preprocess_payload = self._preprocess_cache_payload(
            new_graph,
            model_id,
            new_version,
            snapshot_seq,
            generations["preprocess_cache"],
            migrated_from if actions["preprocess_cache"] == "migrated" else None,
        )
        inflight_payload = self._inflight_index_payload(
            new_graph,
            model_id,
            new_version,
            snapshot_seq,
            generations["inflight_index"],
            old_version if actions["inflight_index"] == "migrated" else None,
            pending_count,
        )

        if actions["gray_plan"] in {"migrated", "retained"} and old_plan is not None:
            accumulator = old_plan["accumulator"]
            compensation = old_plan["compensation"]
            decision_count = int(old_plan["decision_count"])
        else:
            accumulator = "0"
            compensation = "0"
            decision_count = 0
        gray_payload = self._gray_plan_payload(
            model_id=model_id,
            old_version=previous_version,
            new_version=new_version,
            snapshot_seq=snapshot_seq,
            generation=generations["gray_plan"],
            ratio_ppm=ratio_ppm,
            ratio_rounding_error=ratio_rounding_error,
            accumulator=accumulator,
            compensation=compensation,
            decision_count=decision_count,
            migrated_from=old_version if actions["gray_plan"] == "migrated" else None,
        )
        route_payload = {
            "model_id": model_id,
            "version": new_version,
            "previous_version": previous_version,
            "snapshot_seq": snapshot_seq,
            "active": True,
            "state_token": state_token,
        }

        connection.execute(
            "UPDATE route_entries SET active = 0 WHERE model_id = ?",
            (model_id,),
        )
        connection.execute(
            """
            INSERT INTO route_entries(
                model_id, version, snapshot_seq, active, payload_json, generation
            ) VALUES(?, ?, ?, 1, ?, ?)
            ON CONFLICT(model_id, version) DO UPDATE SET
                snapshot_seq=excluded.snapshot_seq,
                active=1,
                payload_json=excluded.payload_json,
                generation=excluded.generation
            """,
            (
                model_id,
                new_version,
                snapshot_seq,
                canonical_json(route_payload),
                generations["route"],
            ),
        )

        artifact_payloads = {
            "route": route_payload,
            "preprocess_cache": preprocess_payload,
            "inflight_index": inflight_payload,
            "gray_plan": gray_payload,
        }
        for kind, artifact_payload in artifact_payloads.items():
            connection.execute(
                """
                INSERT INTO derived_artifacts(
                    model_id, version, kind, snapshot_seq, generation, payload_json
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(model_id, version, kind) DO UPDATE SET
                    snapshot_seq=excluded.snapshot_seq,
                    generation=excluded.generation,
                    payload_json=excluded.payload_json
                """,
                (
                    model_id,
                    new_version,
                    kind,
                    snapshot_seq,
                    generations[kind],
                    canonical_json(artifact_payload),
                ),
            )

        connection.execute(
            """
            INSERT INTO gray_plans(
                model_id, snapshot_seq, old_version, new_version, ratio_ppm,
                ratio_rounding_error, accumulator, compensation, decision_count, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_id) DO UPDATE SET
                snapshot_seq=excluded.snapshot_seq,
                old_version=excluded.old_version,
                new_version=excluded.new_version,
                ratio_ppm=excluded.ratio_ppm,
                ratio_rounding_error=excluded.ratio_rounding_error,
                accumulator=excluded.accumulator,
                compensation=excluded.compensation,
                decision_count=excluded.decision_count,
                updated_at=excluded.updated_at
            """,
            (
                model_id,
                snapshot_seq,
                previous_version,
                new_version,
                ratio_ppm,
                ratio_rounding_error,
                accumulator,
                compensation,
                decision_count,
                utc_now(),
            ),
        )

        artifact_records: list[dict[str, Any]] = []
        for kind in ARTIFACT_KINDS:
            reasons: list[str] = []
            if actions[kind] in {"updated", "migrated", "rebuild"}:
                if kind == "route":
                    if old_version != new_version:
                        reasons.append("version_switch")
                    if not ratio_same:
                        reasons.append("gray_plan_changed")
                elif kind == "gray_plan":
                    if not ratio_same:
                        reasons.append("gray_ratio_changed")
                    reasons.extend(diff["names"])
                else:
                    reasons.extend(diff["names"])
                    if actions[kind] == "migrated" and diff["mask"] == DIFF_QUANT:
                        reasons.append("quant_only_in_place_migration")
            artifact_records.append(
                {
                    "kind": kind,
                    "action": actions[kind],
                    "reasons": reasons,
                    "generation": generations[kind],
                }
            )

        rebuild = [record["kind"] for record in artifact_records if record["action"] == "rebuild"]
        migrate = [record["kind"] for record in artifact_records if record["action"] == "migrated"]
        retained = [record["kind"] for record in artifact_records if record["action"] == "retained"]
        created = [record["kind"] for record in artifact_records if record["action"] == "create"]
        updated = [record["kind"] for record in artifact_records if record["action"] == "updated"]
        migration_id = f"migration-{uuid.uuid4().hex}"
        manifest = {
            "migration_id": migration_id,
            "snapshot_seq": snapshot_seq,
            "state_version": state_token,
            "model_id": model_id,
            "old_version": old_version,
            "new_version": new_version,
            "diff": diff,
            "artifacts": artifact_records,
            "rebuild": rebuild,
            "migrate": migrate,
            "retained": retained,
            "created": created,
            "updated": updated,
            "summary": {
                "rebuild_count": len(rebuild),
                "migrate_count": len(migrate),
                "retained_count": len(retained),
                "created_count": len(created),
                "updated_count": len(updated),
            },
            "atomic_transaction": True,
        }
        connection.execute(
            """
            INSERT INTO migrations(
                migration_id, snapshot_seq, state_token, model_id, old_version,
                new_version, diff_mask, diff_names_json, in_place, mode,
                manifest_json, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                migration_id,
                snapshot_seq,
                state_token,
                model_id,
                old_version,
                new_version,
                diff["mask"],
                canonical_json(diff["names"]),
                1 if diff["in_place_migratable"] else 0,
                diff["mode"],
                canonical_json(manifest),
                utc_now(),
            ),
        )
        return manifest

    # --------------------------------------------------------- gray decisions
    @staticmethod
    def _decision_key(state_token: str, model_id: str, request_id: str) -> str:
        return sha256_json(
            {
                "state_token": state_token,
                "model_id": model_id,
                "request_id": request_id,
            }
        )

    @staticmethod
    def _bind_row_to_dict(row) -> dict[str, Any]:
        return {
            "request_id": row["request_id"],
            "model_id": row["model_id"],
            "version": row["version"],
            "seed": row["seed"],
            "status": row["status"],
            "bound_snapshot_seq": int(row["bound_snapshot_seq"]),
            "bound_state_token": row["bound_state_token"],
            "graph": _json_loads(row["graph_json"]),
            "vocab_refs": _json_loads(row["vocab_refs_json"]),
            "known_vocabs": _json_loads(row["known_vocabs_json"]),
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
            "result": _json_loads(row["result_json"]) if row["result_json"] else None,
            "result_digest": row["result_digest"],
        }

    def _get_binding(self, request_id: str) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM bindings WHERE request_id = ?", (request_id,)
            ).fetchone()
            return self._bind_row_to_dict(row) if row is not None else None
        finally:
            connection.close()

    @staticmethod
    def _coerce_ratio_with_current(
        ratio: Any,
        plan_row,
    ) -> dict[str, Any]:
        try:
            return normalize_ratio(ratio)
        except BusinessError as exc:
            current_ratio = None
            if plan_row is not None:
                current_ratio = decimal_text(
                    Decimal(int(plan_row["ratio_ppm"])) / Decimal(RATIO_SCALE)
                )
            exc.context["current_ratio"] = current_ratio
            exc.context["legal_interval"] = [0, 1]
            raise

    def decide_gray(
        self,
        request_id: str,
        model_id: str,
        ratio: Any | None = None,
    ) -> dict[str, Any]:
        request_id = _require_string(request_id, "request_id")
        model_id = _require_string(model_id, "model_id")
        state = self._current_state()
        if state is None:
            raise BusinessError(E_STATE_EMPTY, "no snapshot is available for gray routing")
        intent_id = self._start_intent(
            INTENT_GRAY,
            state["snapshot_seq"],
            state["state_token"],
            {
                "request_id": request_id,
                "model_id": model_id,
                "ratio_override": None if ratio is None else str(ratio),
            },
        )
        try:
            result = self._decide_gray_transaction(request_id, model_id, ratio, intent_id)
        except Exception:
            self._finish_intent(intent_id, "aborted")
            raise
        return result

    def _decide_gray_transaction(
        self,
        request_id: str,
        model_id: str,
        ratio_override: Any | None,
        intent_id: str,
    ) -> dict[str, Any]:
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                state = self._read_state(connection)
                if state is None:
                    raise BusinessError(E_STATE_EMPTY, "no snapshot is available for gray routing")
                if model_id not in state["active_models"]:
                    raise BusinessError(
                        E_MODEL_NOT_FOUND,
                        "model is not in the active route table",
                        {"model_id": model_id, "snapshot_seq": state["snapshot_seq"]},
                    )
                plan_row = connection.execute(
                    "SELECT * FROM gray_plans WHERE model_id = ?", (model_id,)
                ).fetchone()
                if plan_row is None:
                    raise BusinessError(
                        E_MODEL_NOT_FOUND,
                        "gray plan is unavailable for the model",
                        {"model_id": model_id},
                    )

                if ratio_override is None:
                    ratio_info = {
                        "input": decimal_text(
                            Decimal(int(plan_row["ratio_ppm"])) / Decimal(RATIO_SCALE)
                        ),
                        "units": int(plan_row["ratio_ppm"]),
                        "scale": RATIO_SCALE,
                        "normalized": decimal_text(
                            Decimal(int(plan_row["ratio_ppm"])) / Decimal(RATIO_SCALE)
                        ),
                        "rounding_error": plan_row["ratio_rounding_error"],
                    }
                else:
                    ratio_info = self._coerce_ratio_with_current(ratio_override, plan_row)

                decision_key = self._decision_key(state["state_token"], model_id, request_id)
                existing_decision = connection.execute(
                    "SELECT * FROM gray_decisions WHERE decision_key = ?",
                    (decision_key,),
                ).fetchone()
                if existing_decision is not None:
                    details = _json_loads(existing_decision["details_json"])
                    details["replayed"] = True
                    self._finish_intent_connection(connection, intent_id, "committed")
                    return details

                binding_row = connection.execute(
                    "SELECT * FROM bindings WHERE request_id = ?", (request_id,)
                ).fetchone()
                binding_created = False
                if binding_row is not None:
                    if binding_row["model_id"] != model_id:
                        raise BusinessError(
                            E_REQUEST_BINDING_MISMATCH,
                            "request_id is already bound to a different model",
                            {
                                "request_id": request_id,
                                "bound_model_id": binding_row["model_id"],
                                "requested_model_id": model_id,
                            },
                        )
                    target_version = binding_row["version"]
                    version_role = "prebound"
                    kahan_before = {
                        "accumulator": plan_row["accumulator"],
                        "compensation": plan_row["compensation"],
                    }
                    kahan_after = dict(kahan_before)
                else:
                    old_version = plan_row["old_version"]
                    new_version = plan_row["new_version"]
                    accumulator = Decimal(plan_row["accumulator"])
                    compensation = Decimal(plan_row["compensation"])
                    kahan_before = {
                        "accumulator": decimal_text(accumulator),
                        "compensation": decimal_text(compensation),
                    }
                    units = int(ratio_info["units"])
                    if old_version == new_version:
                        target_version = new_version
                        version_role = "new"
                    elif units <= 0:
                        target_version = old_version
                        version_role = "old"
                    elif units >= RATIO_SCALE:
                        target_version = new_version
                        version_role = "new"
                    else:
                        ratio_decimal = Decimal(units) / Decimal(RATIO_SCALE)
                        compensated_increment = ratio_decimal - compensation
                        total = accumulator + compensated_increment
                        compensation = (total - accumulator) - compensated_increment
                        accumulator = total
                        if accumulator >= Decimal(1):
                            target_version = new_version
                            version_role = "new"
                            accumulator -= Decimal(1)
                        else:
                            target_version = old_version
                            version_role = "old"
                    kahan_after = {
                        "accumulator": decimal_text(accumulator),
                        "compensation": decimal_text(compensation),
                    }
                    version_row = connection.execute(
                        """
                        SELECT graph_json FROM model_versions
                        WHERE model_id = ? AND version = ?
                        """,
                        (model_id, target_version),
                    ).fetchone()
                    if version_row is None:
                        raise BusinessError(
                            E_VERSION_NOT_FOUND,
                            "gray-selected model version graph is unavailable",
                            {"model_id": model_id, "version": target_version},
                        )
                    graph = _json_loads(version_row["graph_json"])
                    connection.execute(
                        """
                        INSERT INTO bindings(
                            request_id, model_id, version, seed, status,
                            bound_snapshot_seq, bound_state_token, graph_json,
                            vocab_refs_json, known_vocabs_json, created_at,
                            completed_at, result_json, result_digest
                        ) VALUES(?, ?, ?, NULL, 'pending', ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                        """,
                        (
                            request_id,
                            model_id,
                            target_version,
                            state["snapshot_seq"],
                            state["state_token"],
                            canonical_json(graph),
                            canonical_json(graph_vocab_refs(graph)),
                            canonical_json(state["known_vocabs"]),
                            utc_now(),
                        ),
                    )
                    binding_created = True
                    connection.execute(
                        """
                        UPDATE gray_plans
                        SET accumulator = ?, compensation = ?, decision_count = decision_count + 1,
                            updated_at = ?
                        WHERE model_id = ?
                        """,
                        (
                            kahan_after["accumulator"],
                            kahan_after["compensation"],
                            utc_now(),
                            model_id,
                        ),
                    )

                details = {
                    "decision_key": decision_key,
                    "request_id": request_id,
                    "model_id": model_id,
                    "snapshot_seq": state["snapshot_seq"],
                    "state_version": state["state_token"],
                    "target_version": target_version,
                    "version_role": version_role,
                    "ratio": ratio_info["input"],
                    "fixed_point": {
                        "units": int(ratio_info["units"]),
                        "scale": int(ratio_info["scale"]),
                    },
                    "normalized_ratio": ratio_info["normalized"],
                    "ratio_rounding_error": ratio_info["rounding_error"],
                    "kahan_before": kahan_before,
                    "kahan_after": kahan_after,
                    "compensated_summation": "kahan_decimal",
                    "binding_created": binding_created,
                    "replayed": False,
                }
                connection.execute(
                    """
                    INSERT INTO gray_decisions(
                        decision_key, state_token, snapshot_seq, model_id, request_id,
                        target_version, version_role, details_json, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision_key,
                        state["state_token"],
                        state["snapshot_seq"],
                        model_id,
                        request_id,
                        target_version,
                        version_role,
                        canonical_json(details),
                        utc_now(),
                    ),
                )
                self._finish_intent_connection(connection, intent_id, "committed")
                return details
        finally:
            connection.close()

    def normalize_ratio(self, ratio: Any) -> dict[str, Any]:
        state = self._current_state()
        plan_row = None
        if state is not None:
            connection = self._connect()
            try:
                plan_row = connection.execute(
                    "SELECT * FROM gray_plans WHERE model_id = ?",
                    (next(iter(state["active_models"]), ""),),
                ).fetchone()
            finally:
                connection.close()
        try:
            return normalize_ratio(ratio)
        except BusinessError as exc:
            if plan_row is not None:
                exc.context["current_ratio"] = decimal_text(
                    Decimal(int(plan_row["ratio_ppm"])) / Decimal(RATIO_SCALE)
                )
            else:
                exc.context["current_ratio"] = None
            exc.context["legal_interval"] = [0, 1]
            raise

    # ------------------------------------------------------ bindings/inference
    def _bind_explicit(
        self,
        request_id: str,
        model_id: str,
        version: str,
        seed: int | None,
    ) -> dict[str, Any]:
        state = self._current_state()
        if state is None:
            raise BusinessError(E_STATE_EMPTY, "no snapshot is available for request binding")
        intent_id = self._start_intent(
            INTENT_BINDING,
            state["snapshot_seq"],
            state["state_token"],
            {"request_id": request_id, "model_id": model_id, "version": version},
        )
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                state = self._read_state(connection)
                if state is None:
                    raise BusinessError(E_STATE_EMPTY, "no snapshot is available for request binding")
                existing = connection.execute(
                    "SELECT * FROM bindings WHERE request_id = ?", (request_id,)
                ).fetchone()
                if existing is not None:
                    if existing["model_id"] != model_id or existing["version"] != version:
                        raise BusinessError(
                            E_REQUEST_BINDING_MISMATCH,
                            "request_id is already bound to another model version",
                            {
                                "request_id": request_id,
                                "bound_model_id": existing["model_id"],
                                "bound_version": existing["version"],
                                "requested_model_id": model_id,
                                "requested_version": version,
                            },
                        )
                    self._finish_intent_connection(connection, intent_id, "committed")
                    return self._bind_row_to_dict(existing)
                version_row = connection.execute(
                    """
                    SELECT graph_json FROM model_versions
                    WHERE model_id = ? AND version = ?
                    """,
                    (model_id, version),
                ).fetchone()
                if version_row is None:
                    raise BusinessError(
                        E_VERSION_NOT_FOUND,
                        "requested model version is unavailable",
                        {"model_id": model_id, "version": version},
                    )
                graph = _json_loads(version_row["graph_json"])
                connection.execute(
                    """
                    INSERT INTO bindings(
                        request_id, model_id, version, seed, status,
                        bound_snapshot_seq, bound_state_token, graph_json,
                        vocab_refs_json, known_vocabs_json, created_at,
                        completed_at, result_json, result_digest
                    ) VALUES(?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                    """,
                    (
                        request_id,
                        model_id,
                        version,
                        seed,
                        state["snapshot_seq"],
                        state["state_token"],
                        canonical_json(graph),
                        canonical_json(graph_vocab_refs(graph)),
                        canonical_json(state["known_vocabs"]),
                        utc_now(),
                    ),
                )
                self._finish_intent_connection(connection, intent_id, "committed")
                row = connection.execute(
                    "SELECT * FROM bindings WHERE request_id = ?", (request_id,)
                ).fetchone()
                return self._bind_row_to_dict(row)
        except Exception:
            self._finish_intent(intent_id, "aborted")
            raise
        finally:
            connection.close()

    def infer(
        self,
        request_id: str,
        model_id: str,
        seed: int = 42,
        version: str | None = None,
    ) -> dict[str, Any]:
        request_id = _require_string(request_id, "request_id")
        model_id = _require_string(model_id, "model_id")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise BusinessError(
                E_BAD_REQUEST,
                "seed must be an integer",
                {"field": "seed", "value": seed},
            )
        if version is not None:
            version = _require_string(version, "version")

        binding = self._get_binding(request_id)
        if binding is None:
            if version is None:
                self.decide_gray(request_id, model_id)
            else:
                binding = self._bind_explicit(request_id, model_id, version, seed)
            binding = self._get_binding(request_id)
        if binding is None:
            raise BusinessError(
                E_INTERNAL,
                "request binding could not be established",
                {"request_id": request_id},
            )
        if binding["model_id"] != model_id:
            raise BusinessError(
                E_REQUEST_BINDING_MISMATCH,
                "request_id is already bound to a different model",
                {
                    "request_id": request_id,
                    "bound_model_id": binding["model_id"],
                    "requested_model_id": model_id,
                },
            )
        if binding["version"] != version and version is not None:
            raise BusinessError(
                E_REQUEST_BINDING_MISMATCH,
                "request_id is already bound to another model version",
                {
                    "request_id": request_id,
                    "bound_version": binding["version"],
                    "requested_version": version,
                },
            )
        if binding["status"] == "interrupted":
            raise BusinessError(
                E_REQUEST_INTERRUPTED,
                "request binding was interrupted by a process restart",
                {
                    "request_id": request_id,
                    "bound_version": binding["version"],
                    "bound_snapshot_seq": binding["bound_snapshot_seq"],
                },
            )
        if binding["status"] == "failed":
            raise BusinessError(
                E_REQUEST_INTERRUPTED,
                "request binding previously failed",
                {"request_id": request_id, "bound_version": binding["version"]},
            )
        if binding["seed"] is not None and binding["seed"] != seed:
            raise BusinessError(
                E_REQUEST_BINDING_MISMATCH,
                "request_id is bound to a different deterministic seed",
                {
                    "request_id": request_id,
                    "bound_seed": binding["seed"],
                    "requested_seed": seed,
                },
            )
        if binding["status"] == "completed" and binding["result"] is not None:
            return {
                "request_id": request_id,
                "model_id": model_id,
                "bound_version": binding["version"],
                "bound_snapshot_seq": binding["bound_snapshot_seq"],
                "binding_status": "completed",
                "cached": True,
                "result": binding["result"],
            }

        intent_id = self._start_intent(
            INTENT_INFERENCE,
            binding["bound_snapshot_seq"],
            binding["bound_state_token"],
            {
                "request_id": request_id,
                "model_id": model_id,
                "version": binding["version"],
                "seed": seed,
            },
        )
        try:
            result = execute_graph(
                binding["graph"],
                model_id=model_id,
                seed=seed,
                known_vocabs=binding["known_vocabs"],
            )
            result["bound_version"] = binding["version"]
            result["bound_snapshot_seq"] = binding["bound_snapshot_seq"]
            result["binding_policy"] = "version_pinned_at_request_entry"
            connection = self._connect()
            try:
                with immediate_transaction(connection):
                    current = connection.execute(
                        "SELECT * FROM bindings WHERE request_id = ?", (request_id,)
                    ).fetchone()
                    if current is None:
                        raise BusinessError(
                            E_REQUEST_BINDING_MISMATCH,
                            "request binding disappeared during inference",
                            {"request_id": request_id},
                        )
                    if current["seed"] is not None and current["seed"] != seed:
                        raise BusinessError(
                            E_REQUEST_BINDING_MISMATCH,
                            "request_id is bound to a different deterministic seed",
                            {
                                "request_id": request_id,
                                "bound_seed": current["seed"],
                                "requested_seed": seed,
                            },
                        )
                    if current["status"] == "completed" and current["result_json"]:
                        cached_result = _json_loads(current["result_json"])
                        self._finish_intent_connection(connection, intent_id, "committed")
                        return {
                            "request_id": request_id,
                            "model_id": model_id,
                            "bound_version": current["version"],
                            "bound_snapshot_seq": int(current["bound_snapshot_seq"]),
                            "binding_status": "completed",
                            "cached": True,
                            "result": cached_result,
                        }
                    if current["status"] != "pending":
                        raise BusinessError(
                            E_REQUEST_INTERRUPTED,
                            "request binding is no longer pending",
                            {
                                "request_id": request_id,
                                "binding_status": current["status"],
                            },
                        )
                    connection.execute(
                        """
                        UPDATE bindings
                        SET seed = ?, status = 'completed', completed_at = ?,
                            result_json = ?, result_digest = ?
                        WHERE request_id = ? AND status = 'pending'
                        """,
                        (
                            seed,
                            utc_now(),
                            canonical_json(result),
                            result["result_digest"],
                            request_id,
                        ),
                    )
                    self._finish_intent_connection(connection, intent_id, "committed")
            finally:
                connection.close()
        except Exception as exc:
            self._finish_intent(intent_id, "aborted")
            if not isinstance(exc, BusinessError) or exc.code != E_VOCAB_MISSING:
                connection = self._connect()
                try:
                    with immediate_transaction(connection):
                        connection.execute(
                            """
                            UPDATE bindings SET status = 'failed', completed_at = ?
                            WHERE request_id = ? AND status = 'pending'
                            """,
                            (utc_now(), request_id),
                        )
                finally:
                    connection.close()
            raise
        return {
            "request_id": request_id,
            "model_id": model_id,
            "bound_version": binding["version"],
            "bound_snapshot_seq": binding["bound_snapshot_seq"],
            "binding_status": "completed",
            "cached": False,
            "result": result,
        }

    # --------------------------------------------------------------- queries
    def get_state(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            with read_transaction(connection):
                state = self._read_state(connection)
                if state is None:
                    raise BusinessError(E_STATE_EMPTY, "no snapshot has been committed yet")
                route_rows = connection.execute(
                    "SELECT * FROM route_entries WHERE active = 1 ORDER BY model_id"
                ).fetchall()
                routes = []
                artifacts = []
                gray_plans: dict[str, Any] = {}
                for row in route_rows:
                    routes.append(
                        {
                            "model_id": row["model_id"],
                            "version": row["version"],
                            "snapshot_seq": int(row["snapshot_seq"]),
                            "generation": int(row["generation"]),
                            "payload": _json_loads(row["payload_json"]),
                        }
                    )
                    artifact_rows = connection.execute(
                        """
                        SELECT kind, snapshot_seq, generation, payload_json
                        FROM derived_artifacts
                        WHERE model_id = ? AND version = ?
                        ORDER BY kind
                        """,
                        (row["model_id"], row["version"]),
                    ).fetchall()
                    for artifact_row in artifact_rows:
                        artifacts.append(
                            {
                                "model_id": row["model_id"],
                                "version": row["version"],
                                "kind": artifact_row["kind"],
                                "snapshot_seq": int(artifact_row["snapshot_seq"]),
                                "generation": int(artifact_row["generation"]),
                                "payload": _json_loads(artifact_row["payload_json"]),
                            }
                        )
                    plan_row = connection.execute(
                        "SELECT * FROM gray_plans WHERE model_id = ?", (row["model_id"],)
                    ).fetchone()
                    if plan_row is not None:
                        gray_plans[row["model_id"]] = {
                            "old_version": plan_row["old_version"],
                            "new_version": plan_row["new_version"],
                            "ratio_ppm": int(plan_row["ratio_ppm"]),
                            "ratio_rounding_error": plan_row["ratio_rounding_error"],
                            "accumulator": plan_row["accumulator"],
                            "compensation": plan_row["compensation"],
                            "decision_count": int(plan_row["decision_count"]),
                        }
        finally:
            connection.close()
        return {
            "snapshot_seq": state["snapshot_seq"],
            "state_version": state["state_token"],
            "generated_at": state["generated_at"],
            "revision": state["revision"],
            "active_models": state["active_models"],
            "known_vocabs": state["known_vocabs"],
            "routes": routes,
            "artifacts": artifacts,
            "gray_plans": gray_plans,
        }

    def list_migrations(
        self,
        snapshot_seq: int | None = None,
        model_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if snapshot_seq is not None:
            clauses.append("snapshot_seq = ?")
            parameters.append(int(snapshot_seq))
        if model_id is not None:
            clauses.append("model_id = ?")
            parameters.append(model_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        parameters.append(max(1, min(int(limit), 1000)))
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT manifest_json FROM migrations
                {where}
                ORDER BY snapshot_seq DESC, created_at DESC
                LIMIT ?
                """,
                tuple(parameters),
            ).fetchall()
            return [_json_loads(row["manifest_json"]) for row in rows]
        finally:
            connection.close()

    def get_artifacts(
        self,
        model_id: str | None = None,
        version: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        parameters: list[Any] = []
        if model_id is not None:
            clauses.append("model_id = ?")
            parameters.append(model_id)
        if version is not None:
            clauses.append("version = ?")
            parameters.append(version)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        connection = self._connect()
        try:
            rows = connection.execute(
                f"""
                SELECT model_id, version, kind, snapshot_seq, generation, payload_json
                FROM derived_artifacts {where}
                ORDER BY model_id, version, kind
                """,
                tuple(parameters),
            ).fetchall()
            return [
                {
                    "model_id": row["model_id"],
                    "version": row["version"],
                    "kind": row["kind"],
                    "snapshot_seq": int(row["snapshot_seq"]),
                    "generation": int(row["generation"]),
                    "payload": _json_loads(row["payload_json"]),
                }
                for row in rows
            ]
        finally:
            connection.close()

    def get_binding(self, request_id: str) -> dict[str, Any]:
        binding = self._get_binding(request_id)
        if binding is None:
            raise BusinessError(
                E_REQUEST_BINDING_MISMATCH,
                "request binding does not exist",
                {"request_id": request_id},
            )
        return binding

    # -------------------------------------------------------------- recovery
    def recover(self) -> dict[str, Any]:
        started_at = utc_now()
        connection = self._connect()
        try:
            with immediate_transaction(connection):
                state = self._read_state(connection)
                started_intents = connection.execute(
                    "SELECT * FROM intents WHERE status = 'started' ORDER BY created_at"
                ).fetchall()
                pending_bindings = connection.execute(
                    "SELECT * FROM bindings WHERE status = 'pending' ORDER BY created_at"
                ).fetchall()

                aborted_intents: list[dict[str, Any]] = []
                for row in started_intents:
                    aborted_intents.append(
                        {
                            "intent_id": row["intent_id"],
                            "kind": row["kind"],
                            "snapshot_seq": row["snapshot_seq"],
                            "base_token": row["base_token"],
                            "created_at": row["created_at"],
                        }
                    )
                    connection.execute(
                        """
                        UPDATE intents
                        SET status = 'aborted', completed_at = ?,
                            recovery_note = 'startup scan found an unfinished intent'
                        WHERE intent_id = ? AND status = 'started'
                        """,
                        (utc_now(), row["intent_id"]),
                    )
                interrupted_bindings: list[dict[str, Any]] = []
                for row in pending_bindings:
                    interrupted_bindings.append(
                        {
                            "request_id": row["request_id"],
                            "model_id": row["model_id"],
                            "version": row["version"],
                            "bound_snapshot_seq": int(row["bound_snapshot_seq"]),
                            "created_at": row["created_at"],
                        }
                    )
                    connection.execute(
                        """
                        UPDATE bindings
                        SET status = 'interrupted', completed_at = ?
                        WHERE request_id = ? AND status = 'pending'
                        """,
                        (utc_now(), row["request_id"]),
                    )

                rolled_back = {
                    "derived_artifacts": 0,
                    "route_entries": 0,
                    "gray_plans": 0,
                    "migrations": 0,
                    "model_versions": 0,
                }
                if state is not None:
                    current_seq = int(state["snapshot_seq"])
                    rolled_back["derived_artifacts"] = connection.execute(
                        "DELETE FROM derived_artifacts WHERE snapshot_seq > ?", (current_seq,)
                    ).rowcount
                    rolled_back["route_entries"] = connection.execute(
                        "DELETE FROM route_entries WHERE snapshot_seq > ?", (current_seq,)
                    ).rowcount
                    rolled_back["gray_plans"] = connection.execute(
                        "DELETE FROM gray_plans WHERE snapshot_seq > ?", (current_seq,)
                    ).rowcount
                    rolled_back["migrations"] = connection.execute(
                        "DELETE FROM migrations WHERE snapshot_seq > ?", (current_seq,)
                    ).rowcount
                    rolled_back["model_versions"] = connection.execute(
                        "DELETE FROM model_versions WHERE source_snapshot_seq > ?", (current_seq,)
                    ).rowcount

                report_id = f"recovery-{uuid.uuid4().hex}"
                finished_at = utc_now()
                details = {
                    "scope": "startup_scan_of_persisted_intents_and_bindings",
                    "persisted_facts_only": True,
                    "exit_report_available": False,
                    "startup_scan_complete": True,
                    "last_committed_snapshot_seq": state["snapshot_seq"] if state else None,
                    "last_committed_state_token": state["state_token"] if state else None,
                    "aborted_intent_count": len(aborted_intents),
                    "interrupted_binding_count": len(interrupted_bindings),
                    "rolled_back": rolled_back,
                    "caveat": (
                        "A forcibly terminated process cannot emit a complete exit report. "
                        "This report only records facts found in SQLite during the next startup scan."
                    ),
                }
                connection.execute(
                    """
                    INSERT INTO recovery_reports(
                        report_id, started_at, finished_at, last_committed_snapshot_seq,
                        state_token, aborted_intents_json, interrupted_bindings_json,
                        details_json, startup_scan_complete
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        report_id,
                        started_at,
                        finished_at,
                        state["snapshot_seq"] if state else None,
                        state["state_token"] if state else None,
                        canonical_json(aborted_intents),
                        canonical_json(interrupted_bindings),
                        canonical_json(details),
                    ),
                )
                report = {
                    "report_id": report_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "last_committed_snapshot_seq": state["snapshot_seq"] if state else None,
                    "state_version": state["state_token"] if state else None,
                    "aborted_intents": aborted_intents,
                    "interrupted_bindings": interrupted_bindings,
                    "details": details,
                }
        finally:
            connection.close()
        return report

    def get_recovery_report(self) -> dict[str, Any]:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM recovery_reports ORDER BY finished_at DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return {
                    "report_id": None,
                    "last_committed_snapshot_seq": None,
                    "state_version": None,
                    "aborted_intents": [],
                    "interrupted_bindings": [],
                    "details": {
                        "startup_scan_complete": False,
                        "persisted_facts_only": True,
                        "exit_report_available": False,
                    },
                }
            details = _json_loads(row["details_json"])
            return {
                "report_id": row["report_id"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "last_committed_snapshot_seq": row["last_committed_snapshot_seq"],
                "state_version": row["state_token"],
                "aborted_intents": _json_loads(row["aborted_intents_json"]),
                "interrupted_bindings": _json_loads(row["interrupted_bindings_json"]),
                "details": details,
            }
        finally:
            connection.close()



