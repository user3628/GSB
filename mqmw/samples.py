"""Load fixed local JSON samples into a fresh middleware database."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .errors import E_INVALID_ARGUMENT, MWError
from .store import SQLiteStore
from .utils import validate_loopback_url


def _read_json(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise MWError(E_INVALID_ARGUMENT, f"sample file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise MWError(E_INVALID_ARGUMENT, f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MWError(E_INVALID_ARGUMENT, f"sample root must be an object: {path}")
    return value


def load_samples(
    store: SQLiteStore,
    *,
    queues_path: str,
    consumers_path: str,
    messages_path: str,
) -> dict[str, Any]:
    queues_doc = _read_json(queues_path)
    consumers_doc = _read_json(consumers_path)
    messages_doc = _read_json(messages_path)

    loaded_queues = []
    for item in queues_doc.get("queues", []):
        queue_name = item.get("queue_name")
        store.register_queue(
            queue_name,
            item.get("priority", 100),
            request_id=f"sample-queue:{queue_name}",
        )
        loaded_queues.append(queue_name)

    loaded_consumers = []
    for item in consumers_doc.get("consumers", []):
        endpoint = validate_loopback_url(item.get("endpoint"), "endpoint")
        store.register_consumer(
            item.get("consumer_id"),
            item.get("group_name"),
            endpoint,
            item.get("queues", []),
            enabled=bool(item.get("enabled", True)),
            request_id=f"sample-consumer:{item.get('consumer_id')}",
        )
        loaded_consumers.append(item.get("consumer_id"))

    batches = []
    for index, batch in enumerate(messages_doc.get("batches", [])):
        queue_name = batch.get("queue_name")
        result = store.enqueue(
            queue_name,
            batch.get("messages", []),
            request_id=f"sample-enqueue:{queue_name}:{index}",
        )
        batches.append(result)
    return {
        "ok": True,
        "queues": loaded_queues,
        "consumers": loaded_consumers,
        "batches": batches,
        "revision": store.revision(),
    }
