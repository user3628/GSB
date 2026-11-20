from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from msmq.formats import (  # noqa: E402
    canonical_json,
    encode_compact,
    encode_jsonl,
    build_future_compact_sample,
)
from msmq.models import MessageRecord, QueueMeta  # noqa: E402

BASE_MS = 4_102_444_800_000
QUEUE_NAME = "demo-orders"
QUEUE = QueueMeta(
    name=QUEUE_NAME,
    semantics="at_least_once",
    visibility_timeout_ms=500,
    version=1,
    extensions={"owner": "local-sample", "purpose": "format-roundtrip"},
)


def sample_messages() -> list[MessageRecord]:
    values = [
        ("msg-0001", "order-created:1001", {"trace": "trace-1001", "region": "local"}),
        ("msg-0002", "order-created:1002", {"trace": "trace-1002"}),
        ("msg-0003", "order-created:1003", {"trace": "trace-1003", "priority": 3}),
        ("msg-0004", "order-created:1004", {}),
        ("msg-0005", "order-created:1005", {"trace": "trace-1005"}),
        ("msg-0006", "order-created:1006", {"trace": "trace-1006", "region": "local"}),
    ]
    return [
        MessageRecord(
            message_id=message_id,
            queue=QUEUE_NAME,
            body=body,
            produced_at_us=BASE_MS * 1_000 + index * 1_000,
            expires_at_us=(BASE_MS + 60_000) * 1_000 + index * 1_000,
            content_type="text/plain; charset=utf-8",
            state="pending",
            record_version=1,
            last_write_at_us=BASE_MS * 1_000 + index * 1_000,
            delivery_count=0,
            extensions=extensions,
            source_format="sample",
            source_version=1,
        )
        for index, (message_id, body, extensions) in enumerate(values)
    ]


def build_legacy_v1() -> bytes:
    record = MessageRecord(
        message_id="legacy-0001",
        queue=QUEUE_NAME,
        body="legacy-message-without-optional-fields",
        produced_at_us=BASE_MS * 1_000 + 123,
        expires_at_us=(BASE_MS + 60_000) * 1_000 + 123,
        extensions={"ignored_in_v1": True},
        source_format="sample",
        source_version=1,
    )
    return encode_compact([record], [QUEUE], version=1)


def build_optional_v2() -> bytes:
    records = [
        replace(
            message,
            extensions={
                **message.extensions,
                "vendor_trace_id": f"vendor-{index:04d}",
                "future_optional_flag": index % 2 == 0,
            },
            source_format="sample",
            source_version=2,
        )
        for index, message in enumerate(sample_messages()[:3], start=1)
    ]
    return encode_compact(records, [QUEUE], version=2)


def build_oversize_batch() -> bytes:
    messages = []
    for index in range(65):
        messages.append(
            MessageRecord(
                message_id=f"overflow-{index:04d}",
                queue=QUEUE_NAME,
                body=f"overflow-body-{index}",
                produced_at_us=(BASE_MS + index) * 1_000,
                expires_at_us=(BASE_MS + 60_000 + index) * 1_000,
                source_format="sample",
                source_version=1,
            )
        )
    lines = [
        canonical_json(
            {
                "_type": "mq_manifest",
                "format": "jsonl",
                "version": 1,
                "queues": [QUEUE.to_dict()],
                "extensions": {"sample": "oversize-batch"},
            }
        )
    ]
    for message in messages:
        lines.append(
            canonical_json(
                {
                    "id": message.message_id,
                    "queue": message.queue,
                    "body": message.body,
                    "produced_at_ms": message.produced_at_us // 1_000,
                    "ttl_ms": (message.expires_at_us - message.produced_at_us) // 1_000,
                    "content_type": message.content_type,
                    "state": message.state,
                    "record_version": message.record_version,
                    "last_write_at_ms": message.last_write_at_us // 1_000,
                    "delivery_count": 0,
                }
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_oversize_body() -> bytes:
    body = "x" * 4097
    lines = [
        canonical_json(
            {
                "_type": "mq_manifest",
                "format": "jsonl",
                "version": 1,
                "queues": [QUEUE.to_dict()],
                "extensions": {"sample": "oversize-body"},
            }
        ),
        canonical_json(
            {
                "id": "oversize-body-0001",
                "queue": QUEUE_NAME,
                "body": body,
                "produced_at_ms": BASE_MS,
                "ttl_ms": 60_000,
            }
        ),
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_conflict_samples() -> tuple[bytes, bytes]:
    queue = replace(QUEUE, name="conflict-queue", visibility_timeout_ms=1_000)
    seed_message = MessageRecord(
        message_id="conflict-1",
        queue=queue.name,
        body="base-body",
        produced_at_us=BASE_MS * 1_000,
        expires_at_us=(BASE_MS + 60_000) * 1_000,
        state="pending",
        record_version=1,
        last_write_at_us=BASE_MS * 1_000,
        source_format="sample",
        source_version=1,
    )
    second_message = replace(
        seed_message,
        message_id="conflict-2",
        body="second-new-body",
        produced_at_us=BASE_MS * 1_000 + 1_000,
        expires_at_us=(BASE_MS + 60_000) * 1_000 + 1_000,
        last_write_at_us=BASE_MS * 1_000 + 1_000,
    )
    ambiguous = replace(seed_message, body="ambiguous-replacement-body")
    seed = encode_jsonl([seed_message], [queue])
    atomic_reject = encode_jsonl([ambiguous, second_message], [queue])
    return seed, atomic_reject


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic local interoperability samples")
    parser.add_argument("--output", default=str(ROOT / "artifacts" / "samples"))
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    messages = sample_messages()
    conflict_seed, conflict_atomic = build_conflict_samples()
    files: dict[str, bytes] = {
        "messages.json": (
            json.dumps(
                {
                    "queue": QUEUE.to_dict(),
                    "messages": [message.semantic_dict() for message in messages],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8"),
        "current_v2.bin": encode_compact(messages, [QUEUE], version=2),
        "current_v1.jsonl": encode_jsonl(messages, [QUEUE], version=1),
        "legacy_v1.bin": build_legacy_v1(),
        "optional_v2.bin": build_optional_v2(),
        "future_required_v3.bin": build_future_compact_sample([QUEUE], messages[:1], version=3),
        "oversize_batch.jsonl": build_oversize_batch(),
        "oversize_body.jsonl": build_oversize_body(),
        "conflict_seed.jsonl": conflict_seed,
        "conflict_atomic_reject.jsonl": conflict_atomic,
    }
    descriptions = {
        "messages.json": "Fixed records used by the HTTP validation driver; no real account data.",
        "current_v2.bin": "Current compact v2 export with field bitmap and CRC32.",
        "current_v1.jsonl": "JSONL v1 manifest plus records with relative TTL and millisecond timestamps.",
        "legacy_v1.bin": "Compact v1 file missing optional fields and containing 123 sub-millisecond microseconds.",
        "optional_v2.bin": "Compact v2 with unknown optional extension keys that must survive re-export.",
        "future_required_v3.bin": "Unsupported compact v3 with an unknown required-extension header flag.",
        "oversize_batch.jsonl": "65 records; JSONL batch capacity is 64.",
        "oversize_body.jsonl": "One 4097-byte body; JSONL body capacity is 4096 bytes.",
        "conflict_seed.jsonl": "Seed a queue with conflict-1 for atomic import conflict validation.",
        "conflict_atomic_reject.jsonl": "First record is an unresolvable conflict; the second must not commit.",
    }
    for name, content in files.items():
        (output / name).write_bytes(content)
    manifest = {
        "deterministic": True,
        "base_time_ms": BASE_MS,
        "queue": QUEUE.to_dict(),
        "files": [
            {
                "name": name,
                "bytes": len(files[name]),
                "sha256": hashlib.sha256(files[name]).hexdigest(),
                "description": descriptions[name],
            }
            for name in sorted(files)
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checksum_lines = [
        f"{hashlib.sha256(files[name]).hexdigest()}  {name}" for name in sorted(files)
    ]
    (output / "checksums.sha256").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")
    print(f"generated {len(files)} deterministic samples in {output}")


if __name__ == "__main__":
    main()

