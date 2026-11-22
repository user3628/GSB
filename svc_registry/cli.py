from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from .formats import decode_snapshot, encode_snapshot
from .models import RegistrationRecord, Snapshot
from .normalization import normalize_registrations
from .policies import TimePolicy, WeightPolicy


ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def _modern_records() -> list[RegistrationRecord]:
    params_http = {
        "timeout_ms": 800,
        "expected_status": 200,
        "expected_body_contains": "OK",
    }
    params_tcp = {"timeout_ms": 800}
    return [
        RegistrationRecord(
            service_name="orders",
            instance_id="orders-v2-a",
            endpoint="http://127.0.0.1:28765/health/ok",
            weight=875,
            weight_encoding="permille",
            lease_ttl_ms=15_000,
            probe_method="http",
            probe_params=params_http,
            registered_at=1_700_000_100_000,
            time_unit="ms",
            lease_version=7,
            probe_window_ms=600,
            last_heartbeat_at=1_700_000_100_000,
            source_version=2,
        ),
        RegistrationRecord(
            service_name="orders",
            instance_id="orders-v2-b",
            endpoint="tcp://127.0.0.1:28766",
            weight=500,
            weight_encoding="permille",
            lease_ttl_ms=15_000,
            probe_method="tcp",
            probe_params=params_tcp,
            registered_at=1_700_000_200_000,
            time_unit="ms",
            lease_version=4,
            probe_window_ms=900,
            last_heartbeat_at=1_700_000_200_000,
            source_version=2,
        ),
    ]


def generate_samples() -> dict[str, Any]:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    legacy_lines = [
        {
            "format_version": 1,
            "kind": "registration",
            "service_name": "catalog",
            "instance_id": "catalog-1",
            "endpoint": "http://127.0.0.1:28765/health/ok",
            "weight": 0.7334,
            "lease_ttl_ms": 5000,
            "probe_method": "http",
            "registered_at_us": 1_700_000_000_123_456,
        },
        {
            "format_version": 1,
            "kind": "registration",
            "service_name": "catalog",
            "instance_id": "catalog-2",
            "endpoint": "tcp://127.0.0.1:28766",
            "weight": 0.5,
            "lease_ttl_ms": 5000,
            "probe_method": "tcp",
            "registered_at_us": 1_700_000_000_234_567,
        },
        {
            "format_version": 1,
            "kind": "registration",
            "service_name": "ledger",
            "instance_id": "ledger-1",
            "endpoint": "http://127.0.0.1:28765/health/ok",
            "weight": 0.6255,
            "lease_ttl_ms": 30_000,
            "probe_method": "lease",
            "registered_at_us": 1_700_000_000_345_678,
        },
    ]
    modern = _modern_records()
    legacy_path = SAMPLES / "legacy_v1.jsonl"
    modern_path = SAMPLES / "modern_v2.bin"
    endpoints_path = SAMPLES / "endpoints.json"
    legacy_path.write_text(
        "\n".join(json.dumps(line, sort_keys=True, separators=(",", ":")) for line in legacy_lines) + "\n",
        encoding="utf-8",
    )
    modern_snapshot = Snapshot(
        kind="registration",
        version=2,
        records=tuple(modern),
        source_format="sample",
    )
    modern_path.write_bytes(encode_snapshot(modern_snapshot, target_format="binary", version=2))
    endpoints_path.write_text(
        json.dumps(
            {
                "http_base": "http://127.0.0.1:28765",
                "tcp_endpoint": "tcp://127.0.0.1:28766",
                "http_probes": {
                    "ok": "/health/ok",
                    "bad_status": "/health/bad",
                    "wrong_marker": "/health/wrong-marker",
                    "slow": "/health/slow?delay_ms=1500",
                },
                "notes": "All endpoints bind to loopback only and are deterministic.",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "legacy": str(legacy_path),
        "modern": str(modern_path),
        "endpoints": str(endpoints_path),
        "modern_bytes": modern_path.stat().st_size,
    }


def cmd_serve(args: argparse.Namespace) -> int:
    os.environ["REGISTRY_DB_PATH"] = args.db
    os.environ["REGISTRY_MAX_CONCURRENCY"] = str(args.concurrency)
    os.environ["REGISTRY_QUEUE_SIZE"] = str(args.queue_size)
    import uvicorn

    uvicorn.run("svc_registry.api:app", host=args.host, port=args.port, log_level=args.log_level)
    return 0


def cmd_generate_samples(args: argparse.Namespace) -> int:
    print(json.dumps(generate_samples(), indent=2, ensure_ascii=False))
    return 0


def cmd_convert(args: argparse.Namespace) -> int:
    source = Path(args.input)
    if args.input_format == "binary":
        payload: bytes | list[bytes] = source.read_bytes()
    else:
        payload = source.read_bytes()
    snapshot, info, negotiation = decode_snapshot(
        payload,
        expected_format=args.input_format,
        expected_kind=args.kind,
    )
    weight_policy = WeightPolicy.from_dict(
        {"source_scale": "ratio", "rounding_rule": args.weight_rounding}
    ) if args.weight_rounding else WeightPolicy()
    time_policy = TimePolicy(truncation_rule=args.time_truncation, declared=True)
    normalized = normalize_registrations(
        list(snapshot.records),  # type: ignore[arg-type]
        weight_policy=weight_policy,
        time_policy=time_policy,
    )
    v2_records = [
        RegistrationRecord(
            service_name=record.service_name,
            instance_id=record.instance_id,
            endpoint=record.endpoint,
            weight=record.weight_permille,
            weight_encoding="permille",
            lease_ttl_ms=record.lease_ttl_ms,
            probe_method=record.probe_method,
            probe_params=record.probe_params,
            registered_at=record.registered_at_ms,
            time_unit="ms",
            lease_version=record.lease_version,
            probe_window_ms=record.probe_window_ms,
            last_heartbeat_at=record.last_heartbeat_at_ms,
            source_version=2,
        )
        for record in normalized.records
    ]
    out_snapshot = Snapshot(kind="registration", version=2, records=tuple(v2_records))
    encoded = encode_snapshot(
        out_snapshot,
        target_format=args.target_format,
        version=2,
        endianness=args.endianness,
        shards=args.shards,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(encoded, list):
        for index, shard in enumerate(encoded):
            target = output.with_name(f"{output.stem}.{index}{output.suffix}")
            target.write_bytes(shard)
            print(target)
    else:
        output.write_bytes(encoded)
        print(output)
    print(json.dumps({
        "source": info.to_dict(),
        "negotiation": negotiation,
        "degradation_details": normalized.details()["degradation_details"],
        "weight_rounding": normalized.details()["weight_rounding"],
        "time_truncation": normalized.details()["time_truncation"],
    }, indent=2, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="svc-registry")
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="run the registry API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=18080)
    serve.add_argument("--db", default="data/registry.db")
    serve.add_argument("--concurrency", type=int, default=4)
    serve.add_argument("--queue-size", type=int, default=8)
    serve.add_argument("--log-level", default="info")
    serve.set_defaults(func=cmd_serve)
    samples = sub.add_parser("generate-samples", help="write deterministic local samples")
    samples.set_defaults(func=cmd_generate_samples)
    convert = sub.add_parser("convert", help="convert a registration snapshot to v2")
    convert.add_argument("--input", required=True)
    convert.add_argument("--output", required=True)
    convert.add_argument("--input-format", choices=["binary", "jsonl", "auto"], default="auto")
    convert.add_argument("--target-format", choices=["binary", "jsonl"], required=True)
    convert.add_argument("--kind", default="registration")
    convert.add_argument("--weight-rounding", choices=["half_up", "floor", "ceil", "truncate"])
    convert.add_argument("--time-truncation", choices=["floor", "ceil", "half_up"], default="floor")
    convert.add_argument("--endianness", choices=["little", "big"], default="little")
    convert.add_argument("--shards", type=int, default=1)
    convert.set_defaults(func=cmd_convert)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
