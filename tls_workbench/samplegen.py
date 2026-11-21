"""Offline deterministic sample generation.

Generates four fixed certificate chains (RSA-2048, RSA-3072, EC P-256 and an
expired RSA-2048 chain) plus deterministic handshake parameter samples.
Nothing here contacts an external CA or the network.
"""

from __future__ import annotations

import json
import os

from . import certificates as cert_mod
from cryptography.hazmat.primitives import serialization

from .certificates import KeySpec, build_certificate, write_json

# Fixed validity windows in epoch milliseconds. DER only retains whole
# seconds; millisecond precision is exercised by the verification instant.
DAY_MS = 86_400_000
T_2025 = 1735689600000  # 2025-01-01T00:00:00.000Z
T_2026 = 1767225600000  # 2026-01-01T00:00:00.000Z
T_2027 = 1798761600000  # 2027-01-01T00:00:00.000Z
T_2028 = 1830297600000  # 2028-01-01T00:00:00.000Z
# Verification wall used by the deterministic handshake samples.
VERIFY_WALL = T_2026  # exactly the boundary instant of some leaf certs

ORG = "Local Deterministic Test CA"

CHAIN_SPECS = [
    {
        "chain_id": "rsa2048_chain",
        "description": "RSA-2048 root/intermediate/leaf, leaf expires exactly at the verification wall (zero-margin boundary)",
        "key_specs": {
            "root": KeySpec("RSA", "2048"),
            "intermediate": KeySpec("RSA", "2048"),
            "leaf": KeySpec("RSA", "2048"),
        },
        "sig_alg": "SHA256_WITH_RSA",
        "serial_offset": 100,
        "windows": {
            "root": (T_2025, T_2025 + 10 * 365 * DAY_MS),
            "intermediate": (T_2025, T_2028),
            "leaf": (T_2025, T_2026),
        },
    },
    {
        "chain_id": "rsa3072_chain",
        "description": "RSA-3072 chain with SHA-384 signatures, longer leaf validity",
        "key_specs": {
            "root": KeySpec("RSA", "3072"),
            "intermediate": KeySpec("RSA", "3072"),
            "leaf": KeySpec("RSA", "3072"),
        },
        "sig_alg": "SHA384_WITH_RSA",
        "serial_offset": 200,
        "windows": {
            "root": (T_2025, T_2025 + 12 * 365 * DAY_MS),
            "intermediate": (T_2025, T_2028),
            "leaf": (T_2025, T_2027),
        },
    },
    {
        "chain_id": "ec256_chain",
        "description": "EC P-256 chain with ECDSA-SHA256",
        "key_specs": {
            "root": KeySpec("EC", "P-256"),
            "intermediate": KeySpec("EC", "P-256"),
            "leaf": KeySpec("EC", "P-256"),
        },
        "sig_alg": "ECDSA_WITH_SHA256",
        "serial_offset": 300,
        "windows": {
            "root": (T_2025, T_2025 + 10 * 365 * DAY_MS),
            "intermediate": (T_2025, T_2028),
            "leaf": (T_2025, T_2027),
        },
    },
    {
        "chain_id": "expired_chain",
        "description": "RSA-2048 chain whose leaf is already expired at the verification wall",
        "key_specs": {
            "root": KeySpec("RSA", "2048"),
            "intermediate": KeySpec("RSA", "2048"),
            "leaf": KeySpec("RSA", "2048"),
        },
        "sig_alg": "SHA256_WITH_RSA",
        "serial_offset": 400,
        "windows": {
            "root": (T_2025, T_2025 + 10 * 365 * DAY_MS),
            "intermediate": (T_2025, T_2028),
            "leaf": (T_2025, T_2026 - DAY_MS),
        },
    },
]

ROLES = ("root", "intermediate", "leaf")


def generate_all(base_dir: str) -> dict:
    der_root = os.path.join(base_dir, "certs", "der")
    key_dir = os.path.join(base_dir, "certs", "keys")
    samples_dir = os.path.join(base_dir, "samples")
    os.makedirs(der_root, exist_ok=True)
    os.makedirs(key_dir, exist_ok=True)
    os.makedirs(samples_dir, exist_ok=True)
    generated: dict[str, list[str]] = {}
    for spec in CHAIN_SPECS:
        chain_dir = os.path.join(der_root, spec["chain_id"])
        os.makedirs(chain_dir, exist_ok=True)
        keys = {
            role: cert_mod.load_or_create_key(key_spec, key_dir)
            for role, key_spec in spec["key_specs"].items()
        }
        metadata_entries = []
        for index, role in enumerate(ROLES):
            not_before, not_after = spec["windows"][role]
            issuer_role = role if role == "root" else ROLES[index - 1]
            cn = f"{spec['chain_id']}-{role}"
            certificate = build_certificate(
                subject_cn=cn,
                issuer_cn=f"{spec['chain_id']}-{issuer_role}",
                organization=ORG,
                serial=spec["serial_offset"] + index + 1,
                not_before_ms=not_before,
                not_after_ms=not_after,
                subject_key=keys[role],
                issuer_key=keys[issuer_role],
                signature_algorithm=spec["sig_alg"],
                is_ca=(role in {"root", "intermediate"}),
            )
            der_bytes = certificate.public_bytes(serialization.Encoding.DER)
            der_file = f"{role}.der"
            with open(os.path.join(chain_dir, der_file), "wb") as handle:
                handle.write(der_bytes)
            info = cert_mod.parse_certificate_der(der_bytes)
            metadata_entries.append(
                {
                    "role": role,
                    "der_file": der_file,
                    "serial_number": info["serial_number"],
                    "not_before_ms": not_before,
                    "not_after_ms": not_after,
                    "signature_algorithm": spec["sig_alg"],
                }
            )
        write_json(
            os.path.join(chain_dir, "chain_metadata.json"),
            {
                "chain_id": spec["chain_id"],
                "description": spec["description"],
                "organization": ORG,
                "certificates": metadata_entries,
            },
        )
        generated[spec["chain_id"]] = [entry["der_file"] for entry in metadata_entries]
    write_json(
        os.path.join(base_dir, "samples", "handshake_samples.json"),
        _handshake_samples(),
    )
    return generated


def _handshake_samples() -> dict:
    # Mixed large/small fixed-point terms (scale 1e9) to expose naive
    # summation error while the Kahan/fixed canonical total stays exact.
    stage_costs = {
        "CLIENT_HELLO": 1_250_000_000,
        "SERVER_HELLO": 2_500_000_000,
        "CERTIFICATE": 8_750_000_000,
        "CERT_VERIFY": 12_000_000_000,
        "FINISHED": 990_000_000,
    }
    chain_overhead = {
        "CLIENT_HELLO": 1_000,
        "SERVER_HELLO": 2_000,
        "CERTIFICATE": 45_000_000,
        "CERT_VERIFY": 78_000_000,
        "FINISHED": 3_000,
    }
    common = {
        "protocol_version": "TLSv1.3",
        "supported_protocol_versions": ["TLSv1.2", "TLSv1.3"],
        "cipher_suite": "TLS_AES_256_GCM_SHA384",
        "supported_cipher_suites": [
            "TLS_AES_256_GCM_SHA384",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        ],
        "sni": "loopback.diagnostic.local",
        "session_ticket_length_ms": 28800000,
        "verification_wall_ms": VERIFY_WALL,
        "truncation_granularity_ms": 1,
        "stage_costs_ms_fixed": stage_costs,
        "chain_overhead_ms_fixed": chain_overhead,
        "per_stage_budget_limits_ms_fixed": {
            "CLIENT_HELLO": 2_000_000_000,
            "SERVER_HELLO": 3_000_000_000,
            "CERTIFICATE": 10_000_000_000,
            "CERT_VERIFY": 14_000_000_000,
            "FINISHED": 2_000_000_000,
        },
    }
    normal = dict(common)
    normal.update(
        {
            "sample_id": "normal_handshake",
            "chain_id": "rsa2048_chain",
            "clock_offset_ms": 0,
            "total_budget_limit_ms_fixed": 30_000_000_000,
        }
    )
    over_budget = dict(common)
    over_budget.update(
        {
            "sample_id": "over_budget_handshake",
            "chain_id": "rsa2048_chain",
            "clock_offset_ms": 0,
            "total_budget_limit_ms_fixed": 20_000_000_000,
        }
    )
    clock_skewed = dict(common)
    clock_skewed.update(
        {
            "sample_id": "clock_skewed_handshake",
            "chain_id": "expired_chain",
            # +25h offset pushes verification well beyond the leaf expiry.
            "clock_offset_ms": 25 * 3600_000,
            "total_budget_limit_ms_fixed": 30_000_000_000,
        }
    )
    boundary = dict(common)
    boundary.update(
        {
            "sample_id": "zero_margin_boundary",
            "chain_id": "rsa2048_chain",
            "clock_offset_ms": 0,
            "total_budget_limit_ms_fixed": 30_000_000_000,
        }
    )
    downgraded = dict(common)
    downgraded.update(
        {
            "sample_id": "downgraded_handshake",
            "chain_id": "ec256_chain",
            "clock_offset_ms": -500,
            "total_budget_limit_ms_fixed": 30_000_000_000,
            "downgrade_items": [
                "STRENGTH_SCORE_DETAIL",
                "SUMMATION_ORDER_AUDIT",
            ],
        }
    )
    return {
        "generated_for_verify_wall_ms": VERIFY_WALL,
        "samples": [normal, over_budget, clock_skewed, boundary, downgraded],
    }
