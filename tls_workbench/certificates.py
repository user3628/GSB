"""Deterministic local certificate authority and DER parsing.

All material is generated offline from fixed inputs. Private keys are cached
as PKCS#8 DER files on first run so every subsequent regeneration produces
the exact same certificates; no external CA or network access is involved.

DER GeneralizedTime/UTCTime only encodes whole seconds, so certificate
validity boundaries are always aligned to the second. Sub-second precision
still matters for the verification instant: its truncation rule lives in
:mod:`tls_workbench.timeutil`.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.x509.oid import NameOID

from .errors import Codes, WorkbenchError

_SIG_HASHES = {
    "SHA256_WITH_RSA": hashes.SHA256(),
    "SHA384_WITH_RSA": hashes.SHA384(),
    "ECDSA_WITH_SHA256": hashes.SHA256(),
    "ECDSA_WITH_SHA384": hashes.SHA384(),
}

_HASH_NAMES = {
    "sha256": "SHA256",
    "sha384": "SHA384",
}

_EC_CURVE_NAMES = {
    "secp256r1": "P-256",
    "secp384r1": "P-384",
}


def epoch_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _ms_to_dt(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def _name(common_name: str, organization: str) -> x509.Name:
    return x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, organization),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


@dataclass(frozen=True)
class KeySpec:
    kind: str
    param: str

    @property
    def filename(self) -> str:
        return f"{self.kind.lower()}_{self.param.replace('-', '').lower()}.der"


def _generate_key(spec: KeySpec):
    if spec.kind == "RSA":
        return rsa.generate_private_key(
            public_exponent=65537, key_size=int(spec.param)
        )
    if spec.kind == "EC":
        curve = {"P-256": ec.SECP256R1, "P-384": ec.SECP384R1}[spec.param]
        return ec.generate_private_key(curve())
    raise WorkbenchError(Codes.CHAIN_INVALID, f"unsupported key kind {spec.kind}")


def load_or_create_key(spec: KeySpec, key_dir: str):
    path = os.path.join(key_dir, spec.filename)
    if os.path.exists(path):
        with open(path, "rb") as handle:
            return serialization.load_der_private_key(handle.read(), password=None)
    key = _generate_key(spec)
    encoded = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    with open(path, "wb") as handle:
        handle.write(encoded)
    return key


def build_certificate(
    *,
    subject_cn: str,
    issuer_cn: str,
    organization: str,
    serial: int,
    not_before_ms: int,
    not_after_ms: int,
    subject_key,
    issuer_key,
    signature_algorithm: str,
    is_ca: bool,
) -> x509.Certificate:
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(subject_cn, organization))
        .issuer_name(_name(issuer_cn, organization))
        .public_key(subject_key.public_key())
        .serial_number(serial)
        .not_valid_before(_ms_to_dt(not_before_ms))
        .not_valid_after(_ms_to_dt(not_after_ms))
        .add_extension(
            x509.BasicConstraints(ca=is_ca, path_length=None), critical=True
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(subject_key.public_key()),
            critical=False,
        )
    )
    builder = builder.add_extension(
        x509.AuthorityKeyIdentifier.from_issuer_public_key(
            issuer_key.public_key()
        ),
        critical=False,
    )
    return builder.sign(issuer_key, _SIG_HASHES[signature_algorithm])


def _public_key_info(public_key) -> dict:
    if isinstance(public_key, rsa.RSAPublicKey):
        numbers = public_key.public_numbers()
        return {
            "public_key_algorithm": "RSA",
            "rsa_modulus_bits": numbers.n.bit_length(),
            "rsa_public_exponent": numbers.e,
            "ec_curve": None,
        }
    if isinstance(public_key, ec.EllipticCurvePublicKey):
        curve_name = _EC_CURVE_NAMES.get(
            public_key.curve.name, public_key.curve.name
        )
        return {
            "public_key_algorithm": "EC",
            "rsa_modulus_bits": None,
            "rsa_public_exponent": None,
            "ec_curve": curve_name,
        }
    raise WorkbenchError(Codes.CHAIN_INVALID, "unsupported public key algorithm")


def parse_certificate_der(der_bytes: bytes) -> dict:
    """Parse a DER certificate into structured, JSON-serialisable metadata."""
    certificate = x509.load_der_x509_certificate(der_bytes)
    try:
        signature_oid = certificate.signature_algorithm_oid
        hash_algorithm = certificate.signature_hash_algorithm
        hash_name = (
            _HASH_NAMES.get(hash_algorithm.name, hash_algorithm.name.upper())
            if hash_algorithm is not None
            else "UNKNOWN"
        )
        public_key = certificate.public_key()
        info = _public_key_info(public_key)
        oid_name = signature_oid._name
        if oid_name == "sha256WithRSAEncryption":
            algorithm_name = "SHA256_WITH_RSA"
        elif oid_name == "sha384WithRSAEncryption":
            algorithm_name = "SHA384_WITH_RSA"
        elif oid_name == "ecdsa-with-SHA256":
            algorithm_name = "ECDSA_WITH_SHA256"
        elif oid_name == "ecdsa-with-SHA384":
            algorithm_name = "ECDSA_WITH_SHA384"
        else:
            algorithm_name = oid_name
        return {
            "serial_number": str(certificate.serial_number),
            "subject": certificate.subject.rfc4514_string(),
            "issuer": certificate.issuer.rfc4514_string(),
            "not_before_ms": epoch_ms(certificate.not_valid_before_utc),
            "not_after_ms": epoch_ms(certificate.not_valid_after_utc),
            "signature_algorithm": algorithm_name,
            "signature_oid_name": oid_name,
            "signature_hash": hash_name,
            "der_sha256": hashlib.sha256(der_bytes).hexdigest(),
            **info,
        }
    except WorkbenchError:
        raise
    except Exception as exc:  # pragma: no cover - defensive parsing guard
        raise WorkbenchError(
            Codes.CHAIN_INVALID, f"certificate metadata extraction failed: {exc}"
        ) from exc


def verify_signature(child: x509.Certificate, issuer_public_key) -> None:
    if isinstance(issuer_public_key, rsa.RSAPublicKey):
        issuer_public_key.verify(
            child.signature,
            child.tbs_certificate_bytes,
            padding.PKCS1v15(),
            child.signature_hash_algorithm,
        )
    elif isinstance(issuer_public_key, ec.EllipticCurvePublicKey):
        issuer_public_key.verify(
            child.signature,
            child.tbs_certificate_bytes,
            ec.ECDSA(child.signature_hash_algorithm),
        )
    else:
        raise WorkbenchError(Codes.SIGNATURE_INVALID, "unsupported issuer key type")


def load_chain(chain_dir: str) -> dict:
    """Load and parse a generated chain directory.

    The previously loaded snapshot is returned untouched on failure by the
    :class:`~tls_workbench.store.SampleStore` wrapper; this function raises
    :class:`WorkbenchError` with ``E_CHAIN_INVALID`` on any parse problem.
    """
    metadata_path = os.path.join(chain_dir, "chain_metadata.json")
    if not os.path.exists(metadata_path):
        raise WorkbenchError(
            Codes.CHAIN_INVALID, f"missing chain metadata in {chain_dir}"
        )
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
    certificates: dict[str, dict] = {}
    parsed: dict[str, x509.Certificate] = {}
    for entry in metadata["certificates"]:
        der_path = os.path.join(chain_dir, entry["der_file"])
        with open(der_path, "rb") as handle:
            der_bytes = handle.read()
        try:
            parsed[entry["role"]] = x509.load_der_x509_certificate(der_bytes)
        except Exception as exc:
            raise WorkbenchError(
                Codes.CHAIN_INVALID,
                f"DER parse failed for {entry['der_file']}: {exc}",
            ) from exc
        certificates[entry["role"]] = parse_certificate_der(der_bytes)
    if "root" not in parsed:
        raise WorkbenchError(
            Codes.CHAIN_INVALID, "chain is missing the root certificate"
        )
    return {
        "chain_id": metadata["chain_id"],
        "description": metadata.get("description", ""),
        "certificates": certificates,
        "_objects": parsed,
    }


def write_json(path: str, payload) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
