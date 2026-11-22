from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .errors import (
    E_VALIDATION,
    E_WEIGHT_INVALID,
    E_WEIGHT_POLICY_REQUIRED,
    BusinessError,
)
from .models import (
    NormalizationStep,
    NormalizedProbeResult,
    NormalizedRegistration,
    ProbeResultRecord,
    RegistrationRecord,
)
from .policies import TimePolicy, WeightPolicy, convert_weight_to_permille, normalize_timestamp_to_ms
from .validation import (
    default_probe_params,
    validate_endpoint,
    validate_probe_method,
    validate_probe_params,
)


@dataclass
class NormalizationBatch:
    records: list[NormalizedRegistration | NormalizedProbeResult] = field(default_factory=list)
    steps: list[NormalizationStep] = field(default_factory=list)
    degradations: list[dict[str, Any]] = field(default_factory=list)

    def details(self) -> dict[str, Any]:
        return {
            "records": [record.to_dict() for record in self.records],
            "normalization": [step.to_dict() for step in self.steps],
            "degradation_details": self.degradations,
            "weight_rounding": [
                {
                    "record_index": step.record_index,
                    "service_name": step.service_name,
                    "instance_id": step.instance_id,
                    "source_weight": step.source_weight,
                    "source_encoding": step.source_weight_encoding,
                    "weight_permille": step.weight_permille,
                    "rounding_rule": step.weight_rounding_rule,
                    "rounding_residue": step.weight_rounding_residue,
                }
                for step in self.steps
                if step.source_weight_encoding == "ratio"
            ],
            "time_truncation": [
                {
                    "record_index": step.record_index,
                    "service_name": step.service_name,
                    "instance_id": step.instance_id,
                    "source_unit": step.time_unit,
                    "truncation_rule": step.truncation_rule,
                    "residue_us": step.time_residue_us,
                }
                for step in self.steps
                if step.time_unit == "us"
            ],
        }


def _check_mixed_weight_encodings(records: Iterable[Any], policy: WeightPolicy) -> None:
    encodings = {record.weight_encoding for record in records}
    if len(encodings) > 1 and (not policy.declared or policy.source_scale != "mixed"):
        raise BusinessError(
            E_WEIGHT_POLICY_REQUIRED,
            "mixed float-ratio and integer-permille weights require source_scale=mixed",
            details={"encodings": sorted(encodings), "declared_source_scale": policy.source_scale},
            http_status=422,
        )
    if "ratio" in encodings and (not policy.declared or policy.source_scale not in ("ratio", "mixed")):
        first = next(record for record in records if record.weight_encoding == "ratio")
        raise BusinessError(
            E_WEIGHT_POLICY_REQUIRED,
            "ratio weights require a declared ratio-to-permille policy",
            details={
                "service_name": first.service_name,
                "instance_id": first.instance_id,
                "required_policy": {"source_scale": "ratio", "rounding_rule": "half_up"},
            },
            http_status=422,
        )


def normalize_registrations(
    records: list[RegistrationRecord],
    *,
    weight_policy: WeightPolicy | None = None,
    time_policy: TimePolicy | None = None,
) -> NormalizationBatch:
    weight_policy = weight_policy or WeightPolicy()
    time_policy = time_policy or TimePolicy()
    _check_mixed_weight_encodings(records, weight_policy)
    batch = NormalizationBatch()
    for index, record in enumerate(records):
        method = record.probe_method or "http"
        method_was_defaulted = not record.probe_method
        method = validate_probe_method(method, record_index=index)
        params = record.probe_params
        params_defaulted = params is None
        if params is None:
            params = default_probe_params(method)
        params = validate_probe_params(method, params, record_index=index)
        validate_endpoint(record.endpoint, method, record_index=index)
        if record.probe_window_ms is not None and (
            isinstance(record.probe_window_ms, bool)
            or not isinstance(record.probe_window_ms, int)
            or record.probe_window_ms <= 0
        ):
            raise BusinessError(
                E_VALIDATION,
                "probe_window_ms must be a positive integer",
                details={"record_index": index, "probe_window_ms": record.probe_window_ms},
            )
        if record.lease_ttl_ms <= 0:
            raise BusinessError(
                E_VALIDATION,
                "lease_ttl_ms must be positive",
                details={"record_index": index, "lease_ttl_ms": record.lease_ttl_ms},
            )
        registered_at_ms, residue = normalize_timestamp_to_ms(
            record.registered_at,
            record.time_unit,
            time_policy,
            field_name="registered_at",
            record_index=index,
        )
        heartbeat_ms = None
        if record.last_heartbeat_at is not None:
            heartbeat_ms, heartbeat_residue = normalize_timestamp_to_ms(
                record.last_heartbeat_at,
                record.time_unit,
                time_policy,
                field_name="last_heartbeat_at",
                record_index=index,
            )
            residue = max(residue, heartbeat_residue)
        weight, weight_residue = convert_weight_to_permille(
            record.weight,
            record.weight_encoding,
            weight_policy,
            record_index=index,
        )
        lease_version = record.lease_version if record.lease_version is not None else 1
        if isinstance(lease_version, bool) or not isinstance(lease_version, int) or lease_version < 1:
            raise BusinessError(
                E_VALIDATION,
                "lease_version must be a positive integer",
                details={"record_index": index, "lease_version": lease_version},
            )
        degradations: list[dict[str, Any]] = []
        if method_was_defaulted:
            degradations.append(
                {
                    "record_index": index,
                    "service_name": record.service_name,
                    "instance_id": record.instance_id,
                    "field": "probe_method",
                    "action": "defaulted",
                    "default": method,
                }
            )
        if params_defaulted:
            degradations.append(
                {
                    "record_index": index,
                    "service_name": record.service_name,
                    "instance_id": record.instance_id,
                    "field": "probe_params",
                    "action": "defaulted",
                    "default": params,
                }
            )
        if record.source_version < 2:
            degradations.append(
                {
                    "record_index": index,
                    "service_name": record.service_name,
                    "instance_id": record.instance_id,
                    "field": "probe_window_ms",
                    "action": "defaulted",
                    "default": None,
                    "reason": "legacy version has no probe window",
                }
            )
        normalized = NormalizedRegistration(
            service_name=record.service_name,
            instance_id=record.instance_id,
            endpoint=record.endpoint,
            weight_permille=weight,
            lease_ttl_ms=record.lease_ttl_ms,
            probe_method=method,
            probe_params=params,
            registered_at_ms=registered_at_ms,
            lease_version=lease_version,
            probe_window_ms=record.probe_window_ms,
            last_heartbeat_at_ms=heartbeat_ms,
        )
        step = NormalizationStep(
            record_index=index,
            service_name=record.service_name,
            instance_id=record.instance_id,
            source_weight=record.weight,
            source_weight_encoding=record.weight_encoding,
            weight_permille=weight,
            weight_rounding_rule=weight_policy.rounding_rule,
            weight_rounding_residue=weight_residue,
            time_unit=record.time_unit,
            truncation_rule=time_policy.truncation_rule,
            time_residue_us=residue,
            degradations=degradations,
        )
        batch.records.append(normalized)
        batch.steps.append(step)
        batch.degradations.extend(degradations)
    return batch


def normalize_probe_results(
    records: list[ProbeResultRecord],
    *,
    weight_policy: WeightPolicy | None = None,
    time_policy: TimePolicy | None = None,
) -> NormalizationBatch:
    weight_policy = weight_policy or WeightPolicy()
    time_policy = time_policy or TimePolicy()
    _check_mixed_weight_encodings(records, weight_policy)
    batch = NormalizationBatch()
    for index, record in enumerate(records):
        method = validate_probe_method(record.probe_method, record_index=index)
        params = record.probe_params
        if params is None:
            params = default_probe_params(method)
            batch.degradations.append(
                {
                    "record_index": index,
                    "service_name": record.service_name,
                    "instance_id": record.instance_id,
                    "field": "probe_params",
                    "action": "defaulted",
                    "default": params,
                }
            )
        params = validate_probe_params(method, params, record_index=index)
        validate_endpoint(record.endpoint, method, record_index=index)
        if record.probe_window_ms is not None and (
            isinstance(record.probe_window_ms, bool)
            or not isinstance(record.probe_window_ms, int)
            or record.probe_window_ms <= 0
        ):
            raise BusinessError(
                E_VALIDATION,
                "probe_window_ms must be a positive integer",
                details={"record_index": index, "probe_window_ms": record.probe_window_ms},
            )
        checked_at_ms, residue = normalize_timestamp_to_ms(
            record.checked_at,
            record.time_unit,
            time_policy,
            field_name="checked_at",
            record_index=index,
        )
        weight, weight_residue = convert_weight_to_permille(
            record.weight,
            record.weight_encoding,
            weight_policy,
            record_index=index,
        )
        if not isinstance(record.success, bool):
            raise BusinessError(
                "E_VALIDATION",
                "probe result success must be boolean",
                details={"record_index": index, "success": record.success},
            )
        normalized = NormalizedProbeResult(
            service_name=record.service_name,
            instance_id=record.instance_id,
            endpoint=record.endpoint,
            weight_permille=weight,
            probe_method=method,
            probe_params=params,
            checked_at_ms=checked_at_ms,
            success=record.success,
            status=record.status,
            latency_ms=record.latency_ms,
            outcome=record.outcome,
            lease_version=record.lease_version,
            probe_window_ms=record.probe_window_ms,
            error_code=record.error_code,
            detail=record.detail,
        )
        step = NormalizationStep(
            record_index=index,
            service_name=record.service_name,
            instance_id=record.instance_id,
            source_weight=record.weight,
            source_weight_encoding=record.weight_encoding,
            weight_permille=weight,
            weight_rounding_rule=weight_policy.rounding_rule,
            weight_rounding_residue=weight_residue,
            time_unit=record.time_unit,
            truncation_rule=time_policy.truncation_rule,
            time_residue_us=residue,
        )
        batch.records.append(normalized)
        batch.steps.append(step)
        batch.degradations.extend(step.degradations)
    return batch
