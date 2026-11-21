from __future__ import annotations

import ipaddress
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SNAPSHOT_SCHEMA = "vgw.snapshot/v1"
PATCH_SCHEMA = "vgw.patch/v1"


class TunnelBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    priority: int = Field(ge=0, le=1000)
    channel: int = Field(ge=0, le=65535)
    egress: str = Field(min_length=1, max_length=64)
    max_payload: int = Field(default=8192, ge=1, le=60000)


class SnapshotDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_id: Literal[SNAPSHOT_SCHEMA] = Field(alias="schema")
    version: int = Field(ge=1)
    generated_at: str = Field(min_length=1, max_length=64)
    channel_count: int = Field(ge=1, le=64)
    round_budget_ms: int = Field(default=25, ge=1, le=1000)
    queue_capacity: int = Field(default=8, ge=1, le=10000)
    default_timeout_ms: int = Field(default=1500, ge=10, le=60000)
    egress_endpoints: dict[str, str]
    bindings: dict[str, TunnelBinding]
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("egress_endpoints")
    @classmethod
    def validate_endpoints(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("egress_endpoints must not be empty")
        for name, address in value.items():
            validate_loopback_address(address, f"egress_endpoints.{name}")
        return value

    @field_validator("bindings")
    @classmethod
    def validate_bindings(cls, value: dict[str, TunnelBinding]) -> dict[str, TunnelBinding]:
        if not value:
            raise ValueError("bindings must not be empty")
        return value

    @model_validator(mode="after")
    def validate_binding_references(self) -> "SnapshotDocument":
        for session, binding in self.bindings.items():
            if binding.channel >= self.channel_count:
                raise ValueError(
                    f"binding {session} references channel {binding.channel}, "
                    f"channel_count={self.channel_count}"
                )
            if binding.egress not in self.egress_endpoints:
                raise ValueError(
                    f"binding {session} references unknown egress {binding.egress!r}"
                )
        return self


class PatchDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_id: Literal[PATCH_SCHEMA] = Field(alias="schema")
    base_version: int = Field(ge=1)
    version: int = Field(ge=2)
    generated_at: str = Field(min_length=1, max_length=64)
    bindings: dict[str, TunnelBinding] = Field(default_factory=dict)
    egress_endpoints: dict[str, str] = Field(default_factory=dict)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @field_validator("egress_endpoints")
    @classmethod
    def validate_endpoints(cls, value: dict[str, str]) -> dict[str, str]:
        for name, address in value.items():
            validate_loopback_address(address, f"egress_endpoints.{name}")
        return value

    @model_validator(mode="after")
    def validate_change_scope(self) -> "PatchDocument":
        if not self.bindings and not self.egress_endpoints:
            raise ValueError("patch must contain at least one binding or endpoint change")
        if self.version != self.base_version + 1:
            raise ValueError("patch version must equal base_version + 1")
        return self


class FrameSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    frame_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=64)
    sequence: Any
    payload: Any = None
    payload_b64: str | None = None
    timeout_ms: int | None = Field(default=None, ge=10, le=60000)

    @model_validator(mode="after")
    def validate_payload_source(self) -> "FrameSubmission":
        if self.payload is None and self.payload_b64 is None:
            raise ValueError("one of payload or payload_b64 is required")
        if self.payload is not None and self.payload_b64 is not None:
            raise ValueError("payload and payload_b64 are mutually exclusive")
        return self


class BatchSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    frames: list[FrameSubmission] = Field(min_length=1, max_length=256)
    max_parallel: int = Field(default=32, ge=1, le=256)


class FrameDecodeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    frame_b64: str = Field(min_length=1)
    max_payload: int = Field(default=60000, ge=1, le=60000)


def validate_loopback_address(address: str, field_name: str = "address") -> tuple[str, int]:
    if not isinstance(address, str) or ":" not in address:
        raise ValueError(f"{field_name} must be host:port")
    host, port_text = address.rsplit(":", 1)
    try:
        port = int(port_text)
    except ValueError as exc:
        raise ValueError(f"{field_name} has a non-numeric port") from exc
    if not (1 <= port <= 65535):
        raise ValueError(f"{field_name} port must be in 1..65535")
    host = host.strip("[]")
    if host == "localhost":
        return host, port
    try:
        ip = ipaddress.ip_address(host)
    except ValueError as exc:
        raise ValueError(f"{field_name} must use a loopback IP or localhost") from exc
    if not ip.is_loopback:
        raise ValueError(f"{field_name} must be loopback-only, got {host}")
    return host, port


def parse_endpoint(address: str) -> tuple[str, int]:
    return validate_loopback_address(address)


def model_dump_canonical(model: BaseModel, *, exclude_digest: bool = False) -> dict[str, Any]:
    data = model.model_dump(by_alias=True, mode="json", exclude_none=True)
    if exclude_digest:
        data.pop("digest", None)
    return data

