"""Version 2/version 1 manifest interoperability.

Version 2 stores a floating point temperature and an explicit top-p value.
Version 1 stores an integer temperature percentage and has no top-p field.
Lossless v2 -> v1 -> v2 round-trips carry a clearly marked downgrade ledger
and preserved sampling extension; plain legacy manifests infer top-p from
configuration.
"""

from __future__ import annotations

import json
import math
import re
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Mapping

from .canonical import digest_json
from .config import EngineConfig
from .errors import invalid_manifest, invalid_param
from .models import Manifest, SamplingRequest


_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_MAX_SEED = 2**63 - 1


def stable_request_id(payload: Mapping[str, Any]) -> str:
    """Derive a deterministic identifier for tooling that needs to create one.

    Import deliberately rejects manifests without ``request_id``. This helper is
    exposed for callers that construct new manifests and want a stable ID.
    """

    material = {key: value for key, value in payload.items() if key != "request_id"}
    return "req_" + digest_json(material)[:24]


def _plain_number(value: Any, field: str, request_id: str | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise invalid_manifest(
            f"{field} must be a JSON number",
            field=field,
            request_id=request_id,
            value=value,
        )
    result = float(value)
    if not math.isfinite(result):
        raise invalid_manifest(
            f"{field} must be finite",
            field=field,
            request_id=request_id,
            value=value,
        )
    return result


def _plain_int(value: Any, field: str, request_id: str | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise invalid_manifest(
            f"{field} must be a JSON integer",
            field=field,
            request_id=request_id,
            value=value,
        )
    return int(value)


def _require(
    payload: Mapping[str, Any], field: str, request_id: str | None = None
) -> Any:
    if field not in payload:
        raise invalid_manifest(
            f"missing required field {field}", field=field, request_id=request_id
        )
    return payload[field]


def _temperature_percent(temperature: float, scale: float) -> int:
    value = int(
        (Decimal(str(temperature)) * Decimal(str(scale))).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )
    return value


class ManifestCodec:
    def __init__(self, config: EngineConfig | None = None) -> None:
        self.config = config or EngineConfig()

    def decode_path(self, path: str | Path) -> Manifest:
        source = Path(path)
        try:
            raw = source.read_bytes()
        except FileNotFoundError:
            raise invalid_manifest("manifest file not found", path=str(source))
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise invalid_manifest(
                "manifest is not valid UTF-8 JSON", path=str(source), detail=str(exc)
            )
        return self.decode_document(
            payload,
            base_dir=str(source.parent.resolve()),
            source_path=str(source.resolve()),
            source_digest=digest_json(payload),
        )

    def decode_document(
        self,
        payload: Mapping[str, Any],
        base_dir: str | Path = ".",
        source_path: str | None = None,
        source_digest: str | None = None,
    ) -> Manifest:
        if not isinstance(payload, Mapping):
            raise invalid_manifest("manifest root must be a JSON object")
        version_raw = payload.get("manifest_version", payload.get("version"))
        if version_raw is None:
            raise invalid_manifest(
                "missing manifest_version", field="manifest_version"
            )
        if isinstance(version_raw, bool) or version_raw not in (1, 2):
            raise invalid_manifest(
                "manifest_version must be 1 or 2",
                field="manifest_version",
                value=version_raw,
            )
        version = int(version_raw)
        vocab = self._decode_vocab(payload)
        requests_raw = _require(payload, "requests")
        if not isinstance(requests_raw, list) or not requests_raw:
            raise invalid_manifest("requests must be a non-empty list", field="requests")
        requests: list[SamplingRequest] = []
        conversions: dict[str, dict[str, Any]] = {}
        warnings: list[str] = []
        seen: set[str] = set()
        for index, item in enumerate(requests_raw):
            if not isinstance(item, Mapping):
                raise invalid_manifest(
                    "request entries must be objects", field="requests", index=index
                )
            if version == 2:
                request, conversion = self._decode_v2_request(
                    item, vocab, index, str(Path(base_dir).resolve())
                )
            else:
                request, conversion = self._decode_v1_request(
                    item, vocab, index, str(Path(base_dir).resolve())
                )
            if request.request_id in seen:
                raise invalid_manifest(
                    "duplicate request_id",
                    request_id=request.request_id,
                    index=index,
                )
            seen.add(request.request_id)
            requests.append(request)
            conversions[request.request_id] = conversion
            if conversion.get("warnings"):
                warnings.extend(str(item) for item in conversion["warnings"])
        metadata = payload.get("metadata", {})
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, Mapping):
            raise invalid_manifest("metadata must be an object", field="metadata")
        return Manifest(
            version=version,
            requests=tuple(requests),
            vocab=vocab,
            metadata=dict(metadata),
            conversions=conversions,
            warnings=tuple(warnings),
            source_path=source_path,
            manifest_digest=source_digest or digest_json(payload),
        )

    def _decode_vocab(self, payload: Mapping[str, Any]) -> tuple[str, ...]:
        raw = _require(payload, "vocab")
        if not isinstance(raw, list) or not raw:
            raise invalid_manifest("vocab must be a non-empty list", field="vocab")
        vocab: list[str] = []
        for index, token in enumerate(raw):
            if not isinstance(token, str) or token == "":
                raise invalid_manifest(
                    "vocabulary entries must be non-empty strings",
                    field="vocab",
                    index=index,
                )
            vocab.append(token)
        if len(set(vocab)) != len(vocab):
            raise invalid_manifest("vocabulary entries must be unique", field="vocab")
        return tuple(vocab)

    def _decode_common_v2(
        self, item: Mapping[str, Any], index: int
    ) -> dict[str, Any]:
        request_id = item.get("request_id", item.get("id"))
        if request_id is None:
            raise invalid_manifest(
                "missing required field request_id",
                field="request_id",
                index=index,
            )
        if not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
            raise invalid_manifest(
                "request_id is missing or invalid",
                field="request_id",
                index=index,
                value=request_id,
            )
        logits_path = item.get("logits_path", item.get("logits"))
        if logits_path is None:
            raise invalid_manifest(
                "missing required field logits_path",
                field="logits_path",
                request_id=request_id,
            )
        if not isinstance(logits_path, str) or not logits_path:
            raise invalid_manifest(
                "logits_path must be a non-empty string",
                field="logits_path",
                request_id=request_id,
            )
        top_k_raw = _require(item, "top_k", request_id)
        top_k: int | None
        if top_k_raw is None:
            top_k = None
        else:
            top_k = _plain_int(top_k_raw, "top_k", request_id)
        seed = _plain_int(_require(item, "seed", request_id), "seed", request_id)
        steps = _plain_int(_require(item, "steps", request_id), "steps", request_id)
        repetition_penalty = _plain_number(
            _require(item, "repetition_penalty", request_id),
            "repetition_penalty",
            request_id,
        )
        return {
            "request_id": request_id,
            "logits_path": logits_path,
            "top_k": top_k,
            "seed": seed,
            "steps": steps,
            "repetition_penalty": repetition_penalty,
        }

    def _validate_common_values(self, values: dict[str, Any]) -> None:
        request_id = values["request_id"]
        if values["top_k"] is not None and values["top_k"] <= 0:
            raise invalid_param(
                "top_k must be positive or null",
                request_id=request_id,
                parameter="top_k",
                value=values["top_k"],
            )
        if not (0.0 < values["repetition_penalty"] < float("inf")):
            raise invalid_param(
                "repetition_penalty must be positive and finite",
                request_id=request_id,
                parameter="repetition_penalty",
                value=values["repetition_penalty"],
            )
        if not (0 <= values["seed"] <= _MAX_SEED):
            raise invalid_param(
                "seed must be in [0, 2^63-1]",
                request_id=request_id,
                parameter="seed",
                value=values["seed"],
            )
        if values["steps"] <= 0:
            raise invalid_param(
                "steps must be positive",
                request_id=request_id,
                parameter="steps",
                value=values["steps"],
            )

    def _decode_prompt_tokens(
        self, item: Mapping[str, Any], request_id: str, vocab_size: int
    ) -> tuple[int, ...]:
        raw = item.get("prompt_tokens", [])
        if not isinstance(raw, list):
            raise invalid_manifest(
                "prompt_tokens must be a list",
                request_id=request_id,
                field="prompt_tokens",
            )
        tokens: list[int] = []
        for index, token in enumerate(raw):
            value = _plain_int(token, "prompt_tokens", request_id)
            if not 0 <= value < vocab_size:
                raise invalid_manifest(
                    "prompt token is outside the vocabulary",
                    request_id=request_id,
                    field="prompt_tokens",
                    index=index,
                    value=value,
                )
            tokens.append(value)
        return tuple(tokens)

    def _decode_v2_request(
        self,
        item: Mapping[str, Any],
        vocab: tuple[str, ...],
        index: int,
        base_dir: str,
    ) -> tuple[SamplingRequest, dict[str, Any]]:
        values = self._decode_common_v2(item, index)
        request_id = values["request_id"]
        temperature = _plain_number(
            _require(item, "temperature", request_id), "temperature", request_id
        )
        top_p = _plain_number(
            _require(item, "top_p", request_id), "top_p", request_id
        )
        self._validate_common_values(values)
        self._validate_temperature(temperature, request_id, "temperature")
        self._validate_top_p(top_p, request_id)
        prompt_tokens = self._decode_prompt_tokens(item, request_id, len(vocab))
        request = SamplingRequest(
            request_id=request_id,
            logits_path=values["logits_path"],
            temperature=temperature,
            top_k=values["top_k"],
            top_p=top_p,
            repetition_penalty=values["repetition_penalty"],
            seed=values["seed"],
            steps=values["steps"],
            prompt_tokens=prompt_tokens,
            manifest_version=2,
            vocab=vocab,
            base_dir=base_dir,
        )
        conversion = {
            "request_id": request_id,
            "source_version": 2,
            "temperature": {
                "mode": "direct_float",
                "value": temperature,
                "detail": "version 2 temperature is used directly",
            },
            "top_p": {
                "mode": "direct",
                "value": top_p,
                "detail": "version 2 carries an explicit top-p field",
            },
            "preserved_extension": False,
            "warnings": [],
        }
        return request, conversion

    def _decode_v1_request(
        self,
        item: Mapping[str, Any],
        vocab: tuple[str, ...],
        index: int,
        base_dir: str,
    ) -> tuple[SamplingRequest, dict[str, Any]]:
        values = self._decode_common_v2(item, index)
        request_id = values["request_id"]
        if "top_p" in item:
            raise invalid_manifest(
                "version 1 request must not contain a top_p field",
                request_id=request_id,
                field="top_p",
            )
        if "temp_percent" in item:
            temp_percent = _plain_int(
                item["temp_percent"], "temp_percent", request_id
            )
        elif "temperature_percent" in item:
            temp_percent = _plain_int(
                item["temperature_percent"], "temperature_percent", request_id
            )
        else:
            raise invalid_manifest(
                "missing required field temp_percent",
                field="temp_percent",
                request_id=request_id,
            )
        self._validate_common_values(values)
        if temp_percent <= 0:
            raise invalid_param(
                "temp_percent must be positive",
                request_id=request_id,
                parameter="temp_percent",
                value=temp_percent,
            )
        temperature = float(temp_percent) / self.config.v1_temperature_scale
        self._validate_temperature(temperature, request_id, "temp_percent")
        prompt_tokens = self._decode_prompt_tokens(item, request_id, len(vocab))

        preserved = item.get("x_preserved_sampling")
        downgrade = item.get("x_downgrade")
        warnings: list[str] = []
        if preserved is not None:
            if not isinstance(preserved, Mapping):
                raise invalid_manifest(
                    "x_preserved_sampling must be an object",
                    request_id=request_id,
                    field="x_preserved_sampling",
                )
            preserved_temperature = _plain_number(
                _require(preserved, "temperature", request_id),
                "x_preserved_sampling.temperature",
                request_id,
            )
            preserved_top_p = _plain_number(
                _require(preserved, "top_p", request_id),
                "x_preserved_sampling.top_p",
                request_id,
            )
            self._validate_temperature(
                preserved_temperature, request_id, "x_preserved_sampling.temperature"
            )
            self._validate_top_p(preserved_top_p, request_id)
            quantized = _temperature_percent(
                preserved_temperature, self.config.v1_temperature_scale
            )
            if quantized != temp_percent:
                raise invalid_manifest(
                    "preserved temperature does not match temp_percent",
                    request_id=request_id,
                    field="x_preserved_sampling.temperature",
                    temp_percent=temp_percent,
                    expected_percent=quantized,
                )
            temperature = preserved_temperature
            top_p = preserved_top_p
            top_p_mode = "preserved_extension"
            detail = "top-p and exact temperature restored from explicit downgrade extension"
            warnings.append(
                f"{request_id}: restored exact temperature/top-p from x_preserved_sampling"
            )
        else:
            top_p = self.config.v1_default_top_p
            top_p_mode = "v1_default_inference"
            detail = "legacy version 1 has no top-p; configured default was applied"
            warnings.append(
                f"{request_id}: legacy manifest has no top-p; default {top_p} applied"
            )
        if downgrade is not None and not isinstance(downgrade, Mapping):
            raise invalid_manifest(
                "x_downgrade must be an object",
                request_id=request_id,
                field="x_downgrade",
            )

        request = SamplingRequest(
            request_id=request_id,
            logits_path=values["logits_path"],
            temperature=temperature,
            top_k=values["top_k"],
            top_p=top_p,
            repetition_penalty=values["repetition_penalty"],
            seed=values["seed"],
            steps=values["steps"],
            prompt_tokens=prompt_tokens,
            manifest_version=1,
            vocab=vocab,
            base_dir=base_dir,
        )
        conversion = {
            "request_id": request_id,
            "source_version": 1,
            "temperature": {
                "mode": "percent_scaled",
                "temp_percent": temp_percent,
                "scale": self.config.v1_temperature_scale,
                "value": temperature,
                "exact": preserved is not None,
                "detail": detail
                if preserved is not None
                else "temperature = temp_percent / configured scale",
            },
            "top_p": {
                "mode": top_p_mode,
                "value": top_p,
                "detail": detail,
            },
            "reused_downgrade_extension": downgrade is not None,
            "preserved_extension": preserved is not None,
            "warnings": warnings,
        }
        return request, conversion

    def _validate_temperature(
        self, temperature: float, request_id: str, parameter: str
    ) -> None:
        if temperature <= 0.0:
            raise invalid_param(
                "temperature must be greater than zero",
                request_id=request_id,
                parameter=parameter,
                value=temperature,
            )
        if temperature > self.config.max_temperature:
            raise invalid_param(
                "temperature exceeds configured maximum",
                request_id=request_id,
                parameter=parameter,
                value=temperature,
                maximum=self.config.max_temperature,
            )

    def _validate_top_p(self, top_p: float, request_id: str) -> None:
        if not (0.0 < top_p <= 1.0):
            raise invalid_param(
                "top_p must be in (0, 1]",
                request_id=request_id,
                parameter="top_p",
                value=top_p,
            )

    def export_document(self, manifest: Manifest, target_version: int) -> dict[str, Any]:
        if target_version not in (1, 2):
            raise invalid_manifest(
                "target_version must be 1 or 2", field="target_version"
            )
        exported: list[dict[str, Any]] = []
        downgrades: list[dict[str, Any]] = []
        for request in manifest.requests:
            if target_version == 2:
                exported.append(self._export_v2_request(request))
            else:
                item, downgrade = self._export_v1_request(request)
                exported.append(item)
                downgrades.append(downgrade)
        metadata = dict(manifest.metadata)
        metadata["exported_from_version"] = manifest.version
        metadata["target_version"] = target_version
        if target_version == 1:
            metadata["downgrades"] = downgrades
            metadata["interop_note"] = (
                "Legacy consumers ignore x_downgrade and x_preserved_sampling. "
                "The extension exists only for explicit, lossless round-trips."
            )
        return {
            "manifest_version": target_version,
            "vocab": list(manifest.vocab),
            "metadata": metadata,
            "requests": exported,
        }

    def export_json(
        self, manifest: Manifest, target_version: int, indent: int = 2
    ) -> str:
        payload = self.export_document(manifest, target_version)
        return json.dumps(payload, ensure_ascii=False, indent=indent) + "\n"

    def _export_v2_request(self, request: SamplingRequest) -> dict[str, Any]:
        return {
            "request_id": request.request_id,
            "logits_path": request.logits_path,
            "temperature": request.temperature,
            "top_k": request.top_k,
            "top_p": request.top_p,
            "repetition_penalty": request.repetition_penalty,
            "seed": request.seed,
            "steps": request.steps,
            "prompt_tokens": list(request.prompt_tokens),
        }

    def _export_v1_request(
        self, request: SamplingRequest
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        percent = _temperature_percent(
            request.temperature, self.config.v1_temperature_scale
        )
        if percent <= 0:
            raise invalid_param(
                "temperature is too small to represent in version 1 percent",
                request_id=request.request_id,
                parameter="temperature",
                value=request.temperature,
                scale=self.config.v1_temperature_scale,
            )
        quantized_temperature = percent / self.config.v1_temperature_scale
        downgrade = {
            "request_id": request.request_id,
            "omitted_fields": ["top_p"],
            "temperature_quantization": {
                "source": request.temperature,
                "scale": self.config.v1_temperature_scale,
                "stored_percent": percent,
                "quantized": quantized_temperature,
                "exact": quantized_temperature == request.temperature,
            },
            "preservation_extension": "x_preserved_sampling",
            "note": (
                "Version 1 cannot represent top-p or a non-percent temperature. "
                "The exact values are retained in a marked extension for round-trips."
            ),
        }
        item = {
            "request_id": request.request_id,
            "logits_path": request.logits_path,
            "temp_percent": percent,
            "top_k": request.top_k,
            "repetition_penalty": request.repetition_penalty,
            "seed": request.seed,
            "steps": request.steps,
            "prompt_tokens": list(request.prompt_tokens),
            "x_downgrade": downgrade,
            "x_preserved_sampling": {
                "temperature": request.temperature,
                "top_p": request.top_p,
            },
        }
        return item, downgrade

    def conversion_detail(self, manifest: Manifest, request_id: str) -> dict[str, Any]:
        try:
            return manifest.conversions[request_id]
        except KeyError:
            raise invalid_manifest(
                "request is not present in the manifest", request_id=request_id
            )


def merge_manifests(manifests: Iterable[Manifest]) -> list[SamplingRequest]:
    """Flatten imported manifests while rejecting duplicate request IDs."""

    result: list[SamplingRequest] = []
    seen: set[str] = set()
    for manifest in manifests:
        for request in manifest.requests:
            if request.request_id in seen:
                raise invalid_manifest(
                    "duplicate request_id across manifests",
                    request_id=request.request_id,
                )
            seen.add(request.request_id)
            result.append(request)
    return result
