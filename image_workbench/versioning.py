"""Stage-level content fingerprints and invalidation analysis."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .chain import ParsedChain
from .config import Config
from .models import TaskSpec
from .operators import KERNEL_VERSION, PARSER_VERSION, PNG_ENCODER_VERSION
from .util import sha256_json


STAGES = ("parsed", "executed", "size_checked", "exported")
STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


@dataclass(frozen=True)
class TaskVersionSet:
    parse_components: dict[str, Any]
    parse_fingerprint: str
    executed_components: dict[str, Any] | None = None
    executed_fingerprint: str | None = None
    size_checked_components: dict[str, Any] | None = None
    size_checked_fingerprint: str | None = None
    exported_components: dict[str, Any] | None = None
    exported_fingerprint: str | None = None

    def fingerprint_for(self, stage: str) -> str | None:
        return {
            "parsed": self.parse_fingerprint,
            "executed": self.executed_fingerprint,
            "size_checked": self.size_checked_fingerprint,
            "exported": self.exported_fingerprint,
        }[stage]

    def components_for(self, stage: str) -> dict[str, Any] | None:
        return {
            "parsed": self.parse_components,
            "executed": self.executed_components,
            "size_checked": self.size_checked_components,
            "exported": self.exported_components,
        }[stage]

    def as_public_dict(self) -> dict[str, Any]:
        return {
            "fingerprints": {
                stage: self.fingerprint_for(stage) for stage in STAGES
            },
            "components": {
                stage: self.components_for(stage) for stage in STAGES
            },
        }


def compute_task_versions(
    task: TaskSpec,
    parsed: ParsedChain | None,
    input_digest: str,
    config: Config,
    *,
    chain_error: dict[str, Any] | None = None,
) -> TaskVersionSet:
    parse_components: dict[str, Any] = {
        "input_content_digest": input_digest,
        "instruction": task.instruction,
        "allowed_operators": list(task.allowed_operators),
        "operations": parsed.public_operations() if parsed else list(task.operations),
        "output_size": list(parsed.output_size) if parsed else None,
        "scheduling": config.scheduling_components(),
        "config_version": config.config_version,
        "parser_version": PARSER_VERSION,
        "chain_error": chain_error,
    }
    parse_fp = sha256_json(parse_components)
    if parsed is None:
        return TaskVersionSet(
            parse_components=parse_components,
            parse_fingerprint=parse_fp,
        )
    executed_components = {
        "parse_fingerprint": parse_fp,
        "input_content_digest": input_digest,
        "operator_kernel_version": KERNEL_VERSION,
    }
    executed_fp = sha256_json(executed_components)
    size_components = {
        "executed_fingerprint": executed_fp,
        "expected_output_size": list(task.expected_size)
        if task.expected_size
        else None,
    }
    size_fp = sha256_json(size_components)
    export_components = {
        "size_checked_fingerprint": size_fp,
        "expected_output_sha256": task.expected_digest,
        "export_format": config.export_format,
        "png_encoder_version": PNG_ENCODER_VERSION,
    }
    export_fp = sha256_json(export_components)
    return TaskVersionSet(
        parse_components=parse_components,
        parse_fingerprint=parse_fp,
        executed_components=executed_components,
        executed_fingerprint=executed_fp,
        size_checked_components=size_components,
        size_checked_fingerprint=size_fp,
        exported_components=export_components,
        exported_fingerprint=export_fp,
    )


def changed_component_names(
    expected: dict[str, Any] | None, recorded: dict[str, Any] | None
) -> list[str]:
    if expected is None:
        return []
    if not isinstance(recorded, dict):
        return ["<recorded_components_missing>"]
    changed: list[str] = []
    for key in sorted(set(expected) | set(recorded)):
        if expected.get(key) != recorded.get(key):
            changed.append(key)
    return changed
