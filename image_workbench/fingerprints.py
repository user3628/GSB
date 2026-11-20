from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from .hashing import digest_json
from .model import FragmentModel

STAGES = ("source", "repair", "style", "export")


@dataclass(frozen=True)
class StageFingerprint:
    stage: str
    fingerprint: str
    input_fingerprint: str
    config_fingerprint: str
    dependencies: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "fingerprint": self.fingerprint,
            "input_fingerprint": self.input_fingerprint,
            "config_fingerprint": self.config_fingerprint,
            "dependencies": copy.deepcopy(self.dependencies),
        }


def compute_stage_fingerprints(
    model: FragmentModel,
    reference: FragmentModel,
    format_config: dict[str, Any],
) -> dict[str, StageFingerprint]:
    metadata_digest = digest_json(model.metadata)
    source_dependencies = {
        "fragment_id": model.fragment_id,
        "pixel_digest": model.pixel_digest,
        "metadata_digest": metadata_digest,
        "source_decoder": "pillow-rgb-u8-v1",
    }
    source_input = digest_json(
        {
            "fragment_id": model.fragment_id,
            "pixel_digest": model.pixel_digest,
            "metadata_digest": metadata_digest,
        }
    )
    source_config = digest_json({"decoder": "pillow-rgb-u8-v1", "schema_version": 2})
    source_fp = digest_json({"stage": "source", "input": source_input, "config": source_config})

    mask_digest = model.mask_digest
    repair_dependencies = {
        "source_fingerprint": source_fp,
        "mask_digest": mask_digest,
        "repair_iterations": model.repair_iterations,
        "algorithm": "jacobi-8-neighbour-diffusion-v1",
    }
    repair_input = digest_json({"source": source_fp, "mask_digest": mask_digest})
    repair_config = digest_json(
        {"algorithm": "jacobi-8-neighbour-diffusion-v1", "iterations": model.repair_iterations}
    )
    repair_fp = digest_json({"stage": "repair", "input": repair_input, "config": repair_config})

    style_dependencies = {
        "repair_fingerprint": repair_fp,
        "style_reference": model.style_reference,
        "reference_pixel_digest": reference.pixel_digest,
        "style_strength": model.style_strength,
        "algorithm": "reference-mean-std-v1",
    }
    style_input = digest_json(
        {
            "repair": repair_fp,
            "style_reference": model.style_reference,
            "reference_pixel_digest": reference.pixel_digest,
        }
    )
    style_config = digest_json(
        {"algorithm": "reference-mean-std-v1", "strength": model.style_strength, "epsilon": 1e-12}
    )
    style_fp = digest_json({"stage": "style", "input": style_input, "config": style_config})

    format_config_digest = digest_json(format_config)
    export_dependencies = {
        "style_fingerprint": style_fp,
        "output_format": model.output_format,
        "format_config_digest": format_config_digest,
    }
    export_input = digest_json({"style": style_fp, "output_format": model.output_format})
    export_config = digest_json({"format_config": format_config, "output_format": model.output_format})
    export_fp = digest_json({"stage": "export", "input": export_input, "config": export_config})

    return {
        "source": StageFingerprint("source", source_fp, source_input, source_config, source_dependencies),
        "repair": StageFingerprint("repair", repair_fp, repair_input, repair_config, repair_dependencies),
        "style": StageFingerprint("style", style_fp, style_input, style_config, style_dependencies),
        "export": StageFingerprint("export", export_fp, export_input, export_config, export_dependencies),
    }


def explain_invalidation(
    stage: str,
    previous_dependencies: dict[str, Any],
    current_dependencies: dict[str, Any],
) -> str:
    if not previous_dependencies:
        return "initial build"
    if stage == "source":
        if previous_dependencies.get("pixel_digest") != current_dependencies.get("pixel_digest"):
            return "pixel content changed"
        if previous_dependencies.get("metadata_digest") != current_dependencies.get("metadata_digest"):
            return "version metadata changed"
        if previous_dependencies.get("fragment_id") != current_dependencies.get("fragment_id"):
            return "fragment identifier changed"
        return "source decode configuration changed"
    if stage == "repair":
        if previous_dependencies.get("mask_digest") != current_dependencies.get("mask_digest"):
            return "repair mask changed"
        if previous_dependencies.get("repair_iterations") != current_dependencies.get("repair_iterations"):
            return "repair iterations changed"
        return "repair algorithm changed"
    if stage == "style":
        if previous_dependencies.get("style_reference") != current_dependencies.get("style_reference"):
            return "style reference identity changed"
        if previous_dependencies.get("reference_pixel_digest") != current_dependencies.get("reference_pixel_digest"):
            return "style reference pixel content changed"
        if previous_dependencies.get("style_strength") != current_dependencies.get("style_strength"):
            previous = previous_dependencies.get("style_strength")
            current = current_dependencies.get("style_strength")
            return f"style strength changed from {previous!r} to {current!r}"
        return "style algorithm changed"
    if stage == "export":
        if previous_dependencies.get("output_format") != current_dependencies.get("output_format"):
            return "output format preference changed"
        if previous_dependencies.get("format_config_digest") != current_dependencies.get("format_config_digest"):
            return "format capability configuration changed"
        return "export encoder configuration changed"
    return "input dependency changed"

