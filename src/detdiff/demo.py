"""Deterministic local demonstration fixture and end-to-end scenario."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from .config import parse_config
from .diagnostics import write_html_dashboard
from .errors import BusinessError
from .pipeline import BuildHooks, BuildStopped, build_workspace, inspect_workspace, verify_against_full
from .quantization import encode_png_rgb8
from .sampling import generate_noise
from .util import canonical_json_bytes, stable_hash
from .workspace import update_shard, write_config


DEMO_WIDTH = 48
DEMO_HEIGHT = 48


def generate_demo_image(seed: int, width: int = DEMO_WIDTH, height: int = DEMO_HEIGHT) -> bytes:
    noise = generate_noise(seed, (height, width, 3), sampler_version=900)
    yy, xx = np.mgrid[0:height, 0:width]
    phase = (seed % 997) / 997.0 * 6.283185307179586
    radial = np.sqrt(((xx - width / 2.0) / width) ** 2 + ((yy - height / 2.0) / height) ** 2)
    base = np.stack(
        [
            0.5 + 0.3 * np.sin(radial * (5.0 + (seed % 5)) * 6.283185307179586 + phase + channel * 1.7)
            for channel in range(3)
        ],
        axis=-1,
    )
    combined = np.clip(0.72 * base + 0.28 * noise, -1.0, 1.0)
    image = np.rint((combined + 1.0) * 127.5).astype(np.uint8)
    return encode_png_rgb8(image, compression_level=9)


def initialize_demo(root: Path, *, reset: bool = False) -> Path:
    root = root.resolve()
    if root.exists() and any(root.iterdir()):
        if not reset:
            raise FileExistsError(f"demo workspace is not empty: {root}; use --reset")
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    (root / "inputs").mkdir()
    (root / "references").mkdir()

    images = {
        "aurora-en": 1101,
        "mountain-zh": 2202,
        "harbor-zh-en": 3303,
        "microstructure-en": 4404,
    }
    for name, seed in images.items():
        (root / "inputs" / f"{name}.png").write_bytes(generate_demo_image(seed))
    (root / "references" / "harbor.png").write_bytes(generate_demo_image(9303))

    raw: dict[str, Any] = {
        "schema_version": 1,
        "global": {
            "encoder": {
                "name": "sha256-xof-16",
                "version": 1,
                "dimension": 16,
                "domain": "detdiff.demo.v1",
            },
            "image": {"width": DEMO_WIDTH, "height": DEMO_HEIGHT, "channels": 3},
            "quantize": {"version": 1, "levels": 256, "png_compression": 9},
            "trace_version": 1,
        },
        "shards": [
            {
                "id": "aurora-en",
                "prompt": "Aurora over a deterministic mineral ridge, fine diagnostic texture",
                "guidance_scale": 3.5,
                "steps": 14,
                "seed": 1101,
                "source_image": "inputs/aurora-en.png",
            },
            {
                "id": "mountain-zh",
                "prompt": "中文提示词：层叠山脊与低温矿物纹理，清晰边界",
                "guidance_scale": 6.0,
                "steps": 18,
                "seed": 2202,
                "source_image": "inputs/mountain-zh.png",
            },
            {
                "id": "harbor-zh-en",
                "prompt": "港口地层与暖色沉积带 / harbor strata with warm sediment bands",
                "guidance_scale": 4.0,
                "steps": 16,
                "seed": 3303,
                "reference": "references/harbor.png",
                "blend_strength": 0.38,
                "source_image": "inputs/harbor-zh-en.png",
            },
            {
                "id": "microstructure-en",
                "prompt": "Low-guidance crystalline microstructure, sparse grain boundaries",
                "guidance_scale": 1.25,
                "steps": 9,
                "seed": 4404,
                "source_image": "inputs/microstructure-en.png",
            },
        ],
    }
    parsed = parse_config(raw, root=root)
    write_config(root / "config.yaml", parsed.raw)
    return root


def run_demo(root: Path, *, reset: bool = False) -> dict[str, Any]:
    root = initialize_demo(root, reset=reset)
    result: dict[str, Any] = {"workspace": str(root)}

    try:
        build_workspace(root, hooks=BuildHooks(stop_after_stage="encode"))
    except BuildStopped as stopped:
        result["interrupted_build"] = stopped.report.to_dict()
        result["resume_from"] = inspect_workspace(root=root)[0].earliest_invalid

    resumed = build_workspace(root)
    result["resumed_build"] = resumed.to_dict()
    baseline = verify_against_full(root)
    result["baseline_full_consistency"] = {
        "ok": baseline["ok"],
        "files_compared": baseline["files_compared"],
        "manifest_sha256": stable_hash(baseline["manifest"]),
    }

    update = update_shard(
        root,
        "mountain-zh",
        {
            "prompt": "中文提示词：层叠山脊、冷色矿物纹理与新增诊断网格",
            "guidance_scale": 6.75,
        },
    )
    result["incremental_update"] = update
    post_update = verify_against_full(root)
    result["post_update_full_consistency"] = {
        "ok": post_update["ok"],
        "files_compared": post_update["files_compared"],
        "manifest_sha256": stable_hash(post_update["manifest"]),
    }

    diagnostics = inspect_workspace(root=root)
    html_path = root / "diagnostics.html"
    write_html_dashboard(diagnostics, html_path, config=parse_config(yaml.safe_load((root / "config.yaml").read_text("utf-8")), root))
    result["diagnostics_html"] = str(html_path)

    probes: dict[str, Any] = {}
    with tempfile.TemporaryDirectory(prefix="detdiff-errors-") as temp:
        clone = Path(temp) / "workspace"
        shutil.copytree(root, clone)
        try:
            update_shard(clone, "aurora-en", {"blend_strength": 1.5})
        except BusinessError as exc:
            probes["invalid_blend"] = exc.to_dict()

        raw = yaml.safe_load((clone / "config.yaml").read_text("utf-8"))
        raw["shards"][0]["guidance_scale"] = -2.0
        try:
            parse_config(raw, root=clone)
        except BusinessError as exc:
            probes["invalid_guidance"] = exc.to_dict()

        raw = yaml.safe_load((clone / "config.yaml").read_text("utf-8"))
        raw["shards"][2]["reference"] = "references/does-not-exist.png"
        write_config(clone / "config.yaml", raw)
        try:
            build_workspace(clone)
        except BusinessError as exc:
            probes["missing_reference"] = exc.to_dict()
    result["error_code_probes"] = probes

    result["status"] = [item.to_dict() for item in diagnostics]
    report_path = root / "demo_report.json"
    report_path.write_bytes(canonical_json_bytes(result) + b"\n")
    result["report_path"] = str(report_path)
    return result

