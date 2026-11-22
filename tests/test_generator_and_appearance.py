from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pytest

from tracklab.appearance import build_appearance_stage, model_from_region
from tracklab.canonical import atomic_write_json, hash_tree, read_json, sha256_file
from tracklab.config import load_settings
from tracklab.errors import E_INVALID_REGION, E_MANIFEST, WorkbenchError
from tracklab.generator import generate_demo
from tracklab.manifest import validate_manifest


def test_generator_is_repeatable_and_manifest_counts_are_consistent(workspace):
    manifest_path = workspace.manifest_path
    summary_path = workspace.reference_summary_path
    first_manifest = manifest_path.read_bytes()
    first_summary = summary_path.read_bytes()
    first_tree = hash_tree(workspace.data_dir / "sequences")

    generator = generate_demo(workspace, force=True)
    second_manifest = manifest_path.read_bytes()
    second_summary = summary_path.read_bytes()
    second_tree = hash_tree(workspace.data_dir / "sequences")

    assert first_manifest == second_manifest
    assert first_summary == second_summary
    assert first_tree == second_tree
    assert generator["manifest_sha256"] == hashlib.sha256(second_manifest).hexdigest()

    manifest = validate_manifest(workspace, verify_frame_files=True)
    assert len(manifest.sequences) == 3
    motion_types = {seq["motion_model"]["type"] for seq in manifest.sequences}
    assert any("uniform" in item for item in motion_types)
    assert any("scaling" in item for item in motion_types)
    assert any("direction_change" in item for item in motion_types)
    assert any(seq["occlusion_intervals"] for seq in manifest.sequences)
    for sequence in manifest.sequences:
        assert sequence["visible_frames"] == len(sequence["frames"])
        assert sequence["visible_frames"] <= sequence["sample_count_upper_bound"]
        assert len(sequence["frames"]) <= sequence["sample_count_upper_bound"]
        for entry in sequence["frames"]:
            path = workspace.data_dir / entry["path"]
            assert path.exists()
            assert sha256_file(path) == entry["sha256"]


def test_appearance_stage_is_byte_identical_and_invalid_regions_are_explicit(workspace):
    manifest = validate_manifest(workspace, verify_frame_files=True)
    sequence = manifest.sequences[0]
    from tracklab.workbench import Workbench

    workbench = Workbench(workspace)
    first = workbench.run_stage(sequence["sequence_id"], "appearance")
    artifact_path = Path(first["appearance"]["artifact_path"])
    first_bytes = artifact_path.read_bytes()
    artifact_path.unlink()
    second = workbench.run_stage(sequence["sequence_id"], "appearance")
    assert artifact_path.read_bytes() == first_bytes
    assert second["appearance"]["appearance_fingerprint"] == first["appearance"]["appearance_fingerprint"]

    frame = np.zeros((72, 96), dtype=np.uint8)
    with pytest.raises(WorkbenchError) as all_zero:
        model_from_region(
            frame,
            [0.1, 0.1, 0.2, 0.2],
            settings=workspace,
            sequence_id=sequence["sequence_id"],
            frame_index=7,
            label="test_region",
        )
    assert all_zero.value.code == E_INVALID_REGION
    assert all_zero.value.context["sequence_id"] == sequence["sequence_id"]
    assert all_zero.value.context["frame_index"] == 7
    assert all_zero.value.context["diagnostic"] == "all_zero_region"

    with pytest.raises(WorkbenchError) as empty:
        model_from_region(
            np.ones((72, 96), dtype=np.uint8) * 20,
            [0.1, 0.1, 0.0, 0.2],
            settings=workspace,
            sequence_id=sequence["sequence_id"],
            frame_index=8,
            label="test_region",
        )
    assert empty.value.code == E_INVALID_REGION
    assert empty.value.context["diagnostic"] == "empty_box"

    with pytest.raises(WorkbenchError) as outside:
        model_from_region(
            np.ones((72, 96), dtype=np.uint8) * 20,
            [0.9, 0.1, 0.3, 0.2],
            settings=workspace,
            sequence_id=sequence["sequence_id"],
            frame_index=9,
            label="test_region",
        )
    assert outside.value.code == E_INVALID_REGION
    assert outside.value.context["diagnostic"] == "box_outside_image"


def test_manifest_rejects_windows_unsafe_frame_components(workspace):
    raw = read_json(workspace.manifest_path)
    raw["sequences"][0]["frames"][0]["path"] = "CON/000000.npy"
    atomic_write_json(workspace.manifest_path, raw, pretty=True)
    with pytest.raises(WorkbenchError) as unsafe_name:
        validate_manifest(workspace, verify_frame_files=False)
    assert unsafe_name.value.code == E_MANIFEST
    assert unsafe_name.value.context["path"] == "CON/000000.npy"
