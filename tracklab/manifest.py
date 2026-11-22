"""Tracking manifest parsing and consistency checks."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import digest, read_json, sha256_file
from .config import Settings
from .errors import E_MANIFEST, E_NOT_FOUND, raise_error
from .geometry import validate_box

_SEQ_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


@dataclass
class ManifestView:
    settings: Settings
    raw: dict[str, Any]

    @property
    def sequences(self) -> list[dict[str, Any]]:
        return list(self.raw["sequences"])

    def by_id(self, sequence_id: str) -> dict[str, Any]:
        for sequence in self.sequences:
            if sequence["sequence_id"] == sequence_id:
                return sequence
        raise_error(
            E_NOT_FOUND,
            "sequence is not present in the tracking manifest",
            context={"sequence_id": sequence_id},
        )

    @property
    def fingerprint(self) -> str:
        return digest(self.raw)

    def frame_path(self, sequence: dict[str, Any], frame_index: int) -> Path:
        entry = sequence["frames"][frame_index]
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise_error(
                E_MANIFEST,
                "frame paths must be relative and remain inside the data directory",
                context={"sequence_id": sequence["sequence_id"], "frame_index": frame_index, "path": entry["path"]},
            )
        path = (self.settings.data_dir / relative).resolve()
        data_root = self.settings.data_dir.resolve()
        try:
            path.relative_to(data_root)
        except ValueError:
            raise_error(
                E_MANIFEST,
                "frame path escapes the data directory",
                context={"sequence_id": sequence["sequence_id"], "frame_index": frame_index, "path": entry["path"]},
            )
        return path

    def load_frame(self, sequence: dict[str, Any], frame_index: int) -> np.ndarray:
        path = self.frame_path(sequence, frame_index)
        if not path.exists():
            raise_error(
                E_MANIFEST,
                "frame file is missing",
                context={"sequence_id": sequence["sequence_id"], "frame_index": frame_index, "path": str(path)},
            )
        try:
            array = np.load(path, allow_pickle=False)
        except (OSError, ValueError) as exc:
            raise_error(
                E_MANIFEST,
                "frame file cannot be loaded as a NumPy array",
                context={"sequence_id": sequence["sequence_id"], "frame_index": frame_index, "path": str(path), "detail": str(exc)},
            )
        expected = tuple(int(v) for v in sequence["image_shape"])
        if array.shape != expected:
            raise_error(
                E_MANIFEST,
                "frame shape does not match the manifest",
                context={
                    "sequence_id": sequence["sequence_id"],
                    "frame_index": frame_index,
                    "expected_shape": list(expected),
                    "actual_shape": list(array.shape),
                },
            )
        if array.dtype != np.uint8:
            raise_error(
                E_MANIFEST,
                "frame dtype must be uint8 with values 0..255",
                context={"sequence_id": sequence["sequence_id"], "frame_index": frame_index, "dtype": str(array.dtype)},
            )
        return array

    def actual_frame_sha256(self, sequence: dict[str, Any], frame_index: int) -> str:
        return sha256_file(self.frame_path(sequence, frame_index))


def _validate_box_field(box: Any, sequence_id: str, frame_index: int, label: str) -> list[float]:
    return validate_box(box, sequence_id=sequence_id, frame_index=frame_index, label=label)


def validate_manifest(settings: Settings, *, verify_frame_files: bool = True) -> ManifestView:
    path = settings.manifest_path
    if not path.exists():
        raise_error(E_MANIFEST, "tracking manifest does not exist", context={"path": str(path)})
    try:
        raw = read_json(path)
    except (OSError, ValueError) as exc:
        raise_error(E_MANIFEST, "tracking manifest is not valid JSON", context={"path": str(path), "detail": str(exc)})
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise_error(E_MANIFEST, "unsupported tracking manifest schema", context={"path": str(path)})
    sequences = raw.get("sequences")
    if not isinstance(sequences, list) or len(sequences) < 3:
        raise_error(E_MANIFEST, "tracking manifest must contain at least three sequences")
    seen_ids: set[str] = set()
    for seq in sequences:
        if not isinstance(seq, dict):
            raise_error(E_MANIFEST, "each sequence entry must be an object")
        sequence_id = seq.get("sequence_id")
        if not isinstance(sequence_id, str) or not _SEQ_RE.fullmatch(sequence_id):
            raise_error(E_MANIFEST, "sequence_id is not Windows-safe", context={"sequence_id": sequence_id})
        if sequence_id.upper() in _WINDOWS_RESERVED:
            raise_error(E_MANIFEST, "sequence_id is a reserved Windows filename", context={"sequence_id": sequence_id})
        if sequence_id in seen_ids:
            raise_error(E_MANIFEST, "sequence_id values must be unique", context={"sequence_id": sequence_id})
        seen_ids.add(sequence_id)
        image_shape = seq.get("image_shape")
        if not isinstance(image_shape, list) or len(image_shape) != 2:
            raise_error(E_MANIFEST, "image_shape must be [height, width]", context={"sequence_id": sequence_id})
        height, width = (int(v) for v in image_shape)
        if height < 8 or width < 8:
            raise_error(E_MANIFEST, "image dimensions are too small", context={"sequence_id": sequence_id})
        fps = seq.get("frame_rate")
        if not isinstance(fps, (int, float)) or float(fps) <= 0:
            raise_error(E_MANIFEST, "frame_rate must be positive", context={"sequence_id": sequence_id})
        initial_box = _validate_box_field(seq.get("initial_box"), sequence_id, 0, "initial_box")
        if not (0.0 <= float(seq.get("search_window_expansion", -1)) <= 4.0):
            raise_error(E_MANIFEST, "search_window_expansion must be in [0, 4]", context={"sequence_id": sequence_id})
        threshold = float(seq.get("appearance_update_threshold", -1))
        floor = float(seq.get("trace_confidence_floor", -1))
        if not (0.0 < threshold <= 1.0):
            raise_error(E_MANIFEST, "appearance_update_threshold must be in (0, 1]", context={"sequence_id": sequence_id})
        if not (0.0 < floor <= 1.0):
            raise_error(E_MANIFEST, "trace_confidence_floor must be in (0, 1]", context={"sequence_id": sequence_id})
        if threshold < floor:
            raise_error(E_MANIFEST, "appearance_update_threshold must be >= trace_confidence_floor", context={"sequence_id": sequence_id})
        frames = seq.get("frames")
        if not isinstance(frames, list) or not frames:
            raise_error(E_MANIFEST, "frames must be a non-empty list", context={"sequence_id": sequence_id})
        visible_frames = seq.get("visible_frames")
        if int(visible_frames) != len(frames):
            raise_error(
                E_MANIFEST,
                "visible_frames does not match the declared frame list",
                context={"sequence_id": sequence_id, "visible_frames": visible_frames, "declared_frames": len(frames)},
            )
        bound = seq.get("sample_count_upper_bound")
        if not isinstance(bound, int) or bound < len(frames):
            raise_error(
                E_MANIFEST,
                "sample_count_upper_bound must be an integer >= visible_frames",
                context={"sequence_id": sequence_id, "sample_count_upper_bound": bound, "visible_frames": len(frames)},
            )
        seen_indices: set[int] = set()
        for index, entry in enumerate(frames):
            if not isinstance(entry, dict) or int(entry.get("frame_index", -1)) != index:
                raise_error(
                    E_MANIFEST,
                    "frame_index values must be contiguous and zero-based",
                    context={"sequence_id": sequence_id, "position": index},
                )
            if index in seen_indices:
                raise_error(E_MANIFEST, "duplicate frame_index", context={"sequence_id": sequence_id, "frame_index": index})
            seen_indices.add(index)
            _validate_box_field(entry.get("box_xywh_norm"), sequence_id, index, "reference_truth_box")
            frame_path = entry.get("path")
            if not isinstance(frame_path, str) or not frame_path:
                raise_error(E_MANIFEST, "frame path is missing", context={"sequence_id": sequence_id, "frame_index": index})
            lowered = frame_path.lower()
            if any(ch in frame_path for ch in '<>:"|?*') or "\\" in frame_path:
                raise_error(E_MANIFEST, "frame path is not portable", context={"sequence_id": sequence_id, "frame_index": index, "path": frame_path})
            path_parts = Path(frame_path).parts
            if Path(frame_path).is_absolute() or ".." in path_parts or any(
                part.endswith((" ", ".")) or part.split(".", 1)[0].upper() in _WINDOWS_RESERVED
                for part in path_parts
            ):
                raise_error(
                    E_MANIFEST,
                    "frame path contains a Windows-unsafe component",
                    context={"sequence_id": sequence_id, "frame_index": index, "path": frame_path},
                )
            if not lowered.endswith(".npy"):
                raise_error(E_MANIFEST, "frame path must end with .npy", context={"sequence_id": sequence_id, "frame_index": index})
            if verify_frame_files:
                candidate = settings.data_dir / frame_path
                if not candidate.exists():
                    raise_error(
                        E_MANIFEST,
                        "declared frame file is missing",
                        context={"sequence_id": sequence_id, "frame_index": index, "path": str(candidate)},
                    )
                try:
                    array = np.load(candidate, allow_pickle=False)
                except (OSError, ValueError) as exc:
                    raise_error(E_MANIFEST, "declared frame file is unreadable", context={"sequence_id": sequence_id, "frame_index": index, "detail": str(exc)})
                if array.shape != (height, width):
                    raise_error(
                        E_MANIFEST,
                        "declared frame shape does not match image_shape",
                        context={"sequence_id": sequence_id, "frame_index": index, "shape": list(array.shape), "image_shape": [height, width]},
                    )
                if array.dtype != np.uint8:
                    raise_error(E_MANIFEST, "declared frame dtype must be uint8", context={"sequence_id": sequence_id, "frame_index": index, "dtype": str(array.dtype)})
    return ManifestView(settings=settings, raw=raw)
