from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

from .errors import PipelineError
from .models import Shot
from .motion import compose_frame


def load_rgb_image(path: Path, shot_id: str, stage: str) -> np.ndarray:
    if not path.exists() or not path.is_file():
        raise PipelineError(
            "E_MISSING_ASSET",
            f"image not found: {path.name}",
            stage=stage,
            shot_id=shot_id,
            details={"path": str(path)},
        )
    try:
        with Image.open(path) as image:
            rgb = image.convert("RGB")
            array = np.asarray(rgb, dtype=np.uint8).copy()
    except Exception as exc:
        raise PipelineError(
            "E_MISSING_ASSET",
            f"image cannot be decoded: {path.name}",
            stage=stage,
            shot_id=shot_id,
            details={"error": str(exc)},
        ) from exc
    return array.astype(np.float32) / np.float32(255.0)


class ComposedFrameProvider:
    def __init__(
        self,
        *,
        shots: dict[str, Shot],
        image_dir: Path,
        config: dict[str, Any],
        cache_plan: dict[str, Any],
        stage: str,
    ) -> None:
        self.shots = shots
        self.image_dir = image_dir
        self.config = config
        self.cache_plan = cache_plan
        self.stage = stage
        self.strategy = cache_plan["strategy"]
        self._frame_cache: OrderedDict[tuple[str, int], np.ndarray] = OrderedDict()
        self._segment_cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._full_cache: dict[tuple[str, int], np.ndarray] = {}
        self._image_cache: OrderedDict[str, np.ndarray] = OrderedDict()
        self._cached_bytes = 0
        self.peak_frame_buffer_bytes = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.composite_operations = 0
        self.images: dict[str, Path] = {
            shot.shot_id: image_dir / shot.image for shot in shots.values()
        }

    def _image(self, shot_id: str) -> np.ndarray:
        if shot_id in self._image_cache:
            self._image_cache.move_to_end(shot_id)
            return self._image_cache[shot_id]
        image = load_rgb_image(self.images[shot_id], shot_id, self.stage)
        self._image_cache[shot_id] = image
        while len(self._image_cache) > 2:
            self._image_cache.popitem(last=False)
        return image

    def validate_images(self) -> tuple[int, int]:
        dimensions: tuple[int, int] | None = None
        for shot in self.shots.values():
            image = load_rgb_image(self.images[shot.shot_id], shot.shot_id, self.stage)
            current = (int(image.shape[1]), int(image.shape[0]))
            if dimensions is None:
                dimensions = current
            elif current != dimensions:
                raise PipelineError(
                    "E_INVALID_ASSET",
                    "all images must share identical dimensions",
                    stage=self.stage,
                    shot_id=shot.shot_id,
                    details={"expected": dimensions, "actual": current},
                )
        return dimensions or (0, 0)

    def _compose_shot(self, shot_id: str) -> np.ndarray:
        shot = self.shots[shot_id]
        image = self._image(shot_id)
        frames = []
        for local_frame in range(shot.duration_frames):
            frame, _ = compose_frame(image, shot, local_frame, self.config)
            frames.append(frame)
            self.composite_operations += 1
        return np.stack(frames, axis=0)

    def _store_segment(self, shot_id: str, frames: np.ndarray) -> None:
        capacity_frames = int(self.cache_plan["limits"].get("segment_cache_frames", 0))
        if capacity_frames < frames.shape[0]:
            return
        old = self._segment_cache.pop(shot_id, None)
        if old is not None:
            self._cached_bytes -= int(old.nbytes)
        while self._segment_cache and (
            self._cached_bytes + int(frames.nbytes) > capacity_frames * int(frames[0].nbytes)
            or len(self._segment_cache) >= 2
        ):
            _, evicted = self._segment_cache.popitem(last=False)
            self._cached_bytes -= int(evicted.nbytes)
        self._segment_cache[shot_id] = frames
        self._cached_bytes += int(frames.nbytes)
        self.peak_frame_buffer_bytes = max(self.peak_frame_buffer_bytes, self._cached_bytes)

    def get(self, shot_id: str, local_frame: int) -> np.ndarray:
        shot = self.shots[shot_id]
        if local_frame < 0 or local_frame >= shot.duration_frames:
            raise PipelineError(
                "E_INVALID_SHOT",
                f"local frame {local_frame} outside shot range",
                stage=self.stage,
                shot_id=shot_id,
            )
        key = (shot_id, local_frame)
        if self.strategy == "full_cache" and key in self._full_cache:
            self.cache_hits += 1
            return self._full_cache[key]
        if self.strategy == "segment_cache" and shot_id in self._segment_cache:
            self.cache_hits += 1
            self._segment_cache.move_to_end(shot_id)
            return self._segment_cache[shot_id][local_frame]
        if key in self._frame_cache:
            self.cache_hits += 1
            self._frame_cache.move_to_end(key)
            return self._frame_cache[key]
        self.cache_misses += 1
        if self.strategy == "segment_cache":
            frames = self._compose_shot(shot_id)
            self._store_segment(shot_id, frames)
            if shot_id in self._segment_cache:
                return self._segment_cache[shot_id][local_frame]
        elif self.strategy == "full_cache":
            frames = self._compose_shot(shot_id)
            for index, frame in enumerate(frames):
                frame_key = (shot_id, index)
                if frame_key not in self._full_cache:
                    self._full_cache[frame_key] = frame
                    self._cached_bytes += int(frame.nbytes)
            self.peak_frame_buffer_bytes = max(self.peak_frame_buffer_bytes, self._cached_bytes)
            return self._full_cache[key]
        image = self._image(shot_id)
        frame, _ = compose_frame(image, shot, local_frame, self.config)
        self.composite_operations += 1
        self.peak_frame_buffer_bytes = max(
            self.peak_frame_buffer_bytes, int(frame.nbytes)
        )
        return frame

    def stats(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "composite_operations": self.composite_operations,
            "peak_frame_buffer_bytes": self.peak_frame_buffer_bytes,
            "memory_budget_bytes": self.cache_plan["inputs"]["memory_budget_bytes"],
            "budget_respected": self.peak_frame_buffer_bytes
            <= self.cache_plan["inputs"]["memory_budget_bytes"],
        }
