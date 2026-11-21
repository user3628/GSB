"""Stage 1 - view projection sampling.

For every landmark declared by the manifest, the pixel (u, v) and the camera
extrinsics/intrinsics define a world-space ray:

    d_cam = [(u - cx) / fx, (v - cy) / fy, 1]
    d_world = R^T @ d_cam          (R, t: world -> camera)
    origin  = -R^T @ t             (camera center in world coords)

Samples are taken at t = depth + k * ray_step for every integer k with
|k * ray_step| <= truncation_distance (i.e. inside the truncation window
around the declared landmark depth). Each sample is re-projected into the
image; samples behind the camera, beyond max_ray_depth, or whose projection
falls outside the image bounds are clipped and counted. Surviving samples
carry the image color read at their (nearest-neighbor) projection.

The output record order is: landmark declaration order, then increasing
sample index - fully deterministic, so identical inputs give byte-identical
contribution records.
"""

from __future__ import annotations

import numpy as np

from ..manifest import ViewSpec


def _sample_depths(depth: float, ray_step: float, truncation: float, max_ray_depth: float):
    steps = int(np.floor(truncation / ray_step + 1e-9))
    depths = []
    for k in range(-steps, steps + 1):
        t = depth + k * ray_step
        depths.append(t)
    return depths, max_ray_depth


def run_projection(view: ViewSpec, image: np.ndarray, config: dict) -> dict:
    """Run stage 1 for one view. ``image`` is HxWx3 uint8."""
    ray_step = float(config["ray_step"])
    truncation = float(config["truncation_distance"])
    max_ray_depth = float(config["max_ray_depth"])

    rot = np.asarray(view.extrinsic.rotation, dtype=np.float64).reshape(3, 3)
    trans = np.asarray(view.extrinsic.translation, dtype=np.float64)
    center = -rot.T @ trans
    fx, fy = view.intrinsic.fx, view.intrinsic.fy
    cx, cy = view.intrinsic.cx, view.intrinsic.cy
    height, width = image.shape[0], image.shape[1]

    rec_t, rec_xyz, rec_rgb, rec_lm, rec_sample = [], [], [], [], []
    clipped = 0

    for lm_index, landmark in enumerate(view.landmarks):
        u, v = landmark.pixel
        d_cam = np.array([(u - cx) / fx, (v - cy) / fy, 1.0], dtype=np.float64)
        d_world = rot.T @ d_cam
        depths, _ = _sample_depths(landmark.depth, ray_step, truncation, max_ray_depth)
        for sample_index, t in enumerate(depths):
            if t <= 0.0 or t > max_ray_depth:
                clipped += 1
                continue
            point = center + t * d_world
            cam = rot @ point + trans
            if cam[2] <= 0.0:
                clipped += 1
                continue
            pu = fx * cam[0] / cam[2] + cx
            pv = fy * cam[1] / cam[2] + cy
            iu = int(np.rint(pu))
            iv = int(np.rint(pv))
            if iu < 0 or iu >= width or iv < 0 or iv >= height:
                clipped += 1
                continue
            color = image[iv, iu].astype(np.float64)
            rec_t.append(t)
            rec_xyz.append((point[0], point[1], point[2]))
            rec_rgb.append((color[0], color[1], color[2]))
            rec_lm.append(lm_index)
            rec_sample.append(sample_index)

    count = len(rec_t)
    return {
        "view_id": view.view_id,
        "count": count,
        "clipped_count": clipped,
        "landmark_count": len(view.landmarks),
        "t": np.asarray(rec_t, dtype="<f8").reshape(count),
        "xyz": np.asarray(rec_xyz, dtype="<f8").reshape(count, 3),
        "rgb": np.asarray(rec_rgb, dtype="<f8").reshape(count, 3),
        "landmark_index": np.asarray(rec_lm, dtype="<u4").reshape(count),
        "sample_index": np.asarray(rec_sample, dtype="<u4").reshape(count),
    }
