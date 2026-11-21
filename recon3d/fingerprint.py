"""版本指纹：把每个阶段的输入内容摘要成稳定哈希，驱动增量失效。

指纹链（上游 -> 下游）：
* sample_fingerprint：清单规范化内容 + 全部分片 PNG 字节；
* features 输入：分片图像字节 + 配置版本 + 描述子规则；
* triangulation 输入：全部 features 产物摘要 + 位姿/内参配置 + 配置版本；
* geometry 输入：triangulation 产物摘要 + 误差阈值/下限/采样密度；
* packaging 输入：geometry 产物摘要 + 真值点云 + 配置版本。

任一分片像素、位姿、内参、阈值或密度变化，都会改变对应阶段输入指纹，
从而精确定位受影响的最早阶段并令其下游全部失效。
"""

from __future__ import annotations

import os

from .canonical import fingerprint, fingerprint_bytes
from .features import DESCRIPTION, N_BINS


def sample_fingerprint(manifest: dict, base_dir: str) -> str:
    image_hashes = {}
    for tile in manifest["tiles"]:
        with open(os.path.join(base_dir, tile["image"]), "rb") as handle:
            image_hashes[tile["tile_id"]] = fingerprint_bytes(handle.read())
    return fingerprint({"manifest": manifest, "images": image_hashes})


def features_input_fingerprint(image_hash: str, manifest: dict) -> str:
    return fingerprint(
        {
            "image_hash": image_hash,
            "config_version": manifest["config_version"],
            "descriptor": DESCRIPTION,
            "n_bins": N_BINS,
        }
    )


def triangulation_input_fingerprint(feature_digests: dict, manifest: dict) -> str:
    pose_config = {
        tile["tile_id"]: {
            "rotation": tile["rotation"],
            "translation": tile["translation"],
            "intrinsics": tile["intrinsics"],
        }
        for tile in manifest["tiles"]
    }
    return fingerprint(
        {
            "feature_digests": dict(sorted(feature_digests.items())),
            "pose_config": pose_config,
            "config_version": manifest["config_version"],
            "algorithm_preference": manifest["algorithm_preference"],
        }
    )


def geometry_input_fingerprint(triangulation_digest: str, manifest: dict) -> str:
    return fingerprint(
        {
            "triangulation_digest": triangulation_digest,
            "reprojection_threshold": manifest["reprojection_threshold"],
            "min_points": manifest["min_points"],
            "sample_density": manifest["sample_density"],
        }
    )


def packaging_input_fingerprint(geometry_digest: str, manifest: dict) -> str:
    return fingerprint(
        {
            "geometry_digest": geometry_digest,
            "truth_points": manifest["truth_points"],
            "config_version": manifest["config_version"],
        }
    )
