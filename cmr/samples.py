"""Deterministic local sample generator.

Produces, from a fixed seed:
  * RGB PNG fragments (gradient background + geometric shapes)
  * a JSONL manifest (meta line declaring feature dims + one line per pair)
  * projection matrices (image->embed, text->embed)
Everything is derived from the configured seed, so repeated runs are
byte-identical. No network, no external services.
"""

import json
import os

import numpy as np
from PIL import Image

PALETTE = {
    "red": (220, 40, 40),
    "green": (40, 180, 70),
    "blue": (50, 90, 220),
    "yellow": (235, 205, 40),
    "purple": (150, 60, 190),
    "cyan": (40, 190, 200),
}
COLOR_ZH = {
    "red": "红色",
    "green": "绿色",
    "blue": "蓝色",
    "yellow": "黄色",
    "purple": "紫色",
    "cyan": "青色",
}
SHAPE_ZH = {"circle": "圆形", "square": "方块", "triangle": "三角形", "stripes": "斜条纹"}


def _gradient(size, c0, c1, direction):
    yy, xx = np.mgrid[0:size, 0:size].astype(np.float64) / (size - 1)
    t = {"h": xx, "v": yy, "d": (xx + yy) / 2.0, "r": 1.0 - (xx + yy) / 2.0}[direction]
    img = np.outer(1.0 - t, c0) + np.outer(t, c1)
    return img.reshape(size, size, 3)


def _draw_shape(img, shape, color, rng):
    size = img.shape[0]
    yy, xx = np.mgrid[0:size, 0:size]
    cx = size * (0.35 + 0.3 * rng.random())
    cy = size * (0.35 + 0.3 * rng.random())
    rad = size * (0.18 + 0.08 * rng.random())
    if shape == "circle":
        mask = (xx - cx) ** 2 + (yy - cy) ** 2 <= rad**2
    elif shape == "square":
        mask = (np.abs(xx - cx) <= rad) & (np.abs(yy - cy) <= rad)
    elif shape == "triangle":
        mask = (yy >= cy - rad) & (yy <= cy + rad) & (np.abs(xx - cx) <= (cy + rad - yy) * 0.9)
    else:  # stripes
        width = max(2, size // 12)
        mask = ((xx + yy) % (2 * width)) < width
    img[mask] = np.asarray(color, dtype=np.float64)
    return img


def generate_samples(data_dir, cfg):
    """Generate fragments + manifest. Returns the manifest path."""
    frag_dir = os.path.join(data_dir, "fragments")
    os.makedirs(frag_dir, exist_ok=True)
    rng = np.random.default_rng(cfg.seed)
    size = cfg.image_size
    names = sorted(PALETTE)
    shapes = ["circle", "square", "triangle", "stripes"]
    directions = ["h", "v", "d", "r"]
    pairs = []
    for idx in range(cfg.num_fragments):
        c0 = np.asarray(PALETTE[names[rng.integers(len(names))]], dtype=np.float64) * 0.35
        c1 = np.asarray(PALETTE[names[rng.integers(len(names))]], dtype=np.float64) * 0.55
        direction = directions[int(rng.integers(len(directions)))]
        img = _gradient(size, c0, c1, direction)
        color_name = names[int(rng.integers(len(names)))]
        shape = shapes[int(rng.integers(len(shapes)))]
        sub_rng = np.random.default_rng(cfg.seed * 1000 + idx)
        img = _draw_shape(img, shape, PALETTE[color_name], sub_rng)
        arr = np.clip(np.round(img), 0, 255).astype(np.uint8)
        image_file = f"fragments/frag_{idx:02d}.png"
        Image.fromarray(arr, "RGB").save(os.path.join(data_dir, image_file), format="PNG", optimize=False)
        caption = (
            f"{COLOR_ZH[color_name]}{SHAPE_ZH[shape]} {color_name} {shape}, "
            f"渐变背景 gradient background, 合成样本 synthetic sample {idx:02d}"
        )
        pairs.append({
            "pair_id": f"pair_{idx:02d}",
            "image_file": image_file,
            "caption": caption,
            "lang": "mixed",
            "source": "synthetic",
        })
    manifest_path = os.path.join(data_dir, "pairs.jsonl")
    meta = {
        "type": "manifest_meta",
        "declared_image_dim": cfg.image_dim,
        "declared_text_dim": cfg.text_dim,
        "num_pairs": len(pairs),
    }
    with open(manifest_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, ensure_ascii=False, sort_keys=True) + "\n")
        for pair in pairs:
            fh.write(json.dumps(pair, ensure_ascii=False, sort_keys=True) + "\n")
    return manifest_path


def generate_projection(data_dir, cfg):
    """Deterministically derive cross-modal projection matrices.

    The leading rows come from an SVD of the paired cross-covariance between
    the sample image and text features (PLS-style alignment), so cosine in
    the shared space is meaningful; remaining rows are a fixed-seed random
    complement at small scale. Fully deterministic for a fixed seed and
    fixed samples.
    """
    from .image_features import extract_image_features
    from .text_features import extract_text_features

    pairs = []
    with open(os.path.join(data_dir, "pairs.jsonl"), "r", encoding="utf-8") as fh:
        for line in fh:
            obj = json.loads(line)
            if obj.get("type") != "manifest_meta":
                pairs.append(obj)
    pairs.sort(key=lambda p: p["pair_id"])
    x_img = np.stack([extract_image_features(os.path.join(data_dir, p["image_file"]), cfg)
                      for p in pairs])
    x_txt = np.stack([extract_text_features(p["caption"], cfg) for p in pairs])
    x_img_c = x_img - x_img.mean(axis=0)
    x_txt_c = x_txt - x_txt.mean(axis=0)
    cross = x_img_c.T @ x_txt_c
    u, _s, vt = np.linalg.svd(cross, full_matrices=False)
    rank = int(min((_s > 1e-12).sum(), cfg.embed_dim))
    w_img = np.zeros((cfg.embed_dim, cfg.image_dim))
    w_txt = np.zeros((cfg.embed_dim, cfg.text_dim))
    w_img[:rank] = u[:, :rank].T
    w_txt[:rank] = vt[:rank, :]
    rng = np.random.default_rng(cfg.proj_seed)
    if rank < cfg.embed_dim:
        rest = cfg.embed_dim - rank
        w_img[rank:] = 0.05 * rng.standard_normal((rest, cfg.image_dim)) / np.sqrt(cfg.image_dim)
        w_txt[rank:] = 0.05 * rng.standard_normal((rest, cfg.text_dim)) / np.sqrt(cfg.text_dim)
    obj = {
        "embed_dim": cfg.embed_dim,
        "image": {"rows": cfg.embed_dim, "cols": cfg.image_dim, "data": w_img.tolist()},
        "text": {"rows": cfg.embed_dim, "cols": cfg.text_dim, "data": w_txt.tolist()},
    }
    path = os.path.join(data_dir, "projection.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, ensure_ascii=False, sort_keys=True)
        fh.write("\n")
    return path


def init_workspace(data_dir, cfg):
    """Create config, projection and samples if absent. Returns created paths."""
    os.makedirs(data_dir, exist_ok=True)
    created = []
    cfg_path = os.path.join(data_dir, "config.json")
    if not os.path.exists(cfg_path):
        cfg.save(cfg_path)
        created.append(cfg_path)
    manifest_path = os.path.join(data_dir, "pairs.jsonl")
    if not os.path.exists(manifest_path):
        generate_samples(data_dir, cfg)
        created.append(manifest_path)
    proj_path = os.path.join(data_dir, "projection.json")
    if not os.path.exists(proj_path):
        generate_projection(data_dir, cfg)
        created.append(proj_path)
    return created

