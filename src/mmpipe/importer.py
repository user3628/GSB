"""样本与配置导入：把 samples 目录的确定性样本装载进工作区。"""

import json
import os

from .codec import canonical_dumps, fingerprint
from .pipeline import load_images, load_texts
from .platform_io import atomic_write
from .storage import INPUTS_DIR, Workspace


def import_samples(workspace: Workspace, texts_path: str,
                   images_path: str, config_path: str) -> dict:
    with open(config_path, "rb") as handle:
        config = json.loads(handle.read())
    texts = load_texts(texts_path)
    images = load_images(images_path)

    atomic_write(workspace.path(INPUTS_DIR, "texts.jsonl"),
                 b"".join(canonical_dumps(rec) + b"\n" for rec in texts))
    atomic_write(workspace.path(INPUTS_DIR, "images.json"),
                 canonical_dumps(images))
    workspace.save_config(config)

    manifest = {
        "texts_file": os.path.abspath(texts_path),
        "images_file": os.path.abspath(images_path),
        "config_file": os.path.abspath(config_path),
        "text_count": len(texts),
        "image_count": len(images),
        "texts_fingerprint": fingerprint(
            [{"id": r.get("id"), "text": r.get("text"), "lang": r.get("lang")}
             for r in sorted(texts, key=lambda r: str(r.get("id")))]),
        "images_fingerprint": fingerprint(
            [{"id": r.get("id"), "feature": r.get("feature"),
              "text_id": r.get("text_id"), "operator": r.get("operator")}
             for r in sorted(images, key=lambda r: str(r.get("id")))]),
        "config_fingerprint": fingerprint(config),
    }
    atomic_write(workspace.path(INPUTS_DIR, "import_manifest.json"),
                 canonical_dumps(manifest))
    workspace.log_event("import", **manifest)
    return manifest


def load_workspace_inputs(workspace: Workspace):
    texts = load_texts(workspace.path(INPUTS_DIR, "texts.jsonl"))
    images = load_images(workspace.path(INPUTS_DIR, "images.json"))
    config = workspace.load_config()
    return config, texts, images
