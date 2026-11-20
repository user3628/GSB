"""Alignment project manifests: structure version 1 / 2 parsing and upgrade.

Version 2 manifests declare a float scale and an explicit projection matrix.
Version 1 manifests declare an integer percentage scale, register the vision
dimension as 64 and carry no projection matrix; the *actual* vision
dimension is explicitly declared in ``actual_vision_dim``.  Importing a v1
manifest performs an explicit upgrade:

* vision dimension corrected to the explicitly declared actual dimension;
* integer percentage scale converted by the configurable rule
  (default ``pct_div_100``: 75 -> 0.75);
* a deterministic projection matrix generated from (text_dim, vision_dim).

The upgraded project can be re-exported as a v2-equivalent manifest.
"""

import numpy as np
import yaml

from .errors import E_FORMAT, EngineError
from .util import fingerprint_obj

PROJECTION_SEED_DOMAIN = "align-projection"


def deterministic_projection_matrix(text_dim, vision_dim):
    """Deterministic projection matrix (text_dim x vision_dim), unit-norm rows."""
    seed_material = "{}|{}|{}".format(PROJECTION_SEED_DOMAIN, text_dim, vision_dim)
    seed = int.from_bytes(
        fingerprint_obj(seed_material).encode("ascii")[:8], "big")
    rng = np.random.default_rng(seed)
    matrix = rng.standard_normal((text_dim, vision_dim))
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / norms


def load_manifest_doc(path):
    with open(path, "r", encoding="utf-8") as handle:
        doc = yaml.safe_load(handle)
    if not isinstance(doc, dict):
        raise EngineError(E_FORMAT, "manifest must be a mapping",
                          {"path": path})
    return doc


def convert_scale_percent(percent, rule):
    percent = int(percent)
    if rule == "pct_div_100":
        return percent / 100.0
    raise EngineError(E_FORMAT, "unknown scale conversion rule",
                      {"rule": rule})


def manifest_to_spec(doc, config):
    """Parse a manifest document into a normalized project spec (v2 shape)."""
    version = doc.get("manifest_version")
    if version not in (1, 2):
        raise EngineError(E_FORMAT, "unsupported manifest version",
                          {"manifest_version": version})
    try:
        text_dim = int(doc["modalities"]["text"]["dim"])
        registered_vision_dim = int(doc["modalities"]["vision"]["dim"])
        shard_refs = doc["shards"]
    except (KeyError, TypeError) as exc:
        raise EngineError(E_FORMAT, "manifest missing required field",
                          {"field": str(exc)})

    upgrade_notes = None
    if version == 2:
        vision_dim = registered_vision_dim
        scale = float(doc["projection"]["scale"])
        matrix = [[float(x) for x in row]
                  for row in doc["projection"]["matrix"]]
    else:
        if "actual_vision_dim" not in doc:
            raise EngineError(
                E_FORMAT, "v1 manifest must explicitly declare actual_vision_dim",
                {})
        vision_dim = int(doc["actual_vision_dim"])
        scale = convert_scale_percent(doc["projection"]["scale_percent"],
                                      config.scale_rule)
        matrix = deterministic_projection_matrix(text_dim, vision_dim).tolist()
        upgrade_notes = {
            "upgraded_from": 1,
            "vision_dim_corrected": {
                "registered": registered_vision_dim,
                "actual": vision_dim,
            },
            "scale_rule": config.scale_rule,
            "scale_percent": int(doc["projection"]["scale_percent"]),
            "projection": "generated_deterministic",
        }

    shape = [len(matrix), len(matrix[0]) if matrix else 0]
    if shape != [text_dim, vision_dim]:
        raise EngineError(
            E_FORMAT, "projection matrix shape does not match modality dims",
            {"matrix_shape": shape, "text_dim": text_dim,
             "vision_dim": vision_dim})

    return {
        "project_id": doc.get("project_id", "align-project"),
        "structure_version": 2,
        "modalities": {"text": {"dim": text_dim},
                       "vision": {"dim": vision_dim}},
        "projection": {"scale": scale, "matrix": matrix,
                       "shape": [text_dim, vision_dim]},
        "shard_refs": {"text": list(shard_refs.get("text", [])),
                       "vision": list(shard_refs.get("vision", []))},
        "upgraded_from": version,
        "upgrade_notes": upgrade_notes,
    }


def export_manifest_v2(project):
    """Re-export an internal project as a version-2 manifest document."""
    if project.projection is None:
        raise EngineError(E_FORMAT, "project has no projection to export", {})
    text_dim = int(project.modalities["text"]["dim"])
    vision_dim = int(project.modalities["vision"]["dim"])
    doc = {
        "manifest_version": 2,
        "project_id": project.project_id,
        "modalities": {"text": {"dim": text_dim},
                       "vision": {"dim": vision_dim}},
        "projection": {
            "scale": float(project.projection["scale"]),
            "matrix": [[float(x) for x in row]
                       for row in project.projection["matrix"]],
        },
        "shards": {
            "text": project.shard_ids("text"),
            "vision": project.shard_ids("vision"),
        },
    }
    return doc


def dump_manifest(doc):
    return yaml.safe_dump(doc, sort_keys=True, allow_unicode=True,
                          default_flow_style=False)
