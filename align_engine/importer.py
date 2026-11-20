"""Import sample shards + manifests into the internal JSON project format.

Sample shard layouts (produced by sample_gen.py):

* text shards:   ``<shards_dir>/<shard_id>.npz`` with arrays
  ``ids``, ``vectors`` (float, N x 64) and ``languages``;
* vision shards: ``<shards_dir>/<shard_id>.npy`` (float, N x 96) plus a
  ``<shard_id>.json`` sidecar with ``ids``, ``text_ids``, ``operators``
  and the declared ``dim``.
"""

import json
import os

import numpy as np

from .config import EngineConfig
from .errors import E_FORMAT, E_NOT_FOUND, EngineError
from .ir import Project, Shard
from .manifest import load_manifest_doc, manifest_to_spec
from .util import atomic_write, fingerprint_obj


def load_text_shard(shards_dir, shard_id, bits, structure_version):
    path = os.path.join(shards_dir, shard_id + ".npz")
    if not os.path.exists(path):
        raise EngineError(E_NOT_FOUND, "text shard file missing",
                          {"shard_id": shard_id, "path": path})
    with np.load(path, allow_pickle=False) as pack:
        ids = [str(x) for x in pack["ids"]]
        vectors = np.asarray(pack["vectors"], dtype=np.float64)
        languages = [str(x) for x in pack["languages"]]
    return Shard.from_vectors(
        shard_id, "text", ids, vectors, bits,
        {"languages": languages}, structure_version=structure_version)


def load_vision_shard(shards_dir, shard_id, bits, structure_version):
    npy_path = os.path.join(shards_dir, shard_id + ".npy")
    sidecar_path = os.path.join(shards_dir, shard_id + ".json")
    for path in (npy_path, sidecar_path):
        if not os.path.exists(path):
            raise EngineError(E_NOT_FOUND, "vision shard file missing",
                              {"shard_id": shard_id, "path": path})
    vectors = np.load(npy_path, allow_pickle=False)
    with open(sidecar_path, "r", encoding="utf-8") as handle:
        sidecar = json.load(handle)
    ids = [str(x) for x in sidecar["ids"]]
    meta = {
        "text_ids": [str(x) for x in sidecar["text_ids"]],
        "operators": list(sidecar.get("operators", [])),
        "sidecar_declared_dim": int(sidecar.get("dim", vectors.shape[1])),
    }
    return Shard.from_vectors(
        shard_id, "vision", ids, np.asarray(vectors, dtype=np.float64), bits,
        meta, structure_version=structure_version)


def discover_extra_shards(extra_dir):
    """Vision shards injected for anomaly testing: <id>.npy + <id>.json."""
    found = []
    if not extra_dir or not os.path.isdir(extra_dir):
        return found
    for name in sorted(os.listdir(extra_dir)):
        if name.endswith(".npy"):
            shard_id = name[:-4]
            if os.path.exists(os.path.join(extra_dir, shard_id + ".json")):
                found.append(shard_id)
    return found


def import_project(workspace, manifest_path, shards_dir, extra_shards_dir=None):
    """Import manifest + shards; write project.json + import_meta.json."""
    config = EngineConfig(_load_config_data(workspace))
    doc = load_manifest_doc(manifest_path)
    spec = manifest_to_spec(doc, config)
    structure_version = spec["structure_version"]

    shards = []
    for shard_id in spec["shard_refs"]["text"]:
        shards.append(load_text_shard(shards_dir, shard_id, config.bits,
                                      structure_version))
    vision_ids = list(spec["shard_refs"]["vision"])
    vision_ids += [sid for sid in discover_extra_shards(extra_shards_dir)
                   if sid not in vision_ids]
    for shard_id in vision_ids:
        directory = (extra_shards_dir
                     if extra_shards_dir and os.path.exists(
                         os.path.join(extra_shards_dir, shard_id + ".npy"))
                     and not os.path.exists(
                         os.path.join(shards_dir, shard_id + ".npy"))
                     else shards_dir)
        shards.append(load_vision_shard(directory, shard_id, config.bits,
                                        structure_version))

    meta = {
        "upgraded_from": spec["upgraded_from"],
        "source_manifest": os.path.basename(manifest_path),
    }
    if spec["upgrade_notes"]:
        meta["upgrade"] = spec["upgrade_notes"]
    project = Project(project_id=spec["project_id"],
                      structure_version=structure_version,
                      modalities=spec["modalities"],
                      projection=spec["projection"],
                      shards=shards, meta=meta)

    project_dir = os.path.join(workspace, "project")
    os.makedirs(project_dir, exist_ok=True)
    atomic_write(os.path.join(project_dir, "project.json"),
                 project.canonical_bytes())
    import_meta = {
        "manifest_path": os.path.abspath(manifest_path),
        "shards_dir": os.path.abspath(shards_dir),
        "upgraded_from": spec["upgraded_from"],
        "structure_fingerprint": structure_fingerprint(project),
        "dims_fingerprint": dims_fingerprint(project),
        "projection_fingerprint": projection_fingerprint(project),
        "shard_fingerprints": {s.shard_id: s.fingerprint()
                               for s in project.shards},
    }
    atomic_write(os.path.join(project_dir, "import_meta.json"),
                 _canonical(import_meta))
    if not os.path.exists(os.path.join(workspace, "config.json")):
        atomic_write(os.path.join(workspace, "config.json"),
                     _canonical(EngineConfig().to_dict()))
    return project, import_meta


def structure_fingerprint(project):
    payload = {
        "structure_version": project.structure_version,
        "modalities": project.modalities,
        "projection": project.projection,
    }
    return fingerprint_obj("structure", _canonical(payload))


def dims_fingerprint(project):
    shape = (project.projection["shape"] if project.projection else None)
    payload = {"modalities": project.modalities, "projection_shape": shape}
    return fingerprint_obj("dims", _canonical(payload))


def projection_fingerprint(project):
    return fingerprint_obj("projection", _canonical(project.projection))


def _canonical(obj):
    from .util import canonical_json_bytes
    return canonical_json_bytes(obj)


def _load_config_data(workspace):
    path = os.path.join(workspace, "config.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    return None
