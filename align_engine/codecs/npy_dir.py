""".npy + sidecar JSON directory package format.

Layout::

    <dir>/project.json            project-level record (no vectors)
    <dir>/shards/<id>.npy         int32 quantized vector matrix
    <dir>/shards/<id>.json        sidecar: scales, bits, ids, meta, ...

The directory is assembled in a temporary sibling directory and moved into
place atomically.
"""

import json
import os
import shutil
import tempfile

import numpy as np

from ..ir import FORMAT_JSON_PROJECT, FORMAT_VERSION, Project, Shard
from ..util import atomic_write, canonical_json_bytes


def write_project(project, path):
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(dir=parent, prefix=".tmp-npydir-")
    try:
        os.makedirs(os.path.join(tmp_dir, "shards"))
        index = []
        for shard in project.shards:
            record = shard.to_dict()
            vectors_q = record.pop("vectors_q")
            npy_name = "shards/{}.npy".format(shard.shard_id)
            sidecar_name = "shards/{}.json".format(shard.shard_id)
            buffer_path = os.path.join(tmp_dir, npy_name)
            with open(buffer_path, "wb") as handle:
                np.lib.format.write_array(
                    handle, np.asarray(vectors_q, dtype=np.int32))
            atomic_write(os.path.join(tmp_dir, sidecar_name),
                         canonical_json_bytes(record))
            index.append({"shard_id": shard.shard_id, "npy": npy_name,
                          "sidecar": sidecar_name})
        meta = {
            "format": FORMAT_JSON_PROJECT,
            "container": "npy_dir",
            "format_version": FORMAT_VERSION,
            "project_id": project.project_id,
            "structure_version": project.structure_version,
            "modalities": project.modalities,
            "projection": project.projection,
            "meta": project.meta,
            "shards": index,
        }
        atomic_write(os.path.join(tmp_dir, "project.json"),
                     canonical_json_bytes(meta))
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)
        os.replace(tmp_dir, path)
    except BaseException:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def read_project(path):
    with open(os.path.join(path, "project.json"), "r", encoding="utf-8") as fh:
        meta = json.load(fh)
    shards = []
    for entry in meta["shards"]:
        with open(os.path.join(path, entry["sidecar"]), "r",
                  encoding="utf-8") as fh:
            record = json.load(fh)
        with open(os.path.join(path, entry["npy"]), "rb") as fh:
            vectors_q = np.lib.format.read_array(fh, allow_pickle=False)
        record["vectors_q"] = vectors_q.tolist()
        shards.append(Shard.from_dict(record))
    return Project(project_id=meta["project_id"],
                   structure_version=int(meta["structure_version"]),
                   modalities=meta["modalities"],
                   projection=meta["projection"], shards=shards,
                   meta=meta.get("meta", {}))
