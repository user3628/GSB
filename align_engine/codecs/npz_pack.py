""".npz vector package format.

Single ``.npz`` archive.  Keys:

* ``meta_json``                - uint8 array: project-level JSON (no shards)
* ``shards/<id>/record_json``  - uint8 array: shard record JSON (no vectors)
* ``shards/<id>/vectors_q``    - int32 quantized vector matrix
* ``shards/<id>/scales``       - float64 per-dimension scale factors

The archive is written with fixed zip entry timestamps and a fixed
compression level so identical content produces byte-identical files.
"""

import io
import json
import os
import tempfile
import zipfile

import numpy as np

from ..ir import FORMAT_JSON_PROJECT, FORMAT_VERSION, Project, Shard
from ..util import canonical_json_bytes

_FIXED_DATE = (1980, 1, 1, 0, 0, 0)


def _npy_bytes(array):
    buffer = io.BytesIO()
    np.lib.format.write_array(buffer, np.asarray(array))
    return buffer.getvalue()


def _read_npy_bytes(data):
    return np.lib.format.read_array(io.BytesIO(data), allow_pickle=False)


def write_project(project, path):
    meta = {
        "format": FORMAT_JSON_PROJECT,
        "container": "npz_pack",
        "format_version": FORMAT_VERSION,
        "project_id": project.project_id,
        "structure_version": project.structure_version,
        "modalities": project.modalities,
        "projection": project.projection,
        "meta": project.meta,
        "shard_order": [s.shard_id for s in project.shards],
    }
    arrays = {"meta_json": np.frombuffer(canonical_json_bytes(meta), np.uint8)}
    for shard in project.shards:
        record = shard.to_dict()
        record.pop("vectors_q")
        record.pop("scales")
        prefix = "shards/{}/".format(shard.shard_id)
        arrays[prefix + "record_json"] = np.frombuffer(
            canonical_json_bytes(record), np.uint8)
        arrays[prefix + "vectors_q"] = shard.vectors_q.astype(np.int32)
        arrays[prefix + "scales"] = np.asarray(shard.scales, dtype=np.float64)

    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".npz")
    try:
        with os.fdopen(fd, "wb") as handle:
            with zipfile.ZipFile(handle, "w", compression=zipfile.ZIP_DEFLATED,
                                 compresslevel=6) as archive:
                for name in sorted(arrays):
                    info = zipfile.ZipInfo(name + ".npy", date_time=_FIXED_DATE)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o600 << 16
                    archive.writestr(info, _npy_bytes(arrays[name]))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def read_project(path):
    arrays = {}
    with zipfile.ZipFile(path, "r") as archive:
        for name in archive.namelist():
            if name.endswith(".npy"):
                arrays[name[:-4]] = _read_npy_bytes(archive.read(name))
    meta = json.loads(bytes(arrays.pop("meta_json").tolist()).decode("utf-8"))
    shards = []
    for shard_id in meta["shard_order"]:
        prefix = "shards/{}/".format(shard_id)
        record = json.loads(
            bytes(arrays[prefix + "record_json"].tolist()).decode("utf-8"))
        record["vectors_q"] = arrays[prefix + "vectors_q"].tolist()
        record["scales"] = [float(x) for x in arrays[prefix + "scales"]]
        shards.append(Shard.from_dict(record))
    return Project(project_id=meta["project_id"],
                   structure_version=int(meta["structure_version"]),
                   modalities=meta["modalities"],
                   projection=meta["projection"], shards=shards,
                   meta=meta.get("meta", {}))
