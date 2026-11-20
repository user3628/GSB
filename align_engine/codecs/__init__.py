"""Bidirectional codecs between the IR and the three on-disk formats."""

from ..errors import E_FORMAT, EngineError
from . import json_project, npy_dir, npz_pack

FORMATS = {
    "json_project": json_project,
    "npz_pack": npz_pack,
    "npy_dir": npy_dir,
}


def write_project(fmt, project, path):
    if fmt not in FORMATS:
        raise EngineError(E_FORMAT, "unknown format", {"format": fmt})
    FORMATS[fmt].write_project(project, path)


def read_project(fmt, path):
    if fmt not in FORMATS:
        raise EngineError(E_FORMAT, "unknown format", {"format": fmt})
    return FORMATS[fmt].read_project(path)
