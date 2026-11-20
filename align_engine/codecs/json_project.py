"""Internal JSON project format: a single canonical JSON file."""

import json

from ..ir import Project
from ..util import atomic_write


def write_project(project, path):
    atomic_write(path, project.canonical_bytes())


def read_project(path):
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    return Project.from_dict(data)
