"""Unified intermediate representation (IR) of an alignment project.

A project is described by: project id, structure version, modality registry
(name -> declared dimension), an optional cross-modal projection (float
scale + projection matrix) and a list of shards.  Each shard carries: shard
id, modality, quantized vector matrix, dimension, quantization bit-width,
per-dimension scale factors, row ids and modality metadata.
"""

from dataclasses import dataclass, field

import numpy as np

from . import quantize as quant
from .errors import E_FORMAT, EngineError
from .util import canonical_json_bytes, fingerprint_obj

FORMAT_JSON_PROJECT = "align-project-json"
FORMAT_VERSION = 1


@dataclass
class Shard:
    shard_id: str
    modality: str
    dim: int
    ids: list
    bits: int
    scales: list
    vectors_q: np.ndarray
    meta: dict
    structure_version: int = 2

    @classmethod
    def from_vectors(cls, shard_id, modality, ids, vectors, bits, meta,
                     structure_version=2):
        matrix = np.asarray(vectors, dtype=np.float64)
        if matrix.ndim != 2:
            raise EngineError(E_FORMAT, "shard vectors must be 2-D",
                              {"shard_id": shard_id})
        if matrix.shape[0] != len(ids):
            raise EngineError(E_FORMAT, "row count does not match ids",
                              {"shard_id": shard_id,
                               "rows": int(matrix.shape[0]),
                               "ids": len(ids)})
        vectors_q, scales = quant.quantize(matrix, bits)
        return cls(shard_id=shard_id, modality=modality,
                   dim=int(matrix.shape[1]), ids=list(ids), bits=int(bits),
                   scales=scales, vectors_q=vectors_q, meta=dict(meta),
                   structure_version=int(structure_version))

    def dequantize(self):
        return quant.dequantize(self.vectors_q, self.scales)

    def to_dict(self):
        return {
            "shard_id": self.shard_id,
            "modality": self.modality,
            "dim": self.dim,
            "count": len(self.ids),
            "ids": list(self.ids),
            "bits": self.bits,
            "scales": list(self.scales),
            "vectors_q": self.vectors_q.tolist(),
            "meta": self.meta,
            "structure_version": self.structure_version,
        }

    @classmethod
    def from_dict(cls, data):
        try:
            vectors_q = np.asarray(data["vectors_q"], dtype=np.int32)
        except KeyError as exc:
            raise EngineError(E_FORMAT, "shard record missing field",
                              {"field": str(exc)})
        shard = cls(shard_id=data["shard_id"], modality=data["modality"],
                    dim=int(data["dim"]), ids=list(data["ids"]),
                    bits=int(data["bits"]), scales=list(data["scales"]),
                    vectors_q=vectors_q, meta=dict(data.get("meta", {})),
                    structure_version=int(data.get("structure_version", 2)))
        shard._validate()
        return shard

    def _validate(self):
        rows, cols = self.vectors_q.shape
        if cols != self.dim or len(self.scales) != self.dim:
            raise EngineError(
                E_FORMAT, "shard dimension inconsistent with scales/matrix",
                {"shard_id": self.shard_id, "dim": self.dim,
                 "matrix_cols": int(cols), "scales": len(self.scales)})
        if rows != len(self.ids) or int(self.__dict__.get("count", rows)) != rows:
            raise EngineError(
                E_FORMAT, "shard row count inconsistent with ids",
                {"shard_id": self.shard_id, "rows": int(rows),
                 "ids": len(self.ids)})

    def fingerprint(self):
        return fingerprint_obj(canonical_json_bytes(self.to_dict()))


@dataclass
class Project:
    project_id: str
    structure_version: int
    modalities: dict
    projection: dict | None
    shards: list
    meta: dict = field(default_factory=dict)

    def shard(self, shard_id):
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise EngineError(E_FORMAT, "unknown shard id",
                          {"shard_id": shard_id})

    def shard_ids(self, modality=None):
        return sorted(s.shard_id for s in self.shards
                      if modality is None or s.modality == modality)

    def to_dict(self):
        return {
            "format": FORMAT_JSON_PROJECT,
            "format_version": FORMAT_VERSION,
            "project_id": self.project_id,
            "structure_version": self.structure_version,
            "modalities": self.modalities,
            "projection": self.projection,
            "shards": [s.to_dict() for s in self.shards],
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, data):
        if data.get("format") != FORMAT_JSON_PROJECT:
            raise EngineError(E_FORMAT, "not an internal JSON project",
                              {"format": data.get("format")})
        projection = data.get("projection")
        if projection is not None:
            matrix = projection["matrix"]
            shape = [len(matrix), len(matrix[0]) if matrix else 0]
            if list(projection["shape"]) != shape:
                raise EngineError(
                    E_FORMAT, "projection matrix shape mismatch",
                    {"declared": projection["shape"], "actual": shape})
        shards = [Shard.from_dict(s) for s in data.get("shards", [])]
        seen = set()
        for shard in shards:
            if shard.shard_id in seen:
                raise EngineError(E_FORMAT, "duplicate shard id",
                                  {"shard_id": shard.shard_id})
            seen.add(shard.shard_id)
        return cls(project_id=data["project_id"],
                   structure_version=int(data["structure_version"]),
                   modalities=dict(data["modalities"]),
                   projection=projection, shards=shards,
                   meta=dict(data.get("meta", {})))

    def canonical_bytes(self):
        return canonical_json_bytes(self.to_dict())

    def fingerprint(self):
        return fingerprint_obj(self.canonical_bytes())
