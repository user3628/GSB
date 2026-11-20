"""Workbench configuration: defaults, persistence, per-stage config digests."""

import json
import os

from .canon import digest_obj, digest_file

STAGES = ["features", "projection", "index", "recall"]

DEFAULTS = {
    "seed": 20261120,
    "num_fragments": 6,
    "image_size": 64,
    "grid": 4,
    "orient_bins": 8,
    "bow_bins": 512,
    "ngram_bins": 512,
    "ngram_min": 2,
    "ngram_max": 4,
    "bow_weight": 1.0,
    "ngram_weight": 0.5,
    "embed_dim": 64,
    "proj_seed": 977,
    "w_cosine": 0.7,
    "w_inverted": 0.3,
    "top_k": 3,
}


class Config:
    def __init__(self, **kw):
        vals = dict(DEFAULTS)
        vals.update(kw)
        for key in DEFAULTS:
            setattr(self, key, vals[key])

    # ---- derived, declared-before-extraction feature dimensions ----
    @property
    def block_dim(self):
        return 3 + 3 + self.orient_bins  # mean RGB + std RGB + edge orientation hist

    @property
    def image_dim(self):
        return self.grid * self.grid * self.block_dim

    @property
    def text_dim(self):
        return self.bow_bins + self.ngram_bins

    def to_dict(self):
        d = {k: getattr(self, k) for k in DEFAULTS}
        d["image_dim"] = self.image_dim
        d["text_dim"] = self.text_dim
        return d

    def save(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(self.to_dict(), fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")

    @classmethod
    def load(cls, path):
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        known = {k: v for k, v in data.items() if k in DEFAULTS}
        return cls(**known)

    # ---- per-stage relevant config components (for fingerprinting) ----
    def component_digests(self, projection_path):
        proj_bytes_digest = digest_file(projection_path) if os.path.exists(projection_path) else "MISSING"
        return {
            "features": digest_obj({
                "grid": self.grid,
                "orient_bins": self.orient_bins,
                "bow_bins": self.bow_bins,
                "ngram_bins": self.ngram_bins,
                "ngram_min": self.ngram_min,
                "ngram_max": self.ngram_max,
                "bow_weight": self.bow_weight,
                "ngram_weight": self.ngram_weight,
                "image_dim": self.image_dim,
                "text_dim": self.text_dim,
            }),
            "projection": digest_obj({
                "embed_dim": self.embed_dim,
                "projection_file_sha256": proj_bytes_digest,
            }),
            "index": digest_obj({"index_params": "v1"}),
            "recall": digest_obj({
                "w_cosine": self.w_cosine,
                "w_inverted": self.w_inverted,
                "top_k": self.top_k,
            }),
        }
