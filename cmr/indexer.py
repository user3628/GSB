"""Cross-modal inverted indexes + dense vector index construction."""

import os

import numpy as np

from .canon import digest_obj, digest_file
from .checkpoint import atomic_write_json
from .errors import CmrError, E_MISSING_FRAGMENT


def build_index(store, cfg, pairs, data_dir):
    """Build text/visual inverted tables and the dense vector index.

    Every posting records weight + source shard. Ordering is canonical
    (terms sorted, postings sorted by (shard, vec)) so the artifact is
    byte-identical for identical inputs and config.
    """
    # Missing fragments must fail the index build, never be skipped.
    for pair in pairs:
        img_path = os.path.join(data_dir, pair["image_file"])
        if not os.path.exists(img_path):
            raise CmrError(E_MISSING_FRAGMENT,
                           f"image fragment not found: {pair['image_file']}",
                           stage="index",
                           detail={"pair_id": pair["pair_id"], "image_file": pair["image_file"]})

    text_inverted = {}
    visual_inverted = {}
    addressing = []
    vectors = []
    shard_digests = {}

    for pair in pairs:
        shard = pair["pair_id"]
        shard_dir = store.path("shards", shard)
        terms = _read_json(os.path.join(shard_dir, "features_terms.json"))
        proj = np.load(os.path.join(shard_dir, "projected.npz"))
        img_vec = proj["img_vec"]
        txt_vec = proj["txt_vec"]

        for term, weight in terms["bow"].items():
            text_inverted.setdefault("b:" + term, []).append(
                {"shard": shard, "vec": 1, "weight": weight})
        for gram, weight in terms["ngram"].items():
            text_inverted.setdefault("g:" + gram, []).append(
                {"shard": shard, "vec": 1, "weight": weight})
        for term in terms["blocks"]:
            visual_inverted.setdefault(term, []).append(
                {"shard": shard, "vec": 0, "weight": 1.0})

        base = len(addressing)
        addressing.append({"shard": shard, "vec": 0, "modality": "image",
                           "pair_id": shard, "image_file": pair["image_file"]})
        addressing.append({"shard": shard, "vec": 1, "modality": "text",
                           "pair_id": shard, "caption": pair["caption"]})
        vectors.append(img_vec)
        vectors.append(txt_vec)
        shard_digests[shard] = digest_obj({
            "terms": terms,
            "img_vec": img_vec.tolist(),
            "txt_vec": txt_vec.tolist(),
            "address_base": base,
        })

    text_inverted = {t: sorted(ps, key=lambda p: (p["shard"], p["vec"]))
                     for t, ps in sorted(text_inverted.items())}
    visual_inverted = {t: sorted(ps, key=lambda p: (p["shard"], p["vec"]))
                       for t, ps in sorted(visual_inverted.items())}

    mat = np.stack(vectors, axis=0).astype(np.float64)
    vec_path = store.path("index", "vectors.npy")
    os.makedirs(os.path.dirname(vec_path), exist_ok=True)
    with open(vec_path, "wb") as fh:
        np.save(fh, mat)

    index_obj = {
        "meta": {
            "num_shards": len(pairs),
            "num_vectors": len(addressing),
            "embed_dim": cfg.embed_dim,
            "image_dim": cfg.image_dim,
            "text_dim": cfg.text_dim,
            "num_text_terms": len(text_inverted),
            "num_visual_terms": len(visual_inverted),
        },
        "addressing": addressing,
        "text_inverted": text_inverted,
        "visual_inverted": visual_inverted,
    }
    index_obj["meta"]["vectors_sha256"] = digest_file(vec_path)
    index_obj["meta"]["index_digest"] = digest_obj({
        "addressing": addressing,
        "text_inverted": text_inverted,
        "visual_inverted": visual_inverted,
        "vectors_sha256": index_obj["meta"]["vectors_sha256"],
    })
    atomic_write_json(store.path("index", "index.json"), index_obj)
    return index_obj, shard_digests


def load_index(store):
    import json

    with open(store.path("index", "index.json"), "r", encoding="utf-8") as fh:
        index_obj = json.load(fh)
    mat = np.load(store.path("index", "vectors.npy"))
    return index_obj, mat


def _read_json(path):
    import json

    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)
