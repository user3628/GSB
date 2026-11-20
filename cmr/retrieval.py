"""Bidirectional retrieval: text->image and image->text with score breakdown."""

from .errors import CmrError, E_QUERY_INVALID
from .image_features import block_hashes, extract_image_features, load_rgb, validate_image_vector
from .projection import project_and_normalize
from .text_features import extract_text_features, normalize_text, term_weights


def _validate_k(k, num_candidates):
    if not isinstance(k, int) or k < 1 or k > num_candidates:
        raise CmrError(
            E_QUERY_INVALID,
            f"k={k!r} out of range: index holds {num_candidates} candidates",
            stage="query",
            detail={"k": k, "num_candidates": num_candidates},
        )


def _rank(candidates, cfg, k):
    """Deterministic ranking: score desc, then (shard, vec) asc."""
    ranked = sorted(candidates, key=lambda r: (-r["score"], r["shard"], r["vec"]))
    for rank, item in enumerate(ranked[:k], start=1):
        item["rank"] = rank
    return ranked[:k]


def _cosine(qvec, mat, rows):
    sub = mat[rows]
    return sub @ qvec  # both sides unit-normalized


def query_text(index_obj, mat, cfg, projections, text, k, stage="query"):
    """Text query -> top-k image shards with full score breakdown."""
    norm = normalize_text(text)
    if not norm:
        raise CmrError(E_QUERY_INVALID, "query text is empty after normalization", stage=stage)
    image_rows = [i for i, a in enumerate(index_obj["addressing"]) if a["modality"] == "image"]
    _validate_k(k, len(image_rows))

    feat = extract_text_features(norm, cfg)
    qvec = project_and_normalize(projections["text"], feat, stage=stage)
    cos = _cosine(qvec, mat, image_rows)

    bow, ngram = term_weights(norm, cfg)
    query_terms = [("b:" + t, w) for t, w in bow.items()]
    query_terms += [("g:" + g, w) for g, w in ngram.items()]
    total_qw = sum(w for _, w in query_terms)

    results = []
    for row_pos, addr_pos in enumerate(image_rows):
        addr = index_obj["addressing"][addr_pos]
        shard = addr["shard"]
        hits = []
        inv_sum = 0.0
        for term, qw in query_terms:
            for posting in index_obj["text_inverted"].get(term, []):
                if posting["shard"] == shard:
                    contrib = qw * posting["weight"]
                    inv_sum += contrib
                    hits.append({"term": term, "query_weight": qw,
                                 "posting_weight": posting["weight"],
                                 "contribution": contrib})
        hits.sort(key=lambda h: h["term"])
        inv_norm = inv_sum / total_qw if total_qw > 0 else 0.0
        cosine = float(cos[row_pos])
        score = cfg.w_cosine * cosine + cfg.w_inverted * inv_norm
        results.append({
            "shard": shard,
            "vec": 0,
            "pair_id": addr["pair_id"],
            "image_file": addr["image_file"],
            "score": score,
            "cosine": cosine,
            "inverted": {"weight": inv_norm, "hits": hits},
            "source": {"shard": shard, "vec": 0},
        })
    ranked = _rank(results, cfg, k)
    return {
        "query": {"type": "text", "text": norm, "k": k},
        "weights": {"w_cosine": cfg.w_cosine, "w_inverted": cfg.w_inverted},
        "num_candidates": len(image_rows),
        "results": ranked,
    }


def query_image_vector(index_obj, mat, cfg, projections, img_vec, k, stage="query"):
    """Raw image feature vector -> top-k captions (cosine-only; no visual terms)."""
    img_vec = validate_image_vector(img_vec, cfg, stage=stage)
    text_rows = [i for i, a in enumerate(index_obj["addressing"]) if a["modality"] == "text"]
    _validate_k(k, len(text_rows))

    qvec = project_and_normalize(projections["image"], img_vec, stage=stage)
    cos = _cosine(qvec, mat, text_rows)
    results = []
    for row_pos, addr_pos in enumerate(text_rows):
        addr = index_obj["addressing"][addr_pos]
        cosine = float(cos[row_pos])
        results.append({
            "shard": addr["shard"],
            "vec": 1,
            "pair_id": addr["pair_id"],
            "caption": addr["caption"],
            "score": cfg.w_cosine * cosine,
            "cosine": cosine,
            "inverted": {"weight": 0.0, "hits": []},
            "source": {"shard": addr["shard"], "vec": 1},
        })
    ranked = _rank(results, cfg, k)
    return {
        "query": {"type": "image_vector", "k": k},
        "weights": {"w_cosine": cfg.w_cosine, "w_inverted": cfg.w_inverted},
        "num_candidates": len(text_rows),
        "results": ranked,
    }


def query_image(index_obj, mat, cfg, projections, image_path, k, data_dir, stage="query"):
    """Image query (PNG path or 'pair:<id>') -> top-k captions."""
    text_rows = [i for i, a in enumerate(index_obj["addressing"]) if a["modality"] == "text"]
    _validate_k(k, len(text_rows))

    if image_path.startswith("pair:"):
        pair_id = image_path.split(":", 1)[1]
        row = next((i for i, a in enumerate(index_obj["addressing"])
                    if a["modality"] == "image" and a["pair_id"] == pair_id), None)
        if row is None:
            raise CmrError(E_QUERY_INVALID, f"unknown pair_id '{pair_id}'", stage=stage)
        qvec = mat[row]
        img = load_rgb(__import__("os").path.join(
            data_dir, index_obj["addressing"][row]["image_file"]), stage=stage)
    else:
        feat = extract_image_features(image_path, cfg, stage=stage)
        qvec = project_and_normalize(projections["image"], feat, stage=stage)
        img = load_rgb(image_path, stage=stage)

    qterms = block_hashes(img, cfg.grid, cfg.orient_bins)
    total_qw = float(len(qterms))
    text_rows = [i for i, a in enumerate(index_obj["addressing"]) if a["modality"] == "text"]
    cos = mat[text_rows] @ qvec

    results = []
    for row_pos, addr_pos in enumerate(text_rows):
        addr = index_obj["addressing"][addr_pos]
        shard = addr["shard"]
        hits = []
        inv_sum = 0.0
        for term in qterms:
            for posting in index_obj["visual_inverted"].get(term, []):
                if posting["shard"] == shard:
                    contrib = 1.0 * posting["weight"]
                    inv_sum += contrib
                    hits.append({"term": term, "query_weight": 1.0,
                                 "posting_weight": posting["weight"],
                                 "contribution": contrib})
        hits.sort(key=lambda h: h["term"])
        inv_norm = inv_sum / total_qw if total_qw > 0 else 0.0
        cosine = float(cos[row_pos])
        score = cfg.w_cosine * cosine + cfg.w_inverted * inv_norm
        results.append({
            "shard": shard,
            "vec": 1,
            "pair_id": addr["pair_id"],
            "caption": addr["caption"],
            "score": score,
            "cosine": cosine,
            "inverted": {"weight": inv_norm, "hits": hits},
            "source": {"shard": shard, "vec": 1},
        })
    ranked = _rank(results, cfg, k)
    return {
        "query": {"type": "image", "image": image_path, "k": k},
        "weights": {"w_cosine": cfg.w_cosine, "w_inverted": cfg.w_inverted},
        "num_candidates": len(text_rows),
        "results": ranked,
    }
