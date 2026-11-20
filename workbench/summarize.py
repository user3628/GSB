"""Stage 4: summarize pipeline outputs into a fixed, digestible artifact."""

from __future__ import annotations

import numpy as np

from .util import canonical_json, sha256_bytes


def summarize_stage(infer_art, encode_art, matrix_bytes, select_art, sample_fp):
    kept_names = [k["feature"] for k in select_art["kept"]]
    kept_set = set(kept_names)
    kept_idx = [
        i for i, c in enumerate(encode_art["columns"]) if c["name"] in kept_set
    ]
    X = np.frombuffer(matrix_bytes, dtype="<f8").reshape(
        encode_art["rows"], encode_art["cols"]
    )
    final_matrix = np.ascontiguousarray(X[:, kept_idx])
    final_bytes = final_matrix.tobytes(order="C")
    final_columns = [encode_art["columns"][i] for i in kept_idx]
    artifact = {
        "stage": "summarize",
        "sample_fp": sample_fp,
        "rows": encode_art["rows"],
        "encoded_features": encode_art["cols"],
        "selected_features": len(kept_idx),
        "stages": {
            "infer": {"rows": infer_art["rows"], "counts": infer_art["counts"]},
            "encode": {
                "cols": encode_art["cols"],
                "matrix_sha256": encode_art["matrix_sha256"],
                "stats": encode_art["stats"],
            },
            "select": {
                "method": select_art["method"],
                "kept": select_art["kept_count"],
                "dropped": select_art["dropped_count"],
            },
        },
        "final_columns": final_columns,
        "final_matrix_sha256": sha256_bytes(final_bytes),
        "final_matrix_nbytes": len(final_bytes),
        "kept_features": select_art["kept"],
        "dropped_features": select_art["dropped"],
    }
    artifact["digest"] = sha256_bytes(canonical_json(artifact))
    return artifact
