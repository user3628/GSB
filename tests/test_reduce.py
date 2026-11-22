import numpy as np
import pytest

from xfpipe.crossing import build_crosses
from xfpipe.errors import E_DIM_INVALID, PipelineError
from xfpipe.fingerprint import canonical_json_bytes
from xfpipe.inference import infer_columns, load_csv
from xfpipe.reduce import reduce_features


def _cross(sample_path, config):
    header, rows = load_csv(sample_path)
    return build_crosses(infer_columns(header, rows, config), config)


def test_reduce_is_byte_identical(sample_path, config):
    cross = _cross(sample_path, config)
    first = canonical_json_bytes(reduce_features(cross, config))
    second = canonical_json_bytes(reduce_features(cross, config))
    assert first == second


def test_candidates_and_explained_variance(sample_path, config):
    art = reduce_features(_cross(sample_path, config), config)
    methods = [c["method"] for c in art["candidates"]]
    assert methods == ["pca", "truncated_svd", "random_projection"]
    pca = art["candidates"][0]
    assert abs(sum(pca["explained_variance_ratio"]) <= 1.0 + 1e-9)
    assert all(r >= 0 for r in pca["explained_variance_ratio"])
    rp = art["candidates"][2]
    assert rp["projection_matrix_fingerprint"]
    assert rp["explained_variance_ratio"] is None


def test_component_signs_are_deterministic(sample_path, config):
    cross = _cross(sample_path, config)
    art = reduce_features(cross, config)
    for cand in art["candidates"][:2]:
        comps = np.asarray(cand["components"]["values"], dtype=np.float64)
        for j in range(comps.shape[1]):
            pivot = int(np.argmax(np.abs(comps[:, j])))
            assert comps[pivot, j] > 0


def test_target_dim_exceeding_features_raises_e_dim_invalid(sample_path, config):
    cross = _cross(sample_path, config)
    n_features = len(cross["features"])
    for cand in config["reduction"]["candidates"]:
        cand["target_dim"] = n_features + 5
    with pytest.raises(PipelineError) as exc:
        reduce_features(cross, config)
    assert exc.value.code == E_DIM_INVALID
    assert exc.value.details["actual_dim"] == n_features
    assert exc.value.details["target_dim"] == n_features + 5


def test_error_bound_is_reported(sample_path, config):
    art = reduce_features(_cross(sample_path, config), config)
    for cand in art["candidates"]:
        bound = cand["error_bound"]
        assert bound["max_abs_error"] <= bound["total_bound"] + 1e-9
        assert bound["quantum_bound"] > 0
        assert len(bound["scale_per_column"]) == cand["target_dim"]
