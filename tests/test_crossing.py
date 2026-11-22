import pytest

from xfpipe.crossing import build_crosses
from xfpipe.errors import E_EMPTY_FEATURE, E_INVALID_CONFIG, PipelineError
from xfpipe.fingerprint import canonical_json_bytes
from xfpipe.inference import infer_columns, load_csv
from xfpipe.sample_gen import default_config


def _infer(sample_path, config):
    header, rows = load_csv(sample_path)
    return infer_columns(header, rows, config)


def test_cross_generation_and_truncation(sample_path, config):
    config["crossing"]["max_crosses"] = 3
    art = build_crosses(_infer(sample_path, config), config)
    assert len(art["combinations"]) == 3
    assert art["truncated"], "expected truncated combinations to be recorded"
    for entry in art["truncated"]:
        assert entry["reason"].startswith("exceeds max_crosses cap")
    # Deterministic order: sorted by (order, columns).
    orders = [c["order"] for c in art["combinations"]]
    assert orders == sorted(orders)


def test_cross_is_byte_identical(sample_path, config):
    first = canonical_json_bytes(build_crosses(_infer(sample_path, config), config))
    second = canonical_json_bytes(build_crosses(_infer(sample_path, config), config))
    assert first == second


def test_zero_order_raises_e_invalid_config(sample_path, config):
    config["crossing"]["max_order"] = 0
    with pytest.raises(PipelineError) as exc:
        build_crosses(_infer(sample_path, config), config)
    assert exc.value.code == E_INVALID_CONFIG
    assert "crossing.max_order" in exc.value.details


def test_zero_categorical_columns_raises_e_invalid_config(tmp_path, config):
    import csv

    path = str(tmp_path / "numonly.csv")
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["a", "b"])
        for i in range(20):
            writer.writerow([f"{i}.5", f"{i * 2}.25"])
    header, rows = load_csv(path)
    art = infer_columns(header, rows, config)
    with pytest.raises(PipelineError) as exc:
        build_crosses(art, config)
    assert exc.value.code == E_INVALID_CONFIG
    problems = exc.value.details.get("problems") or exc.value.details
    assert "categorical_columns" in problems


def test_all_cardinality_one_raises_e_empty_feature(tmp_path, config):
    import csv

    path = str(tmp_path / "const.csv")
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["cat1", "cat2", "num"])
        for i in range(10):
            writer.writerow(["same", "only", f"{i}.5"])
    header, rows = load_csv(path)
    art = infer_columns(header, rows, config)
    with pytest.raises(PipelineError) as exc:
        build_crosses(art, config)
    assert exc.value.code == E_EMPTY_FEATURE
    assert set(exc.value.details["columns"]) == {"cat1", "cat2"}


def test_count_matrix_is_fixed_point(sample_path, config):
    art = build_crosses(_infer(sample_path, config), config)
    matrix = art["matrix"]
    assert all(s == 1.0 for s in matrix["scales"])
    n_rows = art["matrix"]["shape"][0]
    assert n_rows == 640
    # Every row has exactly one hit per kept combination.
    combos = art["combinations"]
    offsets = []
    pos = 0
    for c in combos:
        offsets.append((pos, c["feature_count"]))
        pos += c["feature_count"]
    row = matrix["values"][0]
    for start, width in offsets:
        assert sum(row[start:start + width]) == 1
