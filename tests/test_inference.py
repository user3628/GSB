import csv
import os

import pytest

from xfpipe.errors import E_EMPTY_FEATURE, E_NUMERIC, PipelineError
from xfpipe.inference import MISSING_LABEL, infer_columns, load_csv
from xfpipe.sample_gen import default_config


def test_column_types_and_labels(sample_path, config):
    header, rows = load_csv(sample_path)
    art = infer_columns(header, rows, config)
    types = {c["name"]: c["type"] for c in art["columns"]}
    assert types["age"] == "numeric"
    assert types["income"] == "numeric"
    assert types["region"] == "categorical"
    assert types["segment"] == "categorical"
    assert types["channel"] == "categorical"
    assert types["tier"] == "categorical"
    assert types["label"] == "label"
    # Numeric columns carry deterministic bin edges.
    age = next(c for c in art["columns"] if c["name"] == "age")
    assert age["bins"]["count"] == config["inference"]["numeric_bins"]
    assert len(age["bins"]["edges"]) == config["inference"]["numeric_bins"] + 1
    # Missing cells are explicitly labelled.
    assert MISSING_LABEL in art["row_tokens"]["loyalty"]


def test_inference_is_byte_identical(sample_path, config):
    from xfpipe.fingerprint import canonical_json_bytes

    header, rows = load_csv(sample_path)
    first = canonical_json_bytes(infer_columns(header, rows, config))
    second = canonical_json_bytes(infer_columns(header, rows, config))
    assert first == second


def _write_csv(path, header, data_rows):
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(data_rows)


def test_nan_and_inf_raise_e_numeric(tmp_path, config):
    path = str(tmp_path / "bad.csv")
    _write_csv(path, ["x", "cat"], [["1.0", "a"], ["NaN", "b"], ["Inf", "a"]])
    header, rows = load_csv(path)
    with pytest.raises(PipelineError) as exc:
        infer_columns(header, rows, config)
    assert exc.value.code == E_NUMERIC
    assert exc.value.details["column"] == "x"
    assert exc.value.details["rows"] == [1, 2]


def test_overflow_literal_raises_e_numeric(tmp_path, config):
    path = str(tmp_path / "overflow.csv")
    _write_csv(path, ["x"], [["1.0"], ["1e999"]])  # parses to inf
    header, rows = load_csv(path)
    with pytest.raises(PipelineError) as exc:
        infer_columns(header, rows, config)
    assert exc.value.code == E_NUMERIC
    assert exc.value.details["rows"] == [1]


def test_fully_missing_column_raises_e_empty_feature(tmp_path, config):
    path = str(tmp_path / "empty.csv")
    _write_csv(path, ["x", "ghost"], [["1.0", ""], ["2.0", ""], ["3.0", ""]])
    header, rows = load_csv(path)
    with pytest.raises(PipelineError) as exc:
        infer_columns(header, rows, config)
    assert exc.value.code == E_EMPTY_FEATURE
    assert exc.value.details["column"] == "ghost"
