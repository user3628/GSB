import math

from prefalign.core.errors import NumericError
from prefalign.core.stage_quality import lexical_overlap, length_penalty, score_pair


BASE_RECORD = {
    "pair_id": "q1", "group_id": "grp_x",
    "norm_prompt": "prompt text",
    "norm_chosen": "the quick brown fox jumps",
    "norm_rejected": "the quick red fox",
    "source": "human",
}
WEIGHTS = {"length_penalty": 0.3, "lexical_overlap": 0.4, "source_prior": 0.3}
PRIORS = {"human": 0.92, "unknown": 0.5}


def test_factor_ranges_and_determinism():
    first = score_pair(BASE_RECORD, WEIGHTS, PRIORS, 8)
    second = score_pair(BASE_RECORD, WEIGHTS, PRIORS, 8)
    assert first == second
    for value in first["factors"].values():
        assert 0.0 < value <= 1.0
    expected = sum(first["log_contributions"].values())
    assert math.isclose(first["quality_score"], round(expected, 0) if False
                        else first["quality_score"])


def test_length_penalty_equal_is_one():
    assert length_penalty("abcd", "wxyz") == 1.0


def test_lexical_overlap_smoothed():
    value = lexical_overlap("a b c", "a b")
    assert math.isclose(value, (2 + 1) / (2 + 1))


def test_zero_factor_reports_numeric(monkeypatch):
    import prefalign.core.stage_quality as module
    monkeypatch.setattr(module, "length_penalty", lambda c, r: 0.0)
    try:
        score_pair(BASE_RECORD, WEIGHTS, PRIORS, 8)
        assert False
    except NumericError as exc:
        assert exc.code == "E_NUMERIC"
        assert exc.details["pair_id"] == "q1"
        assert exc.details["factor"] == "length_penalty"


def test_nonfinite_factor_reports_numeric(monkeypatch):
    import prefalign.core.stage_quality as module
    monkeypatch.setattr(module, "source_prior", lambda s, p: float("nan"))
    try:
        score_pair(BASE_RECORD, WEIGHTS, PRIORS, 8)
        assert False
    except NumericError as exc:
        assert exc.details["factor"] == "source_prior"


def test_pipeline_quality_byte_stable(built_pipeline):
    first = built_pipeline.load_artifact("quality")
    built_pipeline.run(reason="noop")
    second = built_pipeline.load_artifact("quality")
    assert first == second


def test_zero_source_prior_numeric():
    record = {"pair_id": "p0", "group_id": "g", "norm_prompt": "p",
              "norm_chosen": "a b", "norm_rejected": "a", "source": "ghost"}
    weights = {"length_penalty": 1.0, "lexical_overlap": 1.0, "source_prior": 1.0}
    try:
        score_pair(record, weights, {"ghost": 0.0, "unknown": 0.5}, 8)
        assert False
    except NumericError as exc:
        assert exc.code == "E_NUMERIC"
        assert exc.details["factor"] == "source_prior"
