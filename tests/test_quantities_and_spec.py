from __future__ import annotations

import copy
import json

import pytest

from pdb_engine.errors import BusinessError
from pdb_engine.quantities import parse_cpu_millicores, parse_memory_bytes
from pdb_engine.scenario import parse_scenario_data


def test_quantity_parsing_examples() -> None:
    assert parse_cpu_millicores("500m") == 500
    assert parse_cpu_millicores("2") == 2000
    assert parse_cpu_millicores("0.25") == 250
    assert parse_memory_bytes("512Mi") == 512 * 1024**2
    assert parse_memory_bytes("1Gi") == 1024**3
    assert parse_memory_bytes("1024") == 1024


@pytest.mark.parametrize(
    "parser,value",
    [
        (parse_cpu_millicores, "1.5m"),
        (parse_cpu_millicores, "abc"),
        (parse_memory_bytes, "12Xi"),
        (parse_memory_bytes, "-1Mi"),
        (parse_memory_bytes, "9223372036854775808"),
    ],
)
def test_quantity_rejects_invalid_or_overflow(parser, value) -> None:
    with pytest.raises(BusinessError) as exc:
        parser(value)
    assert exc.value.code == "E_SPEC_INVALID"


def test_pdb_percentage_rounding_directions(sample_data: dict) -> None:
    data = copy.deepcopy(sample_data)
    data["workloads"][0]["desired_replicas"] = 3
    scenario = parse_scenario_data(data)
    min_pdb = scenario.pdbs["web-a-budget"]
    max_pdb = scenario.pdbs["web-critical-budget"]

    assert min_pdb.rounding == "ceil"
    assert min_pdb.calculated_integer == 2
    assert min_pdb.required_available == 2
    assert max_pdb.rounding == "floor"
    assert max_pdb.calculated_integer == 1
    assert max_pdb.max_unavailable == 1
    assert max_pdb.required_available == 2
    assert min_pdb.semantics()["budget"]["rounding"] == "ceil"
    assert max_pdb.semantics()["budget"]["rounding"] == "floor"


def test_health_excludes_non_running_evicting_and_terminating(sample_data: dict) -> None:
    data = copy.deepcopy(sample_data)
    web_pods = data["workloads"][0]["pods"]
    web_pods[1]["phase"] = "Pending"  # alpha: affects web-a-budget only
    web_pods[2]["eviction_state"] = "evicting"  # beta: affects critical budget only
    scenario = parse_scenario_data(data)

    assert scenario.pdbs["web-a-budget"].initial_available == 2
    assert scenario.pdbs["web-critical-budget"].initial_available == 2


def test_selector_empty_is_an_error(sample_data: dict) -> None:
    data = copy.deepcopy(sample_data)
    data["pdbs"][0]["selector"] = {"no-such-label": "missing"}
    with pytest.raises(BusinessError) as exc:
        parse_scenario_data(data)
    assert exc.value.code == "E_SELECTOR_EMPTY"
    assert exc.value.details["pdb"] == "web-a-budget"


def test_pdb_requires_exactly_one_budget(sample_data: dict) -> None:
    data = copy.deepcopy(sample_data)
    data["pdbs"][0]["maxUnavailable"] = "25%"
    with pytest.raises(BusinessError) as exc:
        parse_scenario_data(data)
    assert exc.value.code == "E_SPEC_INVALID"


def test_invalid_json_is_e_spec_invalid(tmp_path) -> None:
    from pdb_engine.scenario import parse_scenario_text

    with pytest.raises(BusinessError) as exc:
        parse_scenario_text("{not-json")
    assert exc.value.code == "E_SPEC_INVALID"

