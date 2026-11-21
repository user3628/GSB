import json

import pytest

from fiw.catalog import load_samples, topo_order
from fiw.errors import BizError, E_PLAN_INVALID


def test_valid_samples_load(env):
    catalog, plan = load_samples(str(env.endpoints_path), str(env.plan_path))
    assert len(catalog.endpoints) == 5
    assert plan.plan_id == "plan-test"
    assert [i.item_id for i in plan.items] == ["it-1", "it-2", "it-4", "it-3"]


def test_topo_order_respects_upstreams(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    order = topo_order(catalog)
    assert order.index("ep-web-1") < order.index("ep-api-1")
    assert order.index("ep-db-1") < order.index("ep-api-1")


def test_unknown_upstream_rejected(env, tmp_path):
    data = env.catalog_dict()
    data["endpoints"][4]["upstreams"] = ["ep-ghost"]
    env.write_catalog(data)
    with pytest.raises(BizError) as exc:
        load_samples(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID


def test_dependency_cycle_rejected(env):
    data = env.catalog_dict()
    data["endpoints"][0]["upstreams"] = ["ep-api-1"]  # web -> api -> web
    env.write_catalog(data)
    with pytest.raises(BizError) as exc:
        load_samples(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID
    assert "cycle" in exc.value.message


def test_unknown_item_target_rejected(env):
    plan = env.plan_dict()
    plan["items"][0]["target"] = "ep-ghost"
    env.write_plan(plan)
    with pytest.raises(BizError) as exc:
        load_samples(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID


def test_malformed_json_rejected(env):
    env.plan_path.write_text("{not json")
    with pytest.raises(BizError) as exc:
        load_samples(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID


def test_invalid_weight_rejected(env):
    data = env.catalog_dict()
    data["endpoints"][0]["weight"] = 1.5
    env.write_catalog(data)
    with pytest.raises(BizError) as exc:
        load_samples(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID


def test_version_monotonic_replace(env, engine):
    result = engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    assert result["loaded"] is True

    again = engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    assert again["loaded"] is False  # same version: idempotent no-op

    older = env.plan_dict()
    older["version"] = 1
    newer = env.plan_dict()
    newer["version"] = 2
    env.write_plan(newer)
    replaced = engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    assert replaced["loaded"] is True and replaced["replaced"] is True
    assert engine.get_plan("plan-test")["version"] == 2

    env.write_plan(older)  # version 1 < stored 2
    with pytest.raises(BizError) as exc:
        engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    assert exc.value.code == E_PLAN_INVALID


def test_replace_wipes_previous_rounds(env, engine):
    engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    engine.transition("plan-test", "RUNNING")
    engine.run_round("plan-test", 1)
    assert engine.get_round("plan-test", 1)["round_no"] == 1

    newer = env.plan_dict()
    newer["version"] = 2
    env.write_plan(newer)
    engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    with pytest.raises(BizError):
        engine.get_round("plan-test", 1)


def test_failed_load_leaves_no_partial_state(env, engine):
    bad = env.catalog_dict()
    bad["endpoints"][1]["upstreams"] = ["ep-ghost"]
    env.write_catalog(bad)
    with pytest.raises(BizError):
        engine.load_plan(str(env.endpoints_path), str(env.plan_path))
    assert engine.store.get_plan("plan-test") is None
    assert engine.store.pending_intents() == []
