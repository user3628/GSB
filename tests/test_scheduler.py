from fiw.catalog import load_samples
from fiw.models import (
    InjectionItem,
    OUTCOME_QUEUE_FULL,
    OUTCOME_SKIPPED,
    SchedulerConfig,
)
from fiw.scheduler import (
    REASON_BUDGET_EXHAUSTED,
    REASON_DUPLICATE_ENDPOINT,
    REASON_FAULT_DISABLED,
    RoundScheduler,
)


def _scheduler(**overrides):
    cfg = SchedulerConfig(
        global_concurrency=overrides.get("global_concurrency", 2),
        queue_capacity=overrides.get("queue_capacity", 4),
        round_time_budget_ms=overrides.get("round_time_budget_ms", 5000),
        connect_timeout_ms=overrides.get("connect_timeout_ms", 250),
        cost_estimates_ms=overrides.get("cost_estimates_ms", {
            "connection_refused": 20, "connection_timeout": 300,
            "corrupt_body": 30}),
    )
    return RoundScheduler(cfg)


def _item(item_id, target, fault_type, priority, duration=1):
    return InjectionItem(item_id=item_id, target=target, fault_type=fault_type,
                         duration_rounds=duration, priority=priority)


def test_admission_orders_by_priority_then_rotation(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    sched = _scheduler()
    items = [
        _item("low", "ep-web-1", "corrupt_body", 1),
        _item("high", "ep-web-2", "corrupt_body", 9),
        _item("mid", "ep-db-1", "connection_timeout", 5),
    ]
    admitted, skipped, _ = sched.admit(items, catalog, 1,
                                       {ft: True for ft in
                                        ["connection_refused", "connection_timeout",
                                         "corrupt_body"]})
    assert [i.item_id for i in admitted] == ["high", "mid", "low"]
    assert skipped == []


def test_duplicate_endpoint_skipped(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    sched = _scheduler()
    items = [
        _item("first", "ep-web-1", "corrupt_body", 9),
        _item("second", "ep-web-1", "corrupt_body", 1),
    ]
    admitted, skipped, _ = sched.admit(items, catalog, 1,
                                       {"corrupt_body": True})
    assert [i.item_id for i in admitted] == ["first"]
    assert skipped[0]["item_id"] == "second"
    assert skipped[0]["outcome"] == OUTCOME_SKIPPED
    assert skipped[0]["reason"] == REASON_DUPLICATE_ENDPOINT


def test_budget_exhaustion_skips_low_priority(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    sched = _scheduler(round_time_budget_ms=40)
    items = [
        _item("cheap", "ep-cache-1", "connection_refused", 9),
        _item("pricey", "ep-db-1", "connection_timeout", 8),
    ]
    admitted, skipped, remaining = sched.admit(
        items, catalog, 1, {"connection_refused": True,
                            "connection_timeout": True})
    assert [i.item_id for i in admitted] == ["cheap"]
    assert skipped[0]["item_id"] == "pricey"
    assert skipped[0]["reason"] == REASON_BUDGET_EXHAUSTED
    assert remaining <= 20


def test_disabled_fault_type_skipped(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    sched = _scheduler()
    items = [_item("off", "ep-cache-1", "connection_refused", 9)]
    admitted, skipped, _ = sched.admit(items, catalog, 1,
                                       {"connection_refused": False})
    assert admitted == []
    assert skipped[0]["reason"] == REASON_FAULT_DISABLED


def test_queue_full_when_limit_and_queue_exhausted(env):
    catalog, _ = load_samples(str(env.endpoints_path), str(env.plan_path))
    # three endpoints all pointing at the same blackhole port
    sched = _scheduler(global_concurrency=1, queue_capacity=1,
                       connect_timeout_ms=300)
    items = [
        _item("t1", "ep-web-1", "connection_timeout", 9),
        _item("t2", "ep-web-2", "connection_timeout", 8),
        _item("t3", "ep-db-1", "connection_timeout", 7),
    ]
    # point web-1/web-2 at the blackhole so all three really wait
    cat_dict = {e.endpoint_id: e for e in catalog.endpoints}
    bh_port = env.blackhole.port
    endpoints = []
    for ep in catalog.endpoints:
        if ep.endpoint_id in ("ep-web-1", "ep-web-2"):
            ep = type(ep)(**{**ep.__dict__, "port": bh_port})
        endpoints.append(ep)
    from fiw.models import Catalog
    catalog = Catalog(endpoints=tuple(endpoints))
    admitted, _, _ = sched.admit(items, catalog, 1,
                                 {"connection_timeout": True})
    trajectory, _ = sched.execute(admitted, catalog)
    outcomes = {t["item_id"]: t["outcome"] for t in trajectory}
    assert outcomes["t3"] == OUTCOME_QUEUE_FULL
    queue_full = [t for t in trajectory if t["outcome"] == OUTCOME_QUEUE_FULL]
    assert queue_full[0]["code"] == "E_INJECTION_QUEUE_FULL"
