import os

import pytest

from pkq.store import Store


@pytest.fixture()
def store(tmp_path):
    return Store(str(tmp_path / "pkq.db"))


def _mk_session(store, sid="s1"):
    store.create_session(sid, "q-" + sid, "fp", [1, 2], ["pk", "val_a"], 4, 2, "MATERIALIZING")
    store.insert_tasks(sid, [{"task_id": "t0", "pk_lo": 0, "pk_hi": 9, "blocks": [0],
                              "row_lo": 0, "row_hi": None, "est_bytes": 1, "est_cost": 1,
                              "parent_id": None},
                             {"task_id": "t1", "pk_lo": 10, "pk_hi": 19, "blocks": [1],
                              "row_lo": 0, "row_hi": None, "est_bytes": 1, "est_cost": 1,
                              "parent_id": None}])


def test_commit_task_writes_intent_and_pages(store):
    _mk_session(store)
    rows = [{"pk": 1, "val_a": 1}, {"pk": 2, "val_a": 2}, {"pk": 3, "val_a": 3},
            {"pk": 4, "val_a": 4}, {"pk": 5, "val_a": 5}]
    new_pages = store.commit_task("s1", "t0", rows, 9, 4, finish=False)
    assert new_pages == [0]  # 5 行切出 1 页（4 行），余 1 行在缓冲
    page = store.get_page("s1", 0)
    assert page["row_count"] == 4 and page["state"] == "MATERIALIZED"
    assert page["budget_registered"] == 1  # 预算占用与页提交同事务登记
    report = store.recover()
    sess = report["sessions"][0]
    assert sess["reused_tasks"] == ["t0"] and sess["requeued_tasks"] == ["t1"]


def test_page_boundary_does_not_split_same_pk(store):
    _mk_session(store)
    rows = [{"pk": i} for i in [1, 2, 3, 3, 3, 6]]
    store.commit_task("s1", "t0", rows, 9, 4, finish=True)
    page0 = store.get_page("s1", 0)
    assert page0["row_count"] == 5  # 页扩展到同主键行结束
    import json
    assert [r["pk"] for r in json.loads(page0["rows_json"])] == [1, 2, 3, 3, 3]


def test_consume_page_single_consumer(store):
    _mk_session(store)
    store.commit_task("s1", "t0", [{"pk": 1}], 9, 1, finish=True)
    assert store.consume_page("s1", 0) is True
    assert store.consume_page("s1", 0) is False  # 并发/重复消费失败


def test_recover_only_persisted_facts(store):
    _mk_session(store)
    store.commit_task("s1", "t0", [{"pk": 1}], 9, 4, finish=False)
    report = store.recover()
    sess = report["sessions"][0]
    assert sess["materialized_pages"] == 0  # 不足一页，仍在缓冲
    assert sess["reused_tasks"] == ["t0"]
    assert sess["requeued_tasks"] == ["t1"]
