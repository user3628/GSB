import pytest

from gpworkbench.errors import CrashSimulated
from gpworkbench.statemachine import ADMITTED, BATCHED, PENDING


def test_pending_intent_visible_after_crash(service):
    intents_before = len(service.repo.list_pending_intents())
    service.repo.crash_next_intent_type = "ADMIT"
    with pytest.raises(CrashSimulated):
        service.admit_request("r1")

    pending = service.repo.list_pending_intents()
    assert len(pending) == intents_before + 1
    assert pending[0]["status"] == "PENDING"
    r1 = service.repo.get_request("r1")
    assert r1["state"] == PENDING
    assert r1["reserved_memory"] == 0


def test_pending_intent_visible_after_crash(service):
    # 让下一个 ADMIT 意图在落盘后、应用前“崩溃”
    service.repo.crash_after_intent = None
    intents_before = len(service.repo.list_pending_intents())
    service.repo.crash_after_intent = "admit-"  # 前缀匹配需要完整 id，改为捕获方式
    service.repo.crash_after_intent = None

    # 直接通过钩子捕获真实 intent_id：monkeypatch run_intent 的崩溃点
    captured = {}
    original_run = service.repo.run_intent

    def crash_run(intent_type, payload, apply):
        import gpworkbench.repository as repository_mod

        original_crash = service.repo.crash_after_intent
        # 临时让阶段一提交后立即崩溃：包装 uuid
        original_uuid = repository_mod.uuid.uuid4

        def fake_uuid():
            class U:
                hex = "deadbeefcafef00d"

            repository_mod.uuid.uuid4 = original_uuid
            return U()

        repository_mod.uuid.uuid4 = fake_uuid
        service.repo.crash_after_intent = f"{intent_type.lower()}-deadbeefcafef00d"
        try:
            return original_run(intent_type, payload, apply)
        finally:
            service.repo.crash_after_intent = original_crash
            repository_mod.uuid.uuid4 = original_uuid

    service.repo.run_intent = crash_run
    with pytest.raises(CrashSimulated):
        service.admit_request("r1")
    service.repo.run_intent = original_run

    pending = service.repo.list_pending_intents()
    assert len(pending) == intents_before + 1
    assert pending[0]["status"] == "PENDING"
    # 应用事务未提交：请求仍是 PENDING 且无预留
    r1 = service.repo.get_request("r1")
    assert r1["state"] == PENDING
    assert r1["reserved_memory"] == 0


def test_recovery_rolls_forward_pending_admit(service):
    _crash_next_intent(service, "ADMIT")
    with pytest.raises(CrashSimulated):
        service.admit_request("r1")

    report = service.recover()
    assert report["pending_intent_count"] == 1
    assert report["actions"][0]["action"] == "ROLL_FORWARD"
    assert service.repo.get_request("r1")["state"] == ADMITTED
    # 原 PENDING 意图已被标记 ABORTED，前滚产生新 COMMITTED 意图
    original = service.repo.list_pending_intents()
    assert original == []
    committed = [
        i for i in service.repo.db.connect().execute(
            "SELECT * FROM intents WHERE status='COMMITTED'"
        ).fetchall()
    ]
    assert committed


def test_recovery_after_batch_crash_keeps_consistency(service):
    for rid in ("r1", "r2", "r3"):
        service.admit_request(rid)
    _crash_next_intent(service, "BATCH")
    with pytest.raises(CrashSimulated):
        service.run_batching("g1")

    # 崩溃点：BATCH 意图 PENDING，但没有任何批/请求变更落库
    assert service.repo.list_batches() == []
    for rid in ("r1", "r2", "r3"):
        assert service.repo.get_request(rid)["state"] == ADMITTED

    report = service.recover()
    assert report["actions"][0]["action"] == "ROLL_FORWARD"
    batches = service.repo.list_batches(state="PLACED")
    assert batches
    for rid in ("r1", "r2", "r3"):
        request = service.repo.get_request(rid)
        if request["state"] == BATCHED:
            assert request["batch_id"] is not None
    # 恢复报告给出前后占用差异
    assert "occupancy_diff" in report
    assert "state_count_diff" in report
    assert report["snapshot_after"]["batch_allocated_memory"] >= 0


def test_recovery_aborts_intent_when_quota_no_longer_fits(service):
    _crash_next_intent(service, "ADMIT")
    with pytest.raises(CrashSimulated):
        service.admit_request("r1")
    # 恢复前用 r4（30Gi/900m）+ r2（6Gi/80m）把 g1 显存/算力挤到 r1 无法准入
    service.admit_request("r4")
    service.admit_request("r2")
    report = service.recover()
    assert report["actions"][0]["action"] == "ABORTED"
    assert service.repo.get_request("r1")["state"] == PENDING
    recovery_events = service.repo.list_recovery_events()
    assert any(e["action"] == "ABORT" for e in recovery_events)


def test_recovery_is_idempotent(service):
    _crash_next_intent(service, "ADMIT")
    with pytest.raises(CrashSimulated):
        service.admit_request("r1")
    first = service.recover()
    second = service.recover()
    assert first["pending_intent_count"] == 1
    assert second["pending_intent_count"] == 0
    assert second["actions"] == []
    assert service.repo.get_request("r1")["state"] == ADMITTED


def _crash_next_intent(service, intent_type: str) -> None:
    service.repo.crash_next_intent_type = intent_type
