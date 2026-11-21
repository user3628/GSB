"""HTTP API 端到端：全部业务码、幂等、冲突、容差、镜像去重。"""

from tests.conftest import submit_batch, submit_policy


def _event(rid, ts=1000, route="r", client="c", weight=100, target="stable"):
    return {
        "request_id": rid, "timestamp_ms": ts, "route_key": route,
        "client_id": client, "weight": weight, "target_version": target,
    }


def test_health_empty(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["latest_policy_version"] is None


def test_policy_submit_and_monotonic_version(client, make_policy):
    r = submit_policy(client, make_policy(version=1))
    assert r.status_code == 200
    # 重复/回退版本号 -> E_SNAPSHOT_INVALID，既有状态不变
    r2 = submit_policy(client, make_policy(version=1))
    assert r2.status_code == 422
    assert r2.json()["error"]["code"] == "E_SNAPSHOT_INVALID"
    assert client.get("/health").json()["latest_policy_version"] == 1


def test_snapshot_digest_mismatch(client, samples):
    r = submit_policy(client, samples["bad_policy"])
    assert r.status_code == 422
    body = r.json()["error"]
    assert body["code"] == "E_SNAPSHOT_INVALID"
    assert "recomputed" in body["context"]


def test_zero_weight_policy_is_weight_invalid(client, samples):
    r = submit_policy(client, samples["zero_policy"])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_WEIGHT_INVALID"


def test_event_invalid_whole_batch_rejected(client, make_policy, samples):
    submit_policy(client, make_policy(version=1))
    r = submit_batch(client, samples["bad_events"])
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_EVENT_INVALID"
    # 整批不入库
    assert client.get("/batches").json()["batches"] == []


def test_float_ratio_rejected(client, make_policy):
    policy = make_policy(version=1)
    policy["version_weights"] = {"stable": 0.7, "canary": 0.3}  # type: ignore[dict-item]
    # 摘要也会失配；先重算摘要以确保失败点是浮点拒绝而非摘要
    from app.models import canonical_digest
    policy["digest"] = canonical_digest(policy)
    r = submit_policy(client, policy)
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_WEIGHT_INVALID"


def test_full_batch_routing_and_stats(client, samples):
    submit_policy(client, samples["policy_v1"])
    r = submit_batch(client, samples["batch1"])
    assert r.status_code == 200
    body = r.json()
    assert body["routed"] + body["rejected"] == body["received"]
    stats = client.get("/stats/routing").json()
    assert stats["total_weight"] > 0
    # 定点占比固定 9 位小数，且各版本份额求和概念上覆盖全部权重
    for share in stats["shares"].values():
        assert len(share.split(".")[1]) == 9


def test_gray_limit_exceeded_context(client, samples):
    submit_policy(client, samples["policy_v2"])
    r = submit_batch(client, samples["batch3"])
    assert r.status_code == 200
    rejected = [v for v in r.json()["verdicts"] if v["status"] == "rejected"]
    assert rejected, "样本应确定性触发灰度上限"
    v = rejected[0]
    assert v["error_code"] == "E_GRAY_LIMIT_EXCEEDED"
    gc = v["gray_check"]
    for key in ("usage", "limit", "relative_tolerance", "threshold_inclusive"):
        assert len(gc[key].split(".")[1]) == 9
    assert gc["exceeded"] is True


def test_mirror_dedup_window(client, samples):
    submit_policy(client, samples["policy_v3"])
    r = submit_batch(client, samples["batch2"])
    body = r.json()
    # 13 条事件：首个采样命中镜像 + 窗口外一条重新开放镜像 = 2
    assert body["routed"] == 13
    assert body["mirrored"] == 2
    verdicts = body["verdicts"]
    suppressed = [
        v for v in verdicts
        if v["mirror"]["sample_hit"] and not v["mirror"]["mirrored"]
    ]
    assert suppressed and all(v["mirror"]["inside_dedup_window"] for v in suppressed)


def test_idempotent_reuse_and_conflicting_request(client, make_policy):
    submit_policy(client, make_policy(version=1))
    payload = {"batch_id": "b-x", "policy_version": 1,
               "events": [_event("r1", weight=100)]}
    r1 = submit_batch(client, payload)
    assert r1.status_code == 200 and r1.json()["reused"] == 0
    # 同内容重复提交：复用，不二次裁决
    r2 = submit_batch(client, payload)
    assert r2.status_code == 200 and r2.json()["reused"] == 1
    # 同 ID 不同内容：E_CONFLICTING_REQUEST
    conflict = {"batch_id": "b-y", "policy_version": 1,
                "events": [_event("r1", weight=999)]}
    r3 = submit_batch(client, conflict)
    assert r3.status_code == 409
    assert r3.json()["error"]["code"] == "E_CONFLICTING_REQUEST"


def test_unknown_target_version_not_fallback(client, make_policy):
    submit_policy(client, make_policy(version=1))
    payload = {"batch_id": "b-z", "policy_version": 1,
               "events": [_event("r2", target="ghost-version")]}
    r = submit_batch(client, payload)
    v = r.json()["verdicts"][0]
    assert v["status"] == "rejected"
    assert v["error_code"] == "E_WEIGHT_INVALID"


def test_error_contract_and_traces(client, samples):
    submit_policy(client, samples["policy_v1"])
    submit_batch(client, samples["batch1"])
    contract = client.get("/contracts/errors").json()
    assert contract["fixed_point"]["decimals"] == 9
    assert contract["fixed_point"]["bound_respected"] is True
    assert contract["recorded_ops"], "必须持久化每步运算轨迹"
    assert contract["gray_cap_contract"]["tie_break"].startswith("stable")


def test_float64_experiment_endpoint(client, samples):
    r = client.post("/experiments/float64", json={"values": samples["mixed"]["values"]})
    assert r.status_code == 200
    paths = {p["path"]: p for p in r.json()["paths"]}
    assert paths["fixed_point_kahan"]["abs_error"] == "0"
    assert paths["float64_naive_order"]["abs_error"] != "0"


def test_replay_bit_identical(client, samples):
    submit_policy(client, samples["policy_v1"])
    submit_batch(client, samples["batch1"])
    r = client.post("/admin/replay-check")
    assert r.status_code == 200
    assert r.json()["bit_identical"] is True


def test_rebuild_stats_matches_persisted(client, samples):
    submit_policy(client, samples["policy_v1"])
    submit_batch(client, samples["batch1"])
    r = client.post("/admin/rebuild")
    assert r.status_code == 200
    assert r.json()["bit_identical_to_persisted"] is True


def test_verdict_lookup_404(client, make_policy):
    submit_policy(client, make_policy(version=1))
    r = client.get("/verdicts/nope")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "E_NOT_FOUND"
