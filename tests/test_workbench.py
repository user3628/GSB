import json
import multiprocessing as mp
import shutil
from pathlib import Path

import pytest

from quworkbench.canon import canonical
from quworkbench.errors import (
    E_EMPTY_QUERY,
    E_INVALID_CONFIG,
    E_INVALID_STAGE,
    E_SLOT_INVALID,
    E_STALE_CHECKPOINT,
    WorkbenchError,
)
from quworkbench.generator import generate
from quworkbench.model import score_intent, train_nb
from quworkbench.pipeline import Pipeline
from quworkbench.slots import extract_slots, load_dictionary
from quworkbench.store import CheckpointStore
from quworkbench.tokenizer import normalize_query, normalize_text, tokenize


@pytest.fixture()
def workspace(tmp_path):
    ws = tmp_path / "ws"
    generate(ws)
    return ws


# --------------------------------------------------------------- stage 1
def test_normalize_fullwidth_lowercase_offsets():
    normalized, tokens = normalize_query("ＩＰｈｏｎｅ１５　ｐｒｏ 价格")
    assert normalized == "iphone15 pro 价格"
    assert tokens == [
        {"text": "iphone15", "start": 0, "end": 8},
        {"text": "pro", "start": 9, "end": 12},
        {"text": "价", "start": 13, "end": 14},
        {"text": "格", "start": 14, "end": 15},
    ]


def test_normalize_byte_identical_repeat():
    raw = "  星巴克　咖啡\t 北京  营业时间 "
    first = canonical(normalize_query(raw))
    for _ in range(5):
        assert canonical(normalize_query(raw)) == first


@pytest.mark.parametrize("raw", ["", "   ", "！！！？？？", "。。。，、", "＠＃¥％"])
def test_empty_or_punct_only_query_raises(raw):
    with pytest.raises(WorkbenchError) as exc:
        normalize_query(raw, query_id="q-x")
    err = exc.value.to_dict()
    assert err["code"] == E_EMPTY_QUERY
    assert err["query_id"] == "q-x"
    assert err["stage"] == "normalize"


def test_tokenize_mixed_latin_cjk():
    assert tokenize("buy 咖啡 online") == [
        {"text": "buy", "start": 0, "end": 3},
        {"text": "咖", "start": 4, "end": 5},
        {"text": "啡", "start": 5, "end": 6},
        {"text": "online", "start": 7, "end": 13},
    ]


# --------------------------------------------------------------- stage 2a
def _labeled():
    samples = []
    data = {
        "navigational": ["苹果官网", "华为 登录", "app 下载"],
        "informational": ["手机 怎么选", "跑鞋 评测", "教程"],
        "transactional": ["手机 多少钱", "买 跑鞋", "优惠券"],
        "local_service": ["附近 火锅", "北京 地址", "电话"],
    }
    for intent, raws in data.items():
        for raw in raws:
            _, toks = normalize_query(raw)
            samples.append(([t["text"] for t in toks], intent))
    return samples


def test_nb_scores_softmax_and_determinism():
    model = train_nb(_labeled(), 1.0, "<UNK>", {}, labeled_digest="d")
    intent, confidence, scores = score_intent(model, ["手", "机", "多", "少", "钱"])
    assert intent == "transactional"
    assert abs(sum(scores.values()) - 1.0) < 1e-9
    assert scores[intent] == confidence
    again = score_intent(model, ["手", "机", "多", "少", "钱"])
    assert canonical((intent, confidence, scores)) == canonical(again)


def test_nb_unknown_token_fallback():
    model = train_nb(_labeled(), 1.0, "<UNK>", {})
    intent, _, _ = score_intent(model, ["zzzzneverseen"])
    assert intent in scores_intents()


def scores_intents():
    return {"informational", "local_service", "navigational", "transactional"}


def test_nb_alpha_zero_invalid_config():
    with pytest.raises(WorkbenchError) as exc:
        train_nb(_labeled(), 0.0, "<UNK>", {})
    assert exc.value.code == E_INVALID_CONFIG
    assert exc.value.stage == "classify"
    assert "smoothing" in exc.value.reason


def test_nb_insufficient_samples_invalid_config():
    with pytest.raises(WorkbenchError) as exc:
        train_nb([], 1.0, "<UNK>", {})
    assert exc.value.code == E_INVALID_CONFIG
    only_three = [s for s in _labeled() if s[1] != "navigational"]
    with pytest.raises(WorkbenchError) as exc2:
        train_nb(only_three, 1.0, "<UNK>", {})
    assert exc2.value.code == E_INVALID_CONFIG
    assert exc2.value.details["missing_intents"] == ["navigational"]


# --------------------------------------------------------------- stage 2b
def test_slots_longest_match_and_suppression(workspace):
    dictionary = load_dictionary(workspace / "data/dict/slot_dict.json")
    accepted, suppressed = extract_slots("苹果手机多少钱", dictionary, {"brand": 0, "category": 1, "region": 2})
    assert [s["term"] for s in accepted] == ["苹果手机"]
    suppressed_terms = {s["term"] for s in suppressed}
    assert suppressed_terms == {"苹果", "手机"}
    for s in suppressed:
        assert s["reason"] == "overlap_lower_priority"
        assert s["suppressed_by"]["term"] == "苹果手机"
        assert s["dict_version"] == "1.0.0"


def test_slots_region_brand_category(workspace):
    dictionary = load_dictionary(workspace / "data/dict/slot_dict.json")
    accepted, suppressed = extract_slots("星巴克咖啡 北京 营业时间", dictionary, {"brand": 0, "category": 1, "region": 2})
    got = {(s["term"], s["slot_type"], s["start"], s["end"]) for s in accepted}
    assert ("星巴克", "brand", 0, 3) in got
    assert ("咖啡", "category", 3, 5) in got
    assert ("北京", "region", 6, 8) in got


def test_slots_missing_dictionary(tmp_path):
    with pytest.raises(WorkbenchError) as exc:
        load_dictionary(tmp_path / "nope.json")
    assert exc.value.code == E_SLOT_INVALID
    assert exc.value.stage == "slots"
    assert exc.value.reason == "dictionary_missing"


def test_slots_corrupt_dictionary(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not json", encoding="utf-8")
    with pytest.raises(WorkbenchError) as exc:
        load_dictionary(bad)
    assert exc.value.code == E_SLOT_INVALID


# --------------------------------------------------------------- stage 3
def _record(store, shard, fp="sha256:x"):
    return {
        "shard_id": shard,
        "shard_fingerprint": fp,
        "config_fingerprint": "sha256:cfg",
        "input_digests": {},
    }


def test_publish_and_consistent_readback(tmp_path):
    store = CheckpointStore(tmp_path)
    rec = store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [1]})
    assert rec["seq"] == 1
    cp = store.consistent("shard-a", "normalize")
    assert cp.seq == 1
    assert store.read_artifact("shard-a", "normalize") == {"results": [1]}


def test_publish_stale_and_duplicate_rejected_state_unchanged(tmp_path):
    store = CheckpointStore(tmp_path)
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [1]})
    before = (store.journal_path("shard-a", "normalize")).read_bytes()
    for bad_seq in (1, 3, 99):
        with pytest.raises(WorkbenchError) as exc:
            store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [2]}, seq=bad_seq)
        assert exc.value.code == E_STALE_CHECKPOINT
    assert store.journal_path("shard-a", "normalize").read_bytes() == before
    assert store.consistent("shard-a", "normalize").seq == 1


def test_publish_cross_shard_misaligned_rejected(tmp_path):
    store = CheckpointStore(tmp_path)
    with pytest.raises(WorkbenchError) as exc:
        store.publish("shard-b", "normalize", _record(store, "shard-a"), {"results": []})
    assert exc.value.code == E_STALE_CHECKPOINT
    with pytest.raises(WorkbenchError) as exc2:
        store.publish(
            "shard-b",
            "normalize",
            _record(store, "shard-b", fp="sha256:wrong"),
            {"results": []},
            expected_shard_fingerprint="sha256:right",
        )
    assert exc2.value.code == E_STALE_CHECKPOINT
    assert store.consistent("shard-b", "normalize") is None


def test_publish_missing_upstream_invalid_stage(tmp_path):
    store = CheckpointStore(tmp_path)
    with pytest.raises(WorkbenchError) as exc:
        store.publish("shard-a", "classify", _record(store, "shard-a"), {"results": []})
    assert exc.value.code == E_INVALID_STAGE
    assert store.consistent("shard-a", "classify") is None


def test_recovery_ignores_corrupt_and_duplicate_records(tmp_path):
    store = CheckpointStore(tmp_path)
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [1]})
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [2]})
    journal = store.journal_path("shard-a", "normalize")
    with open(journal, "ab") as fh:
        fh.write(canonical(dict(_record(store, "shard-a"), seq=2, stage="normalize")) + b"\n")  # duplicate
        fh.write(b'{"seq": 3, "stage": "norm')  # corrupt tail
    report = store.recover()
    assert report["truncated_journals"]
    cp = store.consistent("shard-a", "normalize")
    assert cp.seq == 2
    lines = [l for l in journal.read_bytes().splitlines() if l.strip()]
    assert len(lines) == 2
    # resume from the consistent checkpoint: next publish must be seq 3
    rec = store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [3]})
    assert rec["seq"] == 3


def test_recovery_drops_record_without_artifact(tmp_path):
    store = CheckpointStore(tmp_path)
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [1]})
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [2]})
    store.artifact_path("shard-a", "normalize", 2).unlink()
    store.recover()
    assert store.consistent("shard-a", "normalize").seq == 1


def _publish_worker(ws, shard, seq, queue):
    try:
        store = CheckpointStore(ws)
        store.publish(shard, "normalize", _record(store, shard), {"results": [seq]}, seq=seq)
        queue.put(("ok", seq))
    except WorkbenchError as exc:
        queue.put(("err", exc.code))


def test_concurrent_publish_only_contiguous_wins(tmp_path):
    store = CheckpointStore(tmp_path)
    store.publish("shard-a", "normalize", _record(store, "shard-a"), {"results": [0]})
    ctx = mp.get_context("fork")
    queue = ctx.Queue()
    procs = [ctx.Process(target=_publish_worker, args=(tmp_path, "shard-a", 2, queue)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    results = [queue.get() for _ in procs]
    assert results.count(("ok", 2)) == 1
    assert results.count(("err", E_STALE_CHECKPOINT)) == 3
    assert CheckpointStore(tmp_path).consistent("shard-a", "normalize").seq == 2


# ------------------------------------------------------- pipeline behavior
def test_invalidation_propagation_and_reuse(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    # config change: classify+slots rebuilt, normalize reused
    pipe.change_config("classify.smoothing_alpha", 0.5)
    status = pipe.status()
    for shard in status["shards"].values():
        assert shard["normalize"]["seq"] == 1
        assert shard["classify"]["seq"] == 2
        assert shard["slots"]["seq"] == 2
        assert not shard["normalize"]["stale"]
    events = pipe.invalidations()
    assert any("smoothing_alpha_changed" in e["reasons"] for e in events)
    assert any("upstream_invalidated" in e["reasons"] for e in events)


def test_shard_replace_only_rebuilds_that_shard(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    variant = [
        json.loads(l)
        for l in (workspace / "data/variants/shard-002-v2.jsonl").read_text(encoding="utf-8").splitlines()
        if l.strip()
    ]
    result = pipe.replace_shard("shard-002", variant)
    touched = {(e["shard_id"], e["stage"]) for e in result["invalidations"]}
    assert touched == {("shard-002", s) for s in ("normalize", "classify", "slots")}
    status = pipe.status()
    assert status["shards"]["shard-000"]["normalize"]["seq"] == 1
    assert status["shards"]["shard-002"]["normalize"]["seq"] == 2


def test_incremental_matches_full_rebuild(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    variant = [
        json.loads(l)
        for l in (workspace / "data/variants/shard-002-v2.jsonl").read_text(encoding="utf-8").splitlines()
        if l.strip()
    ]
    pipe.replace_shard("shard-002", variant)
    pipe.change_config("classify.smoothing_alpha", 0.5)
    pipe.change_config("slots.priority", {"brand": 2, "category": 0, "region": 1})
    outcome = pipe.verify_against_full()
    assert outcome["match"] is True
    # and the digest is stable across a second identical verification
    assert pipe.verify_against_full()["digest"] == outcome["digest"]


def test_dictionary_change_invalidates_slots_only(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    dict_path = workspace / "data/dict/slot_dict.json"
    data = json.loads(dict_path.read_text(encoding="utf-8"))
    data["entries"].append({"term": "测评", "slot_type": "category"})
    data["version"] = "1.1.0"
    dict_path.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    result = Pipeline(workspace).run()
    stages = {e["stage"] for e in result["invalidations"]}
    assert stages == {"slots"}
    assert all("slot_dictionary_changed" in e["reasons"] for e in result["invalidations"])


def test_pipeline_recovery_after_simulated_crash(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    before = pipe.summary()["digest"]
    journal = workspace / "checkpoints/shard-001/classify/records.jsonl"
    with open(journal, "ab") as fh:
        fh.write(b'{"seq": 42, "broken"')
    report = pipe.recover()
    assert report["truncated_journals"]
    pipe.run()
    assert pipe.summary()["digest"] == before


def test_empty_query_entries_in_artifacts(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    artifact = pipe.store.read_artifact("shard-002", "classify")
    errors = [r for r in artifact["results"] if r["status"] == "error"]
    assert errors
    assert all(e["error"]["code"] == E_EMPTY_QUERY for e in errors)
    assert all(e["error"]["query_id"] for e in errors)


def test_slot_diagnostics_endpoint_data(workspace):
    pipe = Pipeline(workspace)
    pipe.run_full()
    diag = pipe.slot_diagnostics("q-0037")
    assert diag is not None
    assert diag["slots"][0]["term"] == "苹果手机"
    assert {s["term"] for s in diag["diagnostics"]["suppressed"]} == {"苹果", "手机"}


# --------------------------------------------------------------- HTTP layer
@pytest.fixture()
def client(tmp_path):
    from fastapi.testclient import TestClient

    from quworkbench.server import create_app

    ws = tmp_path / "ws-http"
    generate(ws)
    return TestClient(create_app(ws))


def test_http_status_run_verify(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.post("/run/full").status_code == 200
    status = client.get("/status").json()
    assert set(status["shards"]) == {"shard-000", "shard-001", "shard-002"}
    assert all(not s["stale"] for sh in status["shards"].values() for s in sh.values())
    assert client.post("/verify").json()["match"] is True


def test_http_replace_config_change_and_invalidations(client):
    client.post("/run/full")
    variant = [
        {"query_id": "q-2001", "raw_query": "成都 火锅 店地址", "locale": "zh-CN", "channel": "web"}
    ]
    r = client.post("/shards/shard-001/replace", json={"records": variant})
    assert r.status_code == 200
    assert {e["stage"] for e in r.json()["invalidations"]} == {"normalize", "classify", "slots"}
    r = client.post("/config/change", json={"key": "slots.priority", "value": {"brand": 1, "category": 0, "region": 2}})
    assert r.status_code == 200
    events = client.get("/invalidations").json()["events"]
    assert any("slot_priority_changed" in e["reasons"] for e in events)
    assert client.post("/verify").json()["match"] is True


def test_http_slot_diagnostics_and_error_mapping(client):
    client.post("/run/full")
    diag = client.get("/queries/q-0037/slots").json()
    assert diag["slots"][0]["term"] == "苹果手机"
    assert {s["term"] for s in diag["diagnostics"]["suppressed"]} == {"苹果", "手机"}
    r = client.post("/config/change", json={"key": "classify.smoothing_alpha", "value": 0.0})
    assert r.status_code == 409
    err = r.json()["error"]
    assert err["code"] == E_INVALID_CONFIG
    assert err["stage"] == "classify"
    assert client.get("/queries/q-9999/slots").status_code == 404


def test_incremental_mismatch_reports_first_diff(workspace):
    from quworkbench.canon import canonical, digest_bytes
    from quworkbench.errors import E_INCREMENTAL_MISMATCH

    pipe = Pipeline(workspace)
    pipe.run_full()
    # tamper with one slots artifact + its journal record to simulate divergence
    cp = pipe.store.consistent("shard-000", "slots")
    artifact = pipe.store.read_artifact("shard-000", "slots")
    target = next(r for r in artifact["results"] if r["status"] == "ok" and r["slots"])
    target["slots"][0]["term"] = "TAMPERED"
    data = canonical(artifact)
    cp.artifact_path.write_bytes(data)
    journal = pipe.store.journal_path("shard-000", "slots")
    lines = journal.read_bytes().splitlines()
    rec = json.loads(lines[-1])
    rec["artifact_digest"] = digest_bytes(data)
    journal.write_bytes(b"\n".join(lines[:-1] + [canonical(rec)]) + b"\n")
    with pytest.raises(WorkbenchError) as exc:
        pipe.verify_against_full()
    err = exc.value.to_dict()
    assert err["code"] == E_INCREMENTAL_MISMATCH
    assert err["stage"] == "slots"
    assert err["query_id"] == target["query_id"]
    assert err["shard_id"] == "shard-000"
