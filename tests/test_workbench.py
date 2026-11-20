"""End-to-end tests for the cmr workbench."""

import json
import os

import numpy as np
import pytest

from cmr.canon import canon_json
from cmr.checkpoint import StateStore, read_json
from cmr.errors import CmrError
from cmr.image_features import extract_image_features
from cmr.server import create_app
from cmr.service import Workbench
from cmr.text_features import extract_text_features, normalize_text


@pytest.fixture()
def wb(tmp_path):
    bench = Workbench(str(tmp_path / "data"), str(tmp_path / "state"))
    bench.init()
    bench.build()
    return bench


def _index_digest(bench):
    return read_json(StateStore(bench.state_dir).path("index", "index.json"))["meta"]["index_digest"]


# ---------------------------------------------------------------- features
def test_feature_determinism_and_dims(wb):
    cfg = wb.load_config()
    img = os.path.join(wb.data_dir, "fragments", "frag_00.png")
    f1 = extract_image_features(img, cfg)
    f2 = extract_image_features(img, cfg)
    assert f1.shape == (cfg.image_dim,) and np.array_equal(f1, f2)
    t1 = extract_text_features("红色  Red　Circle ", cfg)
    t2 = extract_text_features("红色  Red　Circle ", cfg)
    assert t1.shape == (cfg.text_dim,) and np.array_equal(t1, t2)
    # NFKC + case + whitespace folding
    assert normalize_text("ＡＢ  Red　Circle") == "ab red circle"


def test_build_deterministic(tmp_path):
    a = Workbench(str(tmp_path / "a" / "data"), str(tmp_path / "a" / "state"))
    a.init(); a.build()
    b = Workbench(str(tmp_path / "b" / "data"), str(tmp_path / "b" / "state"))
    b.init(); b.build()
    assert _index_digest(a) == _index_digest(b)


# ---------------------------------------------------------------- retrieval
def test_bidirectional_retrieval(wb):
    r = wb.query_text("红色三角形 red triangle", 3)
    assert len(r["results"]) == 3
    top = r["results"][0]
    assert top["pair_id"] == "pair_01"
    for res in r["results"]:
        assert {"cosine", "inverted", "source", "shard", "score"} <= set(res)
    r2 = wb.query_image("pair:pair_00", 2)
    assert r2["results"][0]["pair_id"] == "pair_00"


def test_query_repeat_byte_identical(wb):
    assert canon_json(wb.query_text("渐变背景 gradient", 3)) == \
           canon_json(wb.query_text("渐变背景 gradient", 3))


@pytest.mark.parametrize("text,k,code", [("", 3, "E_QUERY_INVALID"),
                                         ("   ", 3, "E_QUERY_INVALID"),
                                         ("hello", 0, "E_QUERY_INVALID"),
                                         ("hello", 999, "E_QUERY_INVALID")])
def test_query_invalid(wb, text, k, code):
    with pytest.raises(CmrError) as exc:
        wb.query_text(text, k)
    assert exc.value.code == code


def test_query_image_vector_dim_invalid(wb):
    with pytest.raises(CmrError) as exc:
        wb.query_image_vector([0.0] * 10, 3)
    assert exc.value.code == "E_QUERY_INVALID"


# ---------------------------------------------------------------- errors
def test_dim_mismatch(tmp_path):
    bench = Workbench(str(tmp_path / "data"), str(tmp_path / "state"))
    bench.init()
    manifest = os.path.join(bench.data_dir, "pairs.jsonl")
    lines = open(manifest, encoding="utf-8").read().splitlines()
    meta = json.loads(lines[0])
    meta["declared_text_dim"] = 1
    lines[0] = json.dumps(meta, ensure_ascii=False, sort_keys=True)
    open(manifest, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    with pytest.raises(CmrError) as exc:
        bench.build()
    assert exc.value.code == "E_DIM_MISMATCH"
    assert exc.value.stage == "features"


def test_projection_nan(tmp_path):
    bench = Workbench(str(tmp_path / "data"), str(tmp_path / "state"))
    bench.init()
    bench.build()
    proj = os.path.join(bench.data_dir, "projection.json")
    obj = json.load(open(proj, encoding="utf-8"))
    obj["text"]["data"][2][5] = float("inf")
    json.dump(obj, open(proj, "w", encoding="utf-8"))
    with pytest.raises(CmrError) as exc:
        bench.build(from_stage="projection")
    assert exc.value.code == "E_PROJECTION_INVALID"
    assert exc.value.stage == "projection"
    assert exc.value.position == "text[2][5]"


def test_missing_fragment_at_index_stage(tmp_path):
    bench = Workbench(str(tmp_path / "data"), str(tmp_path / "state"))
    bench.init()
    bench.build()
    os.remove(os.path.join(bench.data_dir, "fragments", "frag_02.png"))
    with pytest.raises(CmrError) as exc:
        bench.build(from_stage="index")
    assert exc.value.code == "E_MISSING_FRAGMENT"
    assert exc.value.stage == "index"
    assert exc.value.detail["pair_id"] == "pair_02"


# ---------------------------------------------------------------- invalidation
def test_invalidation_scopes(wb):
    # caption change -> full rebuild from features
    manifest = os.path.join(wb.data_dir, "pairs.jsonl")
    lines = open(manifest, encoding="utf-8").read().splitlines()
    obj = json.loads(lines[1])
    obj["caption"] = "完全不同的标题 totally different caption"
    lines[1] = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    open(manifest, "w", encoding="utf-8").write("\n".join(lines) + "\n")
    rep = wb.build()
    assert rep["invalidation"]["rebuild_scope"] == ["features", "projection", "index", "recall"]

    # ranking weights -> recall only
    cfg_path = os.path.join(wb.data_dir, "config.json")
    cfg = json.load(open(cfg_path, encoding="utf-8"))
    cfg["w_cosine"] = 0.5
    cfg["w_inverted"] = 0.5
    json.dump(cfg, open(cfg_path, "w", encoding="utf-8"))
    rep = wb.build()
    assert rep["invalidation"]["rebuild_scope"] == ["recall"]
    assert wb.build()["status"] == "up_to_date"


def test_projection_change_scope(wb):
    proj = os.path.join(wb.data_dir, "projection.json")
    obj = json.load(open(proj, encoding="utf-8"))
    obj["image"]["data"][0][0] += 0.5
    json.dump(obj, open(proj, "w", encoding="utf-8"))
    rep = wb.build()
    assert rep["invalidation"]["rebuild_scope"] == ["projection", "index", "recall"]


# ---------------------------------------------------------------- stop/resume
def test_stop_and_resume(tmp_path):
    bench = Workbench(str(tmp_path / "data"), str(tmp_path / "state"))
    bench.init()
    store = StateStore(bench.state_dir)
    store.request_stop()
    rep = bench.build()
    assert rep["stopped"] is True and rep["units_done"] == []
    store.clear_stop()
    rep = bench.build(stop_after=3)
    assert rep["stopped"] is True and len(rep["units_done"]) == 3
    rep = bench.build()
    assert rep["status"] == "ok"

    full = Workbench(str(tmp_path / "f" / "data"), str(tmp_path / "f" / "state"))
    full.init(); full.build()
    assert _index_digest(bench) == _index_digest(full)


def test_checkpoint_atomic_and_monotonic(wb):
    store = StateStore(wb.state_dir)
    records = store.list_checkpoints()
    seqs = [r["seq"] for r in records]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    for rec in records:
        assert {"seq", "shard", "stage", "stage_fp", "shard_fp", "artifact_digest"} <= set(rec)


def test_publish_guards(wb):
    store = StateStore(wb.state_dir)
    with pytest.raises(CmrError) as exc:
        wb.publish({"shard": "pair_00", "stage": "features", "stage_fp": "a",
                    "shard_fp": "b", "artifact_digest": "c"},
                   expected_seq=store.last_seq() + 7)
    assert exc.value.code == "E_STALE_CHECKPOINT"
    with pytest.raises(CmrError) as exc:
        wb.publish({"shard": "ghost", "stage": "index", "stage_fp": "a",
                    "shard_fp": "b", "artifact_digest": "c"})
    assert exc.value.code == "E_INVALID_STAGE"


# ---------------------------------------------------------------- injections
@pytest.mark.parametrize("kind,code,stage", [
    ("dim_mismatch", "E_DIM_MISMATCH", "features"),
    ("nan_projection", "E_PROJECTION_INVALID", "projection"),
    ("missing_fragment", "E_MISSING_FRAGMENT", "index"),
    ("empty_query", "E_QUERY_INVALID", "query"),
    ("k_out_of_range", "E_QUERY_INVALID", "query"),
    ("bad_image_vector", "E_QUERY_INVALID", "query"),
])
def test_injections(wb, kind, code, stage):
    digest_before = _index_digest(wb)
    rec = wb.inject(kind)
    assert rec["result"]["code"] == code
    assert rec["result"]["stage"] == stage
    assert rec["inject_id"].startswith("INJ-")
    # published index not polluted
    assert _index_digest(wb) == digest_before


def test_injection_record_file_byte_identical(wb):
    wb.inject("nan_projection")
    store = StateStore(wb.state_dir)
    path = store.path("injections", "INJ-NAN-PROJECTION-0001.json")
    first = open(path, "rb").read()
    wb.inject("nan_projection")
    assert open(path, "rb").read() == first


# ---------------------------------------------------------------- HTTP
@pytest.fixture()
def client(wb):
    from fastapi.testclient import TestClient

    return TestClient(create_app(wb.data_dir, wb.state_dir))


def test_http_status_and_query(client):
    r = client.get("/api/status")
    assert r.status_code == 200
    body = r.json()
    assert body["index_meta"]["num_shards"] == 6
    assert body["published_stage_fps"] == body["current_stage_fps"]

    r = client.post("/api/query/text", json={"text": "红色三角形 red triangle", "k": 2})
    assert r.status_code == 200
    assert len(r.json()["results"]) == 2

    r = client.post("/api/query/image", json={"image": "pair:pair_04", "k": 2})
    assert r.status_code == 200
    assert r.json()["results"][0]["pair_id"] == "pair_04"


def test_http_error_contract(client):
    r = client.post("/api/query/text", json={"text": "", "k": 3})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "E_QUERY_INVALID"


def test_http_diagnostic_page(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "跨模态检索索引工作台" in r.text


def test_http_inject_and_stop(client):
    r = client.post("/api/inject/empty_query")
    assert r.status_code == 200
    assert r.json()["result"]["code"] == "E_QUERY_INVALID"
    assert client.post("/api/stop").json()["stop_requested"] is True
    rep = client.post("/api/resume").json()
    assert rep["status"] in ("ok", "up_to_date")
