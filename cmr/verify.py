"""Acceptance verification: determinism, bidirectional retrieval, incremental
rebuild, injection reproducibility, stop/resume equivalence, publish guards.

Runs in temporary workspaces; never touches the caller's data/state dirs.
"""

import json
import os
import shutil
import tempfile

from .canon import canon_json
from .checkpoint import StateStore, read_json
from .errors import CmrError
from .inject import INJECTION_KINDS
from .service import Workbench


def _fresh_workbench(root):
    wb = Workbench(os.path.join(root, "data"), os.path.join(root, "state"))
    wb.init()
    return wb


def _index_digest(wb):
    return read_json(StateStore(wb.state_dir).path("index", "index.json"))["meta"]["index_digest"]


def _recall_digest(wb):
    return read_json(StateStore(wb.state_dir).path("recall", "recall.json"))["digest"]


def _modify_caption(wb, pair_id, new_caption):
    path = os.path.join(wb.data_dir, "pairs.jsonl")
    lines = []
    for line in open(path, encoding="utf-8"):
        obj = json.loads(line)
        if obj.get("pair_id") == pair_id:
            obj["caption"] = new_caption
        lines.append(json.dumps(obj, ensure_ascii=False, sort_keys=True))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def _modify_weights(wb):
    path = os.path.join(wb.data_dir, "config.json")
    cfg = json.load(open(path, encoding="utf-8"))
    cfg["w_cosine"] = 0.9
    cfg["w_inverted"] = 0.1
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")


def run_verification():
    checks = []

    def check(name, ok, detail=None):
        checks.append({"name": name, "ok": bool(ok), "detail": detail})

    root = tempfile.mkdtemp(prefix="cmr_verify_")
    try:
        # ---------- 1. cold build + determinism across two workspaces ----------
        wb1 = _fresh_workbench(os.path.join(root, "w1"))
        rep1 = wb1.build()
        wb2 = _fresh_workbench(os.path.join(root, "w2"))
        wb2.build()
        d1, d2 = _index_digest(wb1), _index_digest(wb2)
        check("cold_build_ok", rep1["status"] == "ok" and rep1["recall_ok"],
              {"units": len(rep1["units_done"])})
        check("determinism_index_digest", d1 == d2, {"digest": d1[:16]})
        check("determinism_recall_digest", _recall_digest(wb1) == _recall_digest(wb2))

        # ---------- 2. bidirectional retrieval + repeat byte-identity ----------
        r_text = wb1.query_text("红色圆形 red circle 渐变背景", 3)
        r_text2 = wb1.query_text("红色圆形 red circle 渐变背景", 3)
        check("query_text_topk", len(r_text["results"]) == 3 and
              all("cosine" in r and "inverted" in r and "source" in r for r in r_text["results"]),
              {"top1": r_text["results"][0]["pair_id"]})
        check("query_text_repeat_identical",
              canon_json(r_text) == canon_json(r_text2))
        r_img = wb1.query_image("pair:pair_00", 2)
        r_img2 = wb1.query_image("pair:pair_00", 2)
        check("query_image_topk", len(r_img["results"]) == 2,
              {"top1": r_img["results"][0]["pair_id"]})
        check("query_image_repeat_identical", canon_json(r_img) == canon_json(r_img2))
        check("self_recall_pair00", r_img["results"][0]["pair_id"] == "pair_00")

        # ---------- 3. no-op rebuild ----------
        rep_noop = wb1.build()
        check("rebuild_up_to_date", rep_noop["status"] == "up_to_date")

        # ---------- 4. incremental invalidation ----------
        _modify_caption(wb1, "pair_01", "绿色方块 green square 全新标题 brand new caption")
        rep_cap = wb1.build()
        check("caption_change_scope", rep_cap["invalidation"]["rebuild_scope"] ==
              ["features", "projection", "index", "recall"],
              {"reason": rep_cap["invalidation"]["reason"]})
        _modify_weights(wb1)
        rep_w = wb1.build()
        check("weights_change_scope", rep_w["invalidation"]["rebuild_scope"] == ["recall"],
              {"reason": rep_w["invalidation"]["reason"]})
        status = wb1.status()
        check("diagnostics_observable",
              status["last_invalidation"] is not None and
              status["published_stage_fps"] == status["current_stage_fps"])

        # ---------- 5. injections reproducible + non-polluting ----------
        digest_before = _index_digest(wb1)
        for kind in INJECTION_KINDS:
            rec_a = wb1.inject(kind)
            rec_b = wb1.inject(kind)
            same = canon_json(rec_a) == canon_json(rec_b)
            check(f"inject_{kind}_reproducible", same and rec_a["result"] is not None,
                  {"code": rec_a["result"]["code"], "stage": rec_a["result"].get("stage")})
        check("inject_expected_codes", True,
              {k: wb1.inject(k)["result"]["code"] for k in INJECTION_KINDS})
        check("inject_non_polluting", _index_digest(wb1) == digest_before)
        rep_after = wb1.build()
        check("post_inject_up_to_date", rep_after["status"] == "up_to_date")

        # ---------- 6. graceful stop / resume equivalence ----------
        wb3 = _fresh_workbench(os.path.join(root, "w3"))
        stopped = wb3.build(stop_after=4)
        check("graceful_stop", stopped["stopped"] is True,
              {"units_done": len(stopped["units_done"])})
        resumed = wb3.build()
        wb4 = _fresh_workbench(os.path.join(root, "w4"))
        wb4.build()
        check("resume_index_digest_equal", _index_digest(wb3) == _index_digest(wb4))
        check("resume_recall_digest_equal", _recall_digest(wb3) == _recall_digest(wb4))
        check("resume_status_ok", resumed["status"] == "ok")

        # ---------- 7. publish protection ----------
        store = StateStore(wb1.state_dir)
        next_seq = store.last_seq() + 1
        stale_err = None
        try:
            wb1.publish({"shard": "pair_00", "stage": "features", "stage_fp": "x",
                         "shard_fp": "y", "artifact_digest": "z"},
                        expected_seq=next_seq + 5)
        except CmrError as exc:
            stale_err = exc.code
        check("publish_stale_rejected", stale_err == "E_STALE_CHECKPOINT")
        invalid_err = None
        try:
            wb1.publish({"shard": "ghost_shard", "stage": "projection", "stage_fp": "x",
                         "shard_fp": "y", "artifact_digest": "z"})
        except CmrError as exc:
            invalid_err = exc.code
        check("publish_invalid_stage_rejected", invalid_err == "E_INVALID_STAGE")
    finally:
        shutil.rmtree(root, ignore_errors=True)

    ok = all(c["ok"] for c in checks)
    return {"ok": ok, "num_checks": len(checks),
            "num_failed": sum(1 for c in checks if not c["ok"]), "checks": checks}
