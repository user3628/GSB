"""End-to-end deterministic demo producing artifacts/demo_report.json."""
from __future__ import annotations

import json
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from wckpt import Config, Engine, Page  # noqa: E402
from wckpt.errors import TruncateUnsafeError, WalCorruptError  # noqa: E402

DATA = os.path.join(ROOT, "data")
RUN = os.path.join(ROOT, "artifacts", "demo_run")
REPORT = os.path.join(ROOT, "artifacts", "demo_report.json")


def load_json(name):
    with open(os.path.join(DATA, name), "r", encoding="utf-8") as fh:
        return json.load(fh)


def fresh_engine(root, config):
    if os.path.exists(root):
        shutil.rmtree(root)
    engine = Engine(root, config)
    engine.open()
    return engine


def seed_pages(engine, initial):
    # Pages with null digest do not exist yet; the first WAL record creates them.
    for data in initial["pages"].values():
        if data.get("digest") is not None:
            engine.store.upsert_page(Page.from_dict(data))


def feed(engine, records, upto=None):
    for rec in records[:upto]:
        engine.apply_modification(
            page_no=rec["page_no"],
            after_digest=rec["after_digest"],
            txn_id=rec["txn_id"],
        )


def main():
    config_json = load_json("checkpoint_config.json")
    config = Config.from_dict(config_json)
    initial = load_json("initial_pages.json")
    small = load_json("wal_sequence.json")
    large = load_json("wal_sequence_large.json")
    report = {"steps": []}

    def step(name, payload):
        report["steps"].append({"step": name, **payload})
        print(f"[ok] {name}: {json.dumps(payload, ensure_ascii=False)[:240]}")

    # 1. Append + rotation on the small workload.
    engine = fresh_engine(os.path.join(RUN, "small"), config)
    seed_pages(engine, initial)
    feed(engine, small["records"])
    segments = engine.wal_segments()
    lsns = [r.lsn for r in engine.wal.iter_records()]
    step("wal_append_rotation", {
        "records": len(lsns),
        "segments": len(segments),
        "lsn_contiguous": lsns == list(range(1, len(lsns) + 1)),
        "segment_headers_match_first_record": all(
            not s["corrupt"] and s["start_lsn"] >= 0 for s in segments
        ),
    })

    # 2. Range query returns segment identity.
    rows = engine.query_wal(5, 8)
    step("range_query", {
        "returned": len(rows),
        "lsns": [r["record"]["lsn"] for r in rows],
        "segments": sorted({r["segment"] for r in rows}),
    })

    # 3. Corrupt segment -> E_WAL_CORRUPT, the whole segment is excluded.
    engine.close()
    corrupt_root = os.path.join(RUN, "corrupt")
    shutil.copytree(os.path.join(RUN, "small"), corrupt_root)
    target = sorted(
        n for n in os.listdir(os.path.join(corrupt_root, "wal")) if n.endswith(".wal")
    )[config_json["corrupt_injection_segment_index"]]
    with open(os.path.join(corrupt_root, "wal", target), "a", encoding="utf-8") as fh:
        fh.write('{"magic":"REC","lsn": 999999}\n')
    corrupt_engine = Engine(corrupt_root, config)
    corrupt_engine.open()
    corrupt_info = [s for s in corrupt_engine.wal_segments() if s["corrupt"]]
    try:
        list(corrupt_engine.wal.iter_records())
        corrupt_code = None
    except WalCorruptError as exc:
        corrupt_code = exc.code
    step("corrupt_segment", {"segment": target, "code": corrupt_code,
                             "corrupt_segments": [s["segment"] for s in corrupt_info]})
    corrupt_engine.close()

    # 4. Adaptive replay equivalence BEFORE checkpointing (full WAL prefix).
    def replay_prefix(root, cfg, records):
        from wckpt.models import Checkpoint
        eng = fresh_engine(root, cfg)
        seed_pages(eng, initial)
        baseline_pages = {
            int(k): Page.from_dict(v)
            for k, v in initial["pages"].items()
            if v.get("digest") is not None
        }
        feed(eng, records)
        cp0 = Checkpoint(seq=0, start_lsn=1, pages=baseline_pages).seal()
        result = eng.replayer.replay(cp0)
        return eng, result

    batched_engine, batched_result = replay_prefix(
        os.path.join(RUN, "large_batched"), config, large["records"]
    )
    precise_cfg = Config.from_dict(config_json)
    precise_cfg.replay_record_threshold = 10 ** 9
    precise_cfg.replay_dirty_page_threshold = 10 ** 9
    precise_engine, precise_result = replay_prefix(
        os.path.join(RUN, "large_precise"), precise_cfg, large["records"]
    )
    batched_pages = {k: v.to_dict() for k, v in batched_result.final_pages.items()}
    precise_pages = {k: v.to_dict() for k, v in precise_result.final_pages.items()}
    baseline_match = all(
        batched_pages[int(p)]["digest"] == d
        for p, d in large["expected_final_digests"].items()
    )
    step("adaptive_equivalence", {
        "records": batched_result.applied_records,
        "batched_path": batched_result.decision.path,
        "precise_path": precise_result.decision.path,
        "batched_batch_size": batched_result.decision.batch_size,
        "batched_flush_group": batched_result.decision.flush_group_size,
        "batched_reason": batched_result.decision.reason,
        "precise_reason": precise_result.decision.reason,
        "page_level_equal": batched_pages == precise_pages,
        "baseline_digest_match": baseline_match,
    })
    batched_engine.close()
    precise_engine.close()

    # 5. Interruptible replay resumes and still reaches the baseline.
    from wckpt.models import Checkpoint
    interrupt_engine, _ = replay_prefix(
        os.path.join(RUN, "interrupt_a"), config, large["records"][:8000]
    )
    cp0 = Checkpoint(seq=0, start_lsn=1, pages={
        int(k): Page.from_dict(v)
        for k, v in initial["pages"].items()
        if v.get("digest") is not None
    }).seal()
    interrupted = interrupt_engine.replayer.replay(cp0, interrupt_after=1234)
    resumed = interrupt_engine.replayer.replay(
        cp0, already_applied_upto=interrupted.end_lsn
    )
    resume_pages = {k: v.to_dict() for k, v in resumed.final_pages.items()}
    full_engine, full_result = replay_prefix(
        os.path.join(RUN, "interrupt_b"), config, large["records"][:8000]
    )
    full_pages = {k: v.to_dict() for k, v in full_result.final_pages.items()}
    step("interruptible_replay", {
        "interrupted_end_lsn": interrupted.end_lsn,
        "resumed_applied": resumed.applied_records,
        "final_equal_uninterrupted": resume_pages == full_pages,
    })
    interrupt_engine.close()
    full_engine.close()

    # 6. Checkpoint, replay idempotency, incremental checkpoint on small workload.
    engine = Engine(os.path.join(RUN, "small"), config)
    engine.open()
    cp = engine.checkpoint(incremental=False)["result"]
    first_replay = engine.replay()["result"]
    snapshot_after_first = engine.snapshot_pages()
    second_replay = engine.replay()["result"]
    snapshot_after_second = engine.snapshot_pages()
    expected = {int(k): v for k, v in small["expected_final_digests"].items()}
    digest_match = all(snapshot_after_first[p]["digest"] == d for p, d in expected.items())
    step("checkpoint_and_idempotent_replay", {
        "path": first_replay["decision"]["path"],
        "post_checkpoint_records": first_replay["applied_records"],
        "idempotent": snapshot_after_first == snapshot_after_second,
        "digest_match_baseline": digest_match,
        "checkpoint_seq": cp["seq"],
        "checkpoint_start_lsn": cp["start_lsn"],
    })

    feed(engine, small["records"][:40])
    inc_cp = engine.checkpoint(incremental=True)["result"]
    step("incremental_checkpoint", {
        "mode": inc_cp["mode"],
        "seq": inc_cp["seq"],
        "start_lsn": inc_cp["start_lsn"],
        "flushed_pages": inc_cp["flushed_pages"],
        "checksum_present": bool(inc_cp["checksum"]),
    })

    # 7. Truncation: early checkpoint boundary leaves required segments -> unsafe.
    early = fresh_engine(os.path.join(RUN, "truncate"), config)
    seed_pages(early, initial)
    feed(early, small["records"][:60])
    early.checkpoint(incremental=False)
    feed(early, small["records"][60:120])
    try:
        early.truncate_wal()
        trunc = {"code": None}
    except TruncateUnsafeError as exc:
        trunc = {
            "code": exc.code,
            "safe_lsn": exc.details["safe_lsn"],
            "uncovered_count": len(exc.details["uncovered"]),
            "uncovered_sample": exc.details["uncovered"][:2],
        }
    step("truncate_safety", trunc)
    # After a new checkpoint advances past every segment, truncation archives them.
    early.checkpoint(incremental=False)
    safe_result = early.truncate_wal()["result"]
    step("truncate_after_coverage", {
        "archived_segments": safe_result["archived_segments"],
        "remaining_segments": len(early.wal_segments()),
        "archive_files": sorted(os.listdir(early.archive_dir)),
    })
    early.close()

    with open(REPORT, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)
    print("\nreport written to", REPORT)


if __name__ == "__main__":
    main()
