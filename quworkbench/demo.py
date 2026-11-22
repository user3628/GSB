"""Deterministic end-to-end demonstration.

Scenario (all local, fixed seed):
  1. generate 3 shards + labeled samples + slot dictionary
  2. full run                     -> fixed summary digest
  3. single-shard replacement + single config change (incremental)
  4. verify incremental == full rebuild (byte-for-byte)
  5. stale publish attempt        -> E_STALE_CHECKPOINT, state unchanged
  6. simulated crash mid-journal  -> recovery -> rerun -> same fixed summary
"""

import json
import shutil
from pathlib import Path

from .errors import E_STALE_CHECKPOINT, WorkbenchError
from .generator import SEED, generate
from .pipeline import Pipeline


def _read_jsonl(path):
    return [json.loads(l) for l in Path(path).read_bytes().decode("utf-8").splitlines() if l.strip()]


def _simulate_crash(workspace):
    """Corrupt the tail of one journal and drop a stray temp artifact."""
    journal = Path(workspace) / "checkpoints" / "shard-001" / "classify" / "records.jsonl"
    with open(journal, "ab") as fh:
        fh.write(b'{"seq": 99, "stage": "classify", "shard_id": "shard-001", "con')  # truncated
    stray = Path(workspace) / "checkpoints" / "shard-001" / "classify" / ".artifact-000777.json.tmp"
    stray.write_bytes(b'{"stage": "classify", "partial": tr')
    return {"corrupted_journal": str(journal), "stray_tmp": str(stray)}


def run_demo(workspace):
    ws = Path(workspace)
    if ws.exists():
        shutil.rmtree(ws)
    ws.mkdir(parents=True)

    gen = generate(ws, seed=SEED)
    pipe = Pipeline(ws)

    full = pipe.run_full()
    full_digest = full["summary"]["digest"]

    variant = _read_jsonl(ws / "data" / "variants" / "shard-002-v2.jsonl")
    replaced = pipe.replace_shard("shard-002", variant)
    changed = pipe.change_config("classify.smoothing_alpha", 0.5)
    verify = pipe.verify_against_full()
    incremental_digest = verify["digest"]

    stale_error = None
    try:
        pipe.store.publish(
            "shard-000",
            "normalize",
            {
                "shard_id": "shard-000",
                "shard_fingerprint": pipe.shard_digest("shard-000"),
                "config_fingerprint": "sha256:stale",
                "input_digests": {},
            },
            {"stage": "normalize", "shard_id": "shard-000", "results": []},
            seq=999,
        )
    except WorkbenchError as exc:
        stale_error = exc.to_dict()

    crash = _simulate_crash(ws)
    recovery_report = pipe.recover()
    recovered = pipe.run()
    recovered_digest = recovered["summary"]["digest"]

    return {
        "seed": SEED,
        "generated": gen,
        "full_run": {
            "digest": full_digest,
            "counts": full["summary"]["counts"],
        },
        "incremental": {
            "replace_shard_invalidations": replaced["invalidations"],
            "config_change_invalidations": changed["invalidations"],
            "verify_match": verify["match"],
            "digest": incremental_digest,
        },
        "stale_publish_rejected": stale_error is not None
        and stale_error["code"] == E_STALE_CHECKPOINT,
        "crash": crash,
        "recovery": {
            "report": recovery_report,
            "digest": recovered_digest,
            "matches_incremental": recovered_digest == incremental_digest,
        },
        "final_summary": pipe.summary(),
    }
