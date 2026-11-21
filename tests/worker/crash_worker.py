"""Subprocess worker that hard-exits at a chosen crash point."""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from wckpt import Config, Engine  # noqa: E402


def main():
    spec_path = sys.argv[1]
    with open(spec_path, "r", encoding="utf-8") as fh:
        spec = json.load(fh)

    root = spec["root"]
    crash_stage = spec["crash_stage"]
    records = spec["records"]
    seed = int(spec.get("seed_records", 0))
    crash_index = int(spec.get("crash_index", seed))

    state = {"appends": 0, "checkpoint_hooks": 0, "first_checkpoint_done": False}

    def hook(stage):
        if stage == "checkpoint" and crash_stage == "checkpoint":
            # The hook fires once per checkpoint(); let the first one commit.
            if state["first_checkpoint_done"]:
                os._exit(77)
        if stage.startswith("wal") and crash_stage.startswith("wal"):
            if state["appends"] == crash_index:
                os._exit(77)

    engine = Engine(root, Config.from_dict(spec["config"]), crash_hook=hook)
    engine.open()

    if seed:
        for rec in records[:seed]:
            engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"]
            )
        engine.checkpoint(incremental=False)
        state["first_checkpoint_done"] = True
        for rec in records[seed:seed + 12]:
            engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"]
            )

    if crash_stage.startswith("wal"):
        rec = records[crash_index]
        state["appends"] = crash_index
        engine.apply_modification(
            rec["page_no"], rec["after_digest"], rec["txn_id"]
        )
    elif crash_stage == "checkpoint":
        for rec in records[seed + 12:seed + 32]:
            engine.apply_modification(
                rec["page_no"], rec["after_digest"], rec["txn_id"]
            )
        engine.checkpoint(incremental=True)

    engine.close()


if __name__ == "__main__":
    main()
