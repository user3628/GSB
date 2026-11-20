import multiprocessing as mp
import os
import tempfile
import unittest

from diarwb.config import default_config
from diarwb.datagen import build_demo_dataset
from diarwb.errors import EStaleVersion
from diarwb.workbench import Workbench, read_json


def _publish(root, new_name, queue):
    wb = Workbench(root, default_config())
    wb.import_workspace()
    try:
        result = wb.publish_edit(
            "shard-000", "rename_speaker",
            {"old": "S1", "new": new_name}, expected_version=0)
        queue.put(("ok", result["version"], new_name))
    except EStaleVersion as exc:
        queue.put(("stale", exc.details["current_version"], new_name))
    except Exception as exc:  # pragma: no cover
        queue.put(("error", str(exc), new_name))


class ConcurrencyTests(unittest.TestCase):
    def test_file_lock_serializes_competing_publishes(self):
        root = tempfile.mkdtemp(prefix="diarwb-lock-")
        build_demo_dataset(root)
        ctx = mp.get_context("spawn")
        queue = ctx.Queue()
        procs = [ctx.Process(target=_publish, args=(root, name, queue))
                 for name in ("ALICE", "BOB")]
        for p in procs:
            p.start()
        outcomes = [queue.get() for _ in procs]
        for p in procs:
            p.join()
        statuses = sorted(o[0] for o in outcomes)
        self.assertEqual(statuses, ["ok", "stale"])
        winner = next(o for o in outcomes if o[0] == "ok")
        self.assertEqual(winner[1], 1)
        timeline = read_json(os.path.join(root, "timelines",
                                          "shard-000.json"))
        self.assertEqual(timeline["version"], 1)
        self.assertIn(winner[2],
                      {s["speaker"] for s in timeline["segments"]})
        # The loser never corrupted the published timeline.
        names = {s["speaker"] for s in timeline["segments"]}
        self.assertNotIn("ALICE" if winner[2] == "BOB" else "BOB", names)


if __name__ == "__main__":
    unittest.main()
