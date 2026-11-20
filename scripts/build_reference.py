"""Build local reference diagnostics from deterministic runs (no network)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from crwbench.config import load_config  # noqa: E402
from crwbench.pipeline import Pipeline  # noqa: E402
from crwbench.util import canonical_dumps, digest_file  # noqa: E402

CONFIG = os.path.join(ROOT, "config", "default_config.yaml")


def main() -> int:
    config = load_config(CONFIG)
    reference = {"config_fingerprint": config.fingerprint, "profiles": {}}
    for name in ("small", "medium", "large"):
        data_dir = os.path.join(ROOT, "data", f"demo_{name}")
        shards = sorted(
            os.path.join(data_dir, n) for n in os.listdir(data_dir) if n.endswith(".jsonl")
        )
        workdir = os.path.join(ROOT, "work", "demo_runs", name)
        pipe = Pipeline(config, workdir)
        report = pipe.run(shards)
        final = pipe.store.load_artifact("summary")
        normalized = pipe.store.load_artifact("normalize")
        reference["profiles"][name] = {
            "shard_digests": [digest_file(p) for p in shards],
            "strategy": final["summary"]["strategy"],
            "tier": final["tier"],
            "committed_judgments": final["summary"]["committed_judgments"],
            "level_distribution": final["summary"]["level_distribution"],
            "empty_text_diagnostics": sum(
                1 for n in normalized if any(d["code"] == "E_EMPTY_TEXT" for d in n["diagnostics"])
            ),
            "summary_report_digest": report["final_report_digest"],
        }
    out = os.path.join(ROOT, "config", "reference_diagnostics.json")
    with open(out, "wb") as fh:
        fh.write(canonical_dumps(reference))
    print("reference diagnostics written:", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
