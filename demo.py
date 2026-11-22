"""End-to-end deterministic demo for the contract citation-chain component."""
from __future__ import annotations

import json
import os
import shutil

from contract_chain.codec import canonical_bytes, read_json
from contract_chain.config import PipelineConfig
from contract_chain.evidence import import_manifest, reconstruct_decisions
from contract_chain.pipeline import Pipeline
from contract_chain.samplegen import build_sample, write_workspace_bundle

WORKSPACE = "demo_workspace"
SEED = 20241123


def heading(title):
    print("\n=== %s ===" % title)


def main():
    if os.path.exists(WORKSPACE):
        shutil.rmtree(WORKSPACE)
    os.makedirs(WORKSPACE, exist_ok=True)
    paths = write_workspace_bundle(WORKSPACE, seed=SEED)
    heading("1. deterministic workspace inputs")
    for name, path in sorted(paths.items()):
        print("%-18s %s" % (name, path))

    sample = build_sample(SEED)
    pipeline = Pipeline(WORKSPACE)
    result = pipeline.run_full(sample)
    heading("2. stage fingerprints")
    for stage, fingerprint in sorted(result["fingerprints"].items()):
        print("%-10s %s" % (stage, fingerprint))

    detection = read_json(os.path.join(WORKSPACE, "stages", "detect.json"))
    parse = read_json(os.path.join(WORKSPACE, "stages", "parse.json"))
    normalize = read_json(os.path.join(WORKSPACE, "stages", "normalize.json"))
    resolve = read_json(os.path.join(WORKSPACE, "stages", "resolve.json"))

    heading("3. region detection and matching")
    print(json.dumps(detection["stats"], sort_keys=True))
    print("background diagnostics retained:", len(detection["diagnostics"]))

    heading("4. citation parsing")
    print("E_REF_PARSE refs:", [error["details"]["ref_id"] for error in parse["errors"]])

    heading("5. fixed-point term normalization")
    for error in normalize["errors"]:
        print(
            error["code"],
            error["details"]["ref_id"],
            error["details"]["term"],
            error["details"]["reason"],
        )

    heading("6. conflict resolution")
    print(json.dumps(resolve["stats"], sort_keys=True))
    for group in resolve["groups"]:
        if group["conflict"]:
            print(
                group["group_id"],
                "winner=", group["winner_ref_id"],
                "terms=", ",".join(group["conflicting_terms"]),
            )
    for demotion in resolve["demotions"]:
        print("demoted", demotion["ref_id"], "->", demotion["winner_ref_id"], demotion["reason"])

    heading("7. evidence re-import and reconstruction")
    manifest = import_manifest(os.path.join(WORKSPACE, "evidence_manifest.json"))
    rebuilt = reconstruct_decisions(manifest)
    print("manifest records:", manifest["record_count"], "digest:", manifest["manifest_digest"][:16])
    print("reconstructed verdicts:", len(rebuilt))

    heading("8. incremental rebuild with changed region coordinates")
    changed = json.loads(json.dumps(sample))
    changed["pages"][0]["regions"][0]["xywh"][0] += 0.05
    incremental = pipeline.run_incremental(changed)
    print("earliest invalidated stage:", incremental["earliest_changed_stage"])
    print("byte parity:", incremental["parity"])

    heading("9. config-only change (language weight)")
    changed_config = PipelineConfig.from_dict(read_json(os.path.join(WORKSPACE, "config.json")))
    object.__setattr__(changed_config.resolve, "language_weight", {"zh": "1.0", "en": "0.8"})
    config_run = pipeline.run_incremental(new_config=changed_config)
    print("earliest invalidated stage:", config_run["earliest_changed_stage"])
    print("byte parity:", config_run["parity"])

    heading("10. byte determinism guard")
    compare_dir = WORKSPACE + "_compare"
    if os.path.exists(compare_dir):
        shutil.rmtree(compare_dir)
    comparison = Pipeline(compare_dir, pipeline.config)
    comparison.run_full(changed)
    for stage in ("detect", "parse", "normalize", "resolve"):
        left = canonical_bytes(pipeline.store.get_stage(stage))
        right = canonical_bytes(comparison.store.get_stage(stage))
        assert left == right, stage
        print(stage, "byte-identical")
    shutil.rmtree(compare_dir, ignore_errors=True)

    heading("checkpoint sequence")
    print([record["seq"] for record in pipeline.store.records()])
    print("\nDEMO OK")


if __name__ == "__main__":
    main()
