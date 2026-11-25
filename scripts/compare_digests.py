import json
import sys

run_a, run_b, exp_a, exp_b = sys.argv[1:5]
ra, rb = json.load(open(run_a)), json.load(open(run_b))
ea, eb = json.load(open(exp_a)), json.load(open(exp_b))

checks = {
    "version fingerprint": ra["fingerprints"]["version_fingerprint"] == rb["fingerprints"]["version_fingerprint"],
    "calib manifest": ra["calib_manifest_digest"] == rb["calib_manifest_digest"],
    "layer artifacts": [x["artifact_digest"] for x in ra["artifacts"]]
    == [x["artifact_digest"] for x in rb["artifacts"]],
    "export digest": ea["digest"] == eb["digest"],
}
for name, ok in checks.items():
    print(f"[{'OK' if ok else 'FAIL'}] {name}")
    if not ok:
        sys.exit(1)
