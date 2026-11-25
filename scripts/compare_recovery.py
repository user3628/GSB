import json
import sys

continuous, recovered = sys.argv[1:3]
a, b = json.load(open(continuous)), json.load(open(recovered))
art_a = [x["artifact_digest"] for x in a["report"]["artifacts"]]
art_b = [x["artifact_digest"] for x in b["report"]["artifacts"]]
checks = {
    "recovered report digest": a["digest"] == b["digest"],
    "recovered artifacts": art_a == art_b,
    "audit fingerprint": a["report"]["audit"]["version_fingerprint"]
    == b["report"]["audit"]["version_fingerprint"],
}
for name, ok in checks.items():
    print(f"[{'OK' if ok else 'FAIL'}] {name}")
    if not ok:
        sys.exit(1)
