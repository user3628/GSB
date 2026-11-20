"""Generate deterministic local samples and a SHA-256 manifest.

No network or object storage is involved. Re-running the script reproduces
byte-identical files because content is derived from a seeded PRNG and fixed
ASCII blocks.
"""
from __future__ import annotations

import hashlib
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SAMPLES = ROOT / "samples"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4096), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build() -> dict:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    rng = random.Random(20261120)

    # Sample object A: four fixed-size parts with deterministic pseudo-bytes.
    manifest = {"objects": [], "generated_by": "scripts/make_samples.py", "seed": 20261120}
    for obj_name, part_sizes in [
        ("alpha", [256, 256, 256, 128]),       # last part intentionally smaller
        ("bravo", [128, 128, 128]),            # used for missing-part scenarios
    ]:
        obj_dir = SAMPLES / obj_name
        obj_dir.mkdir(exist_ok=True)
        entries = []
        for index, size in enumerate(part_sizes, start=1):
            # Mix an ASCII header with deterministic random bytes.
            header = f"obj={obj_name};part={index:03d};size={size:05d}\n".encode()
            payload = bytes(rng.randrange(256) for _ in range(size - len(header)))
            content = header + payload
            part_path = obj_dir / f"part_{index:03d}.bin"
            part_path.write_bytes(content)
            entries.append({
                "part_number": index,
                "file": str(part_path.relative_to(ROOT)),
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            })
        total = sum(e["size_bytes"] for e in entries)
        object_digest = hashlib.sha256(
            b"".join(e["sha256"].encode("ascii") for e in entries)
        ).hexdigest()
        manifest["objects"].append({
            "object_key": f"demo/{obj_name}.obj",
            "directory": str(obj_dir.relative_to(ROOT)),
            "expected_total_size": total,
            "part_count": len(entries),
            "expected_object_sha256": object_digest,
            "parts": entries,
        })

    manifest_path = SAMPLES / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


if __name__ == "__main__":
    data = build()
    print(f"Wrote {len(data['objects'])} sample objects to {SAMPLES}")
    for obj in data["objects"]:
        print(f"  {obj['object_key']}: {obj['part_count']} parts, "
              f"{obj['expected_total_size']} bytes, sha256={obj['expected_object_sha256']}")
