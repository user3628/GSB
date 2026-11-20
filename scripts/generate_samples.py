from __future__ import annotations

import json
import random
import shutil
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"
DOCUMENTS = SAMPLES / "documents"
BATCHES = SAMPLES / "batches"
SEED = 20261120

ADJECTIVES = ["local", "compact", "durable", "deterministic", "nested", "offline"]
NOUNS = ["ledger", "archive", "catalog", "gazette", "notebook", "atlas"]
VERBS = ["indexes", "tracks", "stores", "preserves", "resolves", "validates"]
TEAMS = ["storage", "search", "platform", "quality"]
TAGS = ["local", "index", "sqlite", "postings", "versioned", "offline", "nested"]


def document(seed: random.Random, number: int) -> dict[str, Any]:
    adjective = seed.choice(ADJECTIVES)
    noun = seed.choice(NOUNS)
    verb = seed.choice(VERBS)
    tags = seed.sample(TAGS, k=3)
    return {
        "_id": f"doc-{number:03d}",
        "title": f"{adjective.title()} {noun.title()} {number}",
        "body": (
            f"The {adjective} {noun} {verb} a local search index. "
            f"Phrase markers anchor stable postings for document {number:03d}."
        ),
        "tags": tags,
        "author": {
            "name": f"Author {seed.randint(1, 99):02d}",
            "team": seed.choice(TEAMS),
        },
        "sections": [
            {
                "title": f"Overview {number}",
                "paragraphs": [
                    f"A local index preserves version {seed.randint(1, 20)}.",
                    f"Array element {number}-a keeps deterministic positions.",
                ],
            },
            {
                "title": f"Appendix {number}",
                "paragraphs": [
                    f"Array element {number}-b remains independently searchable.",
                ],
            },
        ],
    }


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    if DOCUMENTS.exists():
        shutil.rmtree(DOCUMENTS)
    if BATCHES.exists():
        shutil.rmtree(BATCHES)
    DOCUMENTS.mkdir(parents=True)
    BATCHES.mkdir(parents=True)

    seed = random.Random(SEED)
    initial = [document(seed, number) for number in range(1, 13)]

    # Stable phrase fixture and a term that is intentionally absent initially.
    initial[0]["body"] = "A local search index stores deterministic postings for phrase checks."
    initial[0]["tags"] = ["local", "index", "phrase"]

    for item in initial:
        write_json(DOCUMENTS / f"{item['_id']}.json", item)

    write_json(
        BATCHES / "001_initial.json",
        {"batch_id": "sample-initial", "inserts": initial, "updates": [], "deletes": []},
    )

    update_two = json.loads(json.dumps(initial[1]))
    update_two["title"] = "Quantum Migration Ledger 2"
    update_two["tags"] = update_two["tags"] + ["quantum"]

    update_seven = json.loads(json.dumps(initial[6]))
    update_seven["tags"] = update_seven["tags"] + ["migrated"]

    inserted_thirteen = document(random.Random(SEED + 13), 13)
    inserted_thirteen["title"] = "Quantum Arrival Atlas 13"
    inserted_thirteen["tags"] = ["local", "quantum", "arrival"]
    write_json(DOCUMENTS / f"{inserted_thirteen['_id']}.json", inserted_thirteen)

    write_json(
        BATCHES / "002_incremental.json",
        {
            "batch_id": "sample-incremental",
            "inserts": [inserted_thirteen],
            "updates": [update_two, update_seven],
            "deletes": ["doc-004"],
        },
    )

    write_json(
        SAMPLES / "manifest.json",
        {
            "seed": SEED,
            "initial_batch": "batches/001_initial.json",
            "incremental_batch": "batches/002_incremental.json",
            "document_count_initial": len(initial),
            "document_count_after_increment": len(initial) - 1 + 1,
        },
    )
    print(f"Generated {len(initial)} initial documents with seed {SEED}")
    print("Wrote batches/001_initial.json and batches/002_incremental.json")


if __name__ == "__main__":
    main()
