#!/usr/bin/env python3
"""Run the bundled sample end to end and print the fixed run digest."""

from __future__ import annotations

import json
from pathlib import Path

from matgen.config import load_config
from matgen.pipeline import generate_all


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    result = generate_all(
        workspace=root,
        recipes_path=root / "samples" / "recipes.json",
        cards_path=root / "samples" / "color_cards.json",
        config=load_config(root / "samples" / "config.json"),
        output_root=root / "out",
    )
    print(
        json.dumps(
            {
                "status": result["status"],
                "run_digest": result["run_digest"],
                "material_ids": result["completed_material_ids"],
                "output_root": result["output_root"],
            },
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

