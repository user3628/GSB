from __future__ import annotations

import json
from pathlib import Path

import numpy as np


SEED = 20261122


def main() -> None:
    root = Path(__file__).resolve().parent
    rng = np.random.default_rng(SEED)
    tensor = rng.standard_normal((8, 16), dtype=np.float32)
    np.save(root / "input_tensor.npy", tensor)
    payload = {
        "seed": SEED,
        "dtype": "float32",
        "shape": list(tensor.shape),
        "sha256": __import__("hashlib").sha256(tensor.tobytes()).hexdigest(),
        "values": tensor.tolist(),
    }
    with open(root / "input_tensor.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


if __name__ == "__main__":
    main()

