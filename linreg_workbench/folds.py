"""Deterministic stratified fold construction."""

from __future__ import annotations

import random
from typing import Any

from .canonical import digest
from .errors import InvalidCV


def folds_fingerprint(preprocess_fingerprint: str, config: dict[str, Any]) -> str:
    return digest(
        {
            "kind": "folds-v1",
            "preprocess_fingerprint": preprocess_fingerprint,
            "folds": config["cv"]["folds"],
            "seed": config["cv"]["seed"],
            "stratify": config["cv"]["stratify"],
        }
    )


def make_folds(
    y_values: list[float], config: dict[str, Any], preprocess_fingerprint: str
) -> dict[str, Any]:
    y = [int(value) for value in y_values]
    k = int(config["cv"]["folds"])
    seed = int(config["cv"]["seed"])
    if k < 2:
        raise InvalidCV(
            "Cross-validation requires at least two folds",
            details={"fold": None, "reason": "folds must be >= 2", "folds": k},
        )
    if k > len(y):
        raise InvalidCV(
            "Cross-validation folds exceed sample count",
            details={"fold": None, "reason": "folds > rows", "folds": k, "rows": len(y)},
        )
    assignment = [-1] * len(y)
    if config["cv"]["stratify"]:
        for class_value in sorted(set(y)):
            indices = [index for index, value in enumerate(y) if value == class_value]
            rng = random.Random(seed + class_value * 100003)
            rng.shuffle(indices)
            for position, index in enumerate(indices):
                assignment[index] = position % k
    else:
        indices = list(range(len(y)))
        random.Random(seed).shuffle(indices)
        for position, index in enumerate(indices):
            assignment[index] = position % k

    folds: list[dict[str, Any]] = []
    for fold_index in range(k):
        validation = [i for i, assigned in enumerate(assignment) if assigned == fold_index]
        training = [i for i, assigned in enumerate(assignment) if assigned != fold_index]
        validation_counts = {
            "0": sum(1 for i in validation if y[i] == 0),
            "1": sum(1 for i in validation if y[i] == 1),
        }
        training_counts = {
            "0": sum(1 for i in training if y[i] == 0),
            "1": sum(1 for i in training if y[i] == 1),
        }
        fold_number = fold_index + 1
        if not validation:
            raise InvalidCV(
                "Validation fold is empty",
                details={"fold": fold_number, "reason": "empty validation fold"},
            )
        for label, key in ((0, "0"), (1, "1")):
            if validation_counts[key] == 0:
                raise InvalidCV(
                    "Validation fold contains a single class",
                    details={
                        "fold": fold_number,
                        "reason": f"validation class {label} is absent",
                        "class_counts": validation_counts,
                    },
                )
            if training_counts[key] == 0:
                raise InvalidCV(
                    "Training partition contains a single class",
                    details={
                        "fold": fold_number,
                        "reason": f"training class {label} is absent",
                        "class_counts": training_counts,
                    },
                )
        folds.append(
            {
                "fold": fold_number,
                "training_indices": training,
                "validation_indices": validation,
                "training_class_counts": training_counts,
                "validation_class_counts": validation_counts,
            }
        )
    artifact = {
        "artifact_type": "folds",
        "fingerprint": folds_fingerprint(preprocess_fingerprint, config),
        "preprocess_fingerprint": preprocess_fingerprint,
        "folds_count": k,
        "seed": seed,
        "stratified": bool(config["cv"]["stratify"]),
        "folds": folds,
    }
    artifact["fold_digest"] = digest(artifact["folds"])
    return artifact
