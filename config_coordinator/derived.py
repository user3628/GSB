"""Derived artifacts, their references, and cascade invalidation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .codec import digest_json, expected_config_digest
from .models import ComponentDeclaration


@dataclass
class DerivedArtifact:
    artifact_key: str
    artifact_type: str
    artifact_id: str
    value: dict[str, Any]
    refs: list[str]
    digest: str
    invalidated: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "artifact_key": self.artifact_key,
            "artifact_type": self.artifact_type,
            "artifact_id": self.artifact_id,
            "value": self.value,
            "refs": self.refs,
            "digest": self.digest,
            "invalidated": self.invalidated,
        }


@dataclass
class DerivedBuild:
    artifacts: list[DerivedArtifact]
    expected_digests: dict[str, str]
    invalidated_artifacts: list[str] = field(default_factory=list)

    def by_key(self) -> dict[str, DerivedArtifact]:
        return {artifact.artifact_key: artifact for artifact in self.artifacts}


def _artifact_reference(artifact: DerivedArtifact) -> str:
    return f"artifact:{artifact.artifact_key}"


def build_derived_artifacts(
    *,
    version: int,
    configs: dict[str, Any],
    components: list[ComponentDeclaration],
    states: dict[str, dict[str, Any]],
    batch_indices: dict[str, int],
    changed_refs: Iterable[str] = (),
) -> DerivedBuild:
    """Build all three artifact families for a complete snapshot version.

    All rows are intended to be written by one SQLite transaction.  The
    ``invalidated`` flag and returned list only describe the dependency
    closure; they never represent a partially published version.
    """

    component_map = {component.component_id: component for component in components}
    expected: dict[str, str] = {}
    artifacts: list[DerivedArtifact] = []

    for component in sorted(components, key=lambda item: item.component_id):
        expected_digest = expected_config_digest(
            component.config_keys,
            component.expected_config_version,
            configs,
        )
        expected[component.component_id] = expected_digest
        refs = [f"component:{component.component_id}"] + [
            f"config:{key}" for key in sorted(set(component.config_keys))
        ]
        value = {
            "component_id": component.component_id,
            "digest": expected_digest,
            "config_keys": sorted(set(component.config_keys)),
            "expected_config_version": component.expected_config_version,
        }
        artifacts.append(
            DerivedArtifact(
                artifact_key=f"expected_digest:{component.component_id}",
                artifact_type="expected_digest",
                artifact_id=component.component_id,
                value=value,
                refs=refs,
                digest=digest_json(value),
            )
        )

    logical_groups: dict[str, list[str]] = {}
    for component in components:
        logical_groups.setdefault(component.logical_id, []).append(component.component_id)

    for component in sorted(components, key=lambda item: item.component_id):
        group = sorted(logical_groups[component.logical_id])
        matched = sum(
            1
            for member_id in group
            if (states.get(member_id) or {}).get("last_applied_digest") == expected[member_id]
        )
        total = len(group)
        score = matched / total if total else 0.0
        refs = sorted(
            {f"component:{member_id}" for member_id in group}
            | {f"artifact:expected_digest:{member_id}" for member_id in group}
            | {f"state:{member_id}" for member_id in group}
        )
        value = {
            "component_id": component.component_id,
            "logical_component": component.logical_id,
            "matched": matched,
            "total": total,
            "score": score,
        }
        artifacts.append(
            DerivedArtifact(
                artifact_key=f"consistency_score:{component.component_id}",
                artifact_type="consistency_score",
                artifact_id=component.component_id,
                value=value,
                refs=refs,
                digest=digest_json(value),
            )
        )

    for component in sorted(components, key=lambda item: item.component_id):
        batch_index = int(batch_indices.get(component.component_id, -1))
        refs = sorted(
            {f"component:{component.component_id}", f"artifact:expected_digest:{component.component_id}"}
            | {f"component:{dependency}" for dependency in component.depends_on}
        )
        value = {
            "component_id": component.component_id,
            "batch_index": batch_index,
            "logical_component": component.logical_id,
        }
        artifacts.append(
            DerivedArtifact(
                artifact_key=f"batch_index:{component.component_id}",
                artifact_type="batch_index",
                artifact_id=component.component_id,
                value=value,
                refs=refs,
                digest=digest_json(value),
            )
        )

    changed = set(changed_refs)
    invalidated: set[str] = set()
    # A fixpoint is needed because consistency and batch artifacts depend on
    # expected-digest artifacts, which may in turn be invalidated by a config.
    while True:
        newly_invalidated = {
            artifact.artifact_key
            for artifact in artifacts
            if artifact.artifact_key not in invalidated and changed.intersection(artifact.refs)
        }
        if not newly_invalidated:
            break
        invalidated.update(newly_invalidated)
        for artifact in artifacts:
            if artifact.artifact_key in newly_invalidated:
                changed.add(_artifact_reference(artifact))

    for artifact in artifacts:
        artifact.invalidated = artifact.artifact_key in invalidated

    return DerivedBuild(
        artifacts=artifacts,
        expected_digests=expected,
        invalidated_artifacts=sorted(invalidated),
    )


def changed_refs_for(
    changed_config_keys: Iterable[str], changed_component_ids: Iterable[str]
) -> list[str]:
    return sorted(
        {f"config:{key}" for key in changed_config_keys}
        | {f"component:{component_id}" for component_id in changed_component_ids}
    )


def inferred_affected_from_artifacts(
    artifacts: list[DerivedArtifact], changed_refs: Iterable[str]
) -> list[str]:
    refs = set(changed_refs)
    return sorted(
        {
            artifact.artifact_id
            for artifact in artifacts
            if artifact.artifact_type == "expected_digest" and refs.intersection(artifact.refs)
        }
    )
