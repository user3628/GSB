from __future__ import annotations

from dataclasses import replace

import pytest

from embedding_gather.codec import (
    decode_manifest,
    decode_v2,
    encode_v1,
    encode_v2,
    read_manifest_file,
    semantic_signature,
)
from embedding_gather.errors import E_DIM_MISMATCH, E_PAD_OVERLAP, BusinessError
from embedding_gather.model import V1UpgradeConfig
from embedding_gather.weights import load_manifest_facts


def test_v1_upgrade_corrects_percentages_offsets_dimensions_and_pad(sample_copy):
    manifest = read_manifest_file(sample_copy / "manifest_v1.json")
    facts = load_manifest_facts(manifest, sample_copy)
    project = decode_manifest(manifest, facts=facts, v1_config=V1UpgradeConfig())

    assert project.structure_version == 1
    assert project.embedding_dim == 8
    assert [(shard.row_start, shard.row_end, shard.row_offset) for shard in project.shards] == [
        (0, 32, 0),
        (32, 64, 32),
        (64, 96, 64),
    ]
    assert [shard.pad_ranges for shard in project.shards] == [((28, 32),), (), ((92, 96),)]
    assert all("corrected embedding_dim 16 -> 8" in "\n".join(shard.notes) for shard in project.shards)
    assert any("recovered 1 pad range" in note for note in project.provenance)


def test_percentage_rounding_is_configurable(sample_copy):
    manifest = read_manifest_file(sample_copy / "manifest_v1.json")
    facts = load_manifest_facts(manifest, sample_copy)
    floor_project = decode_manifest(
        manifest,
        facts=facts,
        v1_config=V1UpgradeConfig(
            percentage_rounding="floor",
            total_rows=96,
            recover_pad_from="none",
        ),
    )
    assert [(shard.row_start, shard.row_end) for shard in floor_project.shards] == [
        (0, 31),
        (31, 63),
        (63, 96),
    ]


def test_v1_and_v2_roundtrips_preserve_offsets_and_pad(sample_copy):
    v1 = read_manifest_file(sample_copy / "manifest_v1.json")
    facts = load_manifest_facts(v1, sample_copy)
    project = decode_manifest(v1, facts=facts, v1_config=V1UpgradeConfig())

    through_v2 = decode_manifest(encode_v2(project), facts=facts)
    downgraded = encode_v1(project)
    through_v1 = decode_manifest(downgraded, facts=facts, v1_config=V1UpgradeConfig())

    assert semantic_signature(project) == semantic_signature(through_v2)
    assert semantic_signature(project) == semantic_signature(through_v1)
    assert "_compat" in downgraded
    assert all("row_offset" not in shard for shard in downgraded["shards"])
    assert all("pad_ranges" not in shard for shard in downgraded["shards"])
    restored = {shard.shard_id: shard for shard in through_v1.shards}
    assert restored["shard-002"].row_offset == 64
    assert restored["shard-002"].pad_ranges == ((92, 96),)


def test_declared_offsets_are_not_lost_in_v2(sample_copy):
    manifest = read_manifest_file(sample_copy / "manifest_v2.json")
    facts = load_manifest_facts(manifest, sample_copy)
    project = decode_v2(manifest, facts=facts)
    encoded = encode_v2(project)
    assert [shard["row_offset"] for shard in encoded["shards"]] == [0, 32, 64]
    assert [shard["pad_marker"] for shard in encoded["shards"]] == [1.0, 1.0, 1.0]


def test_pad_overlap_is_rejected(sample_copy):
    manifest = read_manifest_file(sample_copy / "manifest_v2.json")
    facts = load_manifest_facts(manifest, sample_copy)
    project = decode_v2(manifest, facts=facts)
    first = project.shards[0]
    overlapping = replace(
        project.shards[1],
        row_start=30,
        row_end=64,
        row_offset=30,
    )
    broken = replace(project, shards=(first, overlapping, project.shards[2]))
    with pytest.raises(BusinessError) as error:
        broken.validate()
    assert error.value.code == E_PAD_OVERLAP
    assert set(error.value.context["shard_ids"]) == {"shard-000", "shard-001"}


def test_dimension_mismatch_is_rejected():
    shard = replace(
        decode_v2(
            read_manifest_file("samples/main/manifest_v2.json"),
            facts=load_manifest_facts(read_manifest_file("samples/main/manifest_v2.json"), "samples/main"),
        ).shards[0],
        embedding_dim=7,
    )
    project = decode_v2(
        read_manifest_file("samples/main/manifest_v2.json"),
        facts=load_manifest_facts(read_manifest_file("samples/main/manifest_v2.json"), "samples/main"),
    )
    with pytest.raises(BusinessError) as error:
        replace(project, shards=(shard,) + project.shards[1:]).validate()
    assert error.value.code == E_DIM_MISMATCH
    assert error.value.context["shard_id"] == "shard-000"
