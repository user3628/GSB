from __future__ import annotations

import json
from pathlib import Path

from embedding_gather.cli import main
from embedding_gather.codec import decode_manifest, encode_v1, read_manifest_file, semantic_signature
from embedding_gather.model import V1UpgradeConfig
from embedding_gather.samplegen import generate_all
from embedding_gather.weights import load_manifest_facts


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_sample_generation_is_byte_deterministic(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate_all(first)
    generate_all(second)
    assert _files(first) == _files(second)


def test_ragged_shard_roundtrip(ragged_copy):
    manifest = read_manifest_file(ragged_copy / "manifest_v1.json")
    facts = load_manifest_facts(manifest, ragged_copy)
    project = decode_manifest(manifest, facts=facts, v1_config=V1UpgradeConfig())
    assert [(shard.row_end - shard.row_start) for shard in project.shards] == [37, 33, 31]
    assert project.embedding_dim == 6
    roundtrip = decode_manifest(encode_v1(project), facts=facts, v1_config=V1UpgradeConfig())
    assert semantic_signature(project) == semantic_signature(roundtrip)


def test_error_cli_reports_stable_codes(tmp_path, capsys):
    code = main(
        [
            "errors",
            "--manifest",
            "samples/main/manifest_v2.json",
            "--root",
            "samples/main",
            "--workspace",
            str(tmp_path / "errors"),
            "--reset",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert code == 0
    assert {item["code"] for item in output} == {
        "E_EMPTY_BATCH",
        "E_INDEX_OOB",
        "E_INDEX_NONINTEGER",
        "E_NUMERIC",
    }


def test_demo_cli_reports_resume_equality(tmp_path, capsys):
    code = main(["demo", "--state", str(tmp_path / "demo")])
    output = json.loads(capsys.readouterr().out)
    assert code == 0
    assert output["codec_v1_roundtrip_semantics_equal"] is True
    assert output["codec_v2_to_v1_roundtrip_semantics_equal"] is True
    assert output["stop_resume_bundle_byte_equal"] is True
    assert output["incremental_invalidation"]["earliest_invalid_stage"] == "gather"
