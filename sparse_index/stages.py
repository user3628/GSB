"""The four deterministic build stages."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Callable, Iterable

from .codec import FILE_MAGIC, decode_block, encode_block
from .config import STAGES, IndexConfig
from .errors import GracefulStop, error
from .tokenizer import read_jsonl_documents, tokenize_documents, tokenizer_metadata
from .utils import atomic_write_bytes, atomic_write_json, canonical_json_bytes, sha256_bytes


StopCheck = Callable[[], bool]


def _check_stop(stop_check: StopCheck | None) -> None:
    if stop_check is not None and stop_check():
        raise GracefulStop("收到停止请求，已在安全单元边界退出")


def stage1_tokenize(
    config: IndexConfig,
    stage_dir: Path,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    raw_documents = read_jsonl_documents(config.shard_paths)
    if not raw_documents:
        raise error("E_DATA", "文档分片为空")
    terms, tokenized = tokenize_documents(
        raw_documents,
        config.stopwords,
        stop_check=stop_check,
    )
    # Vocabulary numbering is a deterministic corpus-wide fold.  A stop may
    # still land immediately before the atomic artifact publication.
    _check_stop(stop_check)
    documents_payload = [
        {
            "doc_id": document.doc_id,
            "title": document.title,
            "body": document.body,
            "lang": document.lang,
            "source": document.source,
            "empty_body": document.empty_body,
            "doc_length": document.doc_length,
            "title_terms": document.title_terms,
            "body_terms": document.body_terms,
            "term_freqs": document.term_freqs,
        }
        for document in tokenized
    ]
    payload = {
        "schema_version": 1,
        "stage": "tokenize",
        "stage_number": 1,
        "index_version": config.index_version,
        "stage_fingerprint": config.stage_fingerprint(1),
        "input_digest": config.dataset_digest,
        "tokenizer": tokenizer_metadata(config),
        "terms": terms,
        "documents": documents_payload,
    }
    stage_dir.mkdir(parents=True, exist_ok=True)
    path = stage_dir / "terms.json"
    atomic_write_bytes(path, canonical_json_bytes(payload))
    return payload


def load_stage1(stage_dir: Path) -> dict[str, Any]:
    from .utils import load_json

    payload = load_json(stage_dir / "terms.json")
    if not isinstance(payload, dict) or payload.get("stage") != "tokenize":
        raise error("E_INVALID_STAGE", "缺少或损坏阶段 1 产物", stage="tokenize")
    return payload


def stage2_statistics(
    config: IndexConfig,
    stage1_dir: Path,
    stage_dir: Path,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    config.require_stage2_parameters()
    stage1 = load_stage1(stage1_dir)
    documents = stage1["documents"]
    terms = stage1["terms"]
    document_count = len(documents)
    if document_count <= 0:
        raise error("E_DATA", "阶段 2 没有可用文档")
    total_document_length = sum(int(document["doc_length"]) for document in documents)
    average_document_length = total_document_length / document_count
    document_lengths = {
        str(document["doc_id"]): int(document["doc_length"])
        for document in documents
    }
    dictionary: dict[int, list[tuple[str, int]]] = {int(term["term_id"]): [] for term in terms}
    for document in documents:
        for term_id, frequency in document["term_freqs"]:
            dictionary[int(term_id)].append((document["doc_id"], int(frequency)))
    k1 = float(config.value("bm25.k1", 1.2))
    b = float(config.value("bm25.b", 0.75))
    scale = int(config.value("quantization.scale", 10000))
    statistic_terms: list[dict[str, Any]] = []
    for term in terms:
        _check_stop(stop_check)
        term_id = int(term["term_id"])
        postings_for_term = sorted(dictionary.get(term_id, []), key=lambda item: item[0])
        document_frequency = len(postings_for_term)
        if document_frequency <= 0:
            raise error(
                "E_WEIGHT_NUMERIC",
                "词项在所有分片中均未出现",
                doc_id="<none>",
                term_id=term_id,
            )
        idf = math.log(1.0 + (document_count - document_frequency + 0.5) / (document_frequency + 0.5))
        quantized_postings: list[list[Any]] = []
        for doc_id, frequency in postings_for_term:
            document_length = document_lengths[doc_id]
            denominator = frequency + k1 * (
                1.0 - b + b * (document_length / average_document_length)
            )
            try:
                raw_weight = idf * (frequency * (k1 + 1.0)) / denominator
            except (ZeroDivisionError, OverflowError):
                raw_weight = float("nan")
            if not math.isfinite(float(raw_weight)):
                raise error(
                    "E_WEIGHT_NUMERIC",
                    "BM25 权重出现 NaN 或 Inf",
                    doc_id=doc_id,
                    term_id=term_id,
                    raw_weight=str(raw_weight),
                )
            quantized = int(math.floor(raw_weight * scale + 0.5))
            quantized_postings.append([doc_id, quantized])
        statistic_terms.append(
            {
                "term_id": term_id,
                "term": term["term"],
                "document_frequency": document_frequency,
                "idf": idf,
                "quant_scale": scale,
                "postings": quantized_postings,
            }
        )
    documents_payload = [
        {
            "doc_id": document["doc_id"],
            "doc_length": int(document["doc_length"]),
            "empty_body": bool(document.get("empty_body", False)),
        }
        for document in documents
    ]
    payload = {
        "schema_version": 1,
        "stage": "statistics",
        "stage_number": 2,
        "index_version": config.index_version,
        "stage_fingerprint": config.stage_fingerprint(2),
        "input_digest": config.stage_fingerprint(1),
        "bm25": {"k1": k1, "b": b},
        "quantization": {"scale": scale},
        "statistics": {
            "document_count": document_count,
            "total_document_length": total_document_length,
            "average_document_length": average_document_length,
            "term_count": len(statistic_terms),
            "empty_body_count": sum(1 for document in documents_payload if document["empty_body"]),
        },
        "terms": statistic_terms,
        "documents": documents_payload,
    }
    stage_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_json(stage_dir / "statistics.json", payload)
    return payload


def load_stage2(stage_dir: Path) -> dict[str, Any]:
    from .utils import load_json

    payload = load_json(stage_dir / "statistics.json")
    if not isinstance(payload, dict) or payload.get("stage") != "statistics":
        raise error("E_INVALID_STAGE", "缺少或损坏阶段 2 产物", stage="statistics")
    return payload


def stage3_postings(
    config: IndexConfig,
    stage2_dir: Path,
    stage_dir: Path,
    stop_check: StopCheck | None = None,
) -> dict[str, Any]:
    config.require_stage3_parameters()
    stage2 = load_stage2(stage2_dir)
    skip_step = int(config.value("postings.skip_step", 4))
    block_size = int(config.value("postings.block_size", 8))
    documents = stage2["documents"]
    doc_id_to_ordinal = {
        str(document["doc_id"]): index for index, document in enumerate(documents)
    }
    ordinal_to_doc_id = [str(document["doc_id"]) for document in documents]
    binary = bytearray(FILE_MAGIC)
    manifest_terms: list[dict[str, Any]] = []
    for term in stage2["terms"]:
        _check_stop(stop_check)
        postings = [
            (str(doc_id), int(weight)) for doc_id, weight in term["postings"]
        ]
        postings.sort(key=lambda item: item[0])
        blocks: list[dict[str, Any]] = []
        skips: list[dict[str, Any]] = []
        for start in range(0, len(postings), block_size):
            part = postings[start : start + block_size]
            block_bytes = encode_block(part, doc_id_to_ordinal)
            offset = len(binary)
            binary.extend(block_bytes)
            block_index = len(blocks)
            blocks.append(
                {
                    "block_index": block_index,
                    "offset": offset,
                    "length": len(block_bytes),
                    "posting_count": len(part),
                    "first_doc_id": part[0][0],
                    "last_doc_id": part[-1][0],
                    "crc32": int.from_bytes(block_bytes[9:13], "big"),
                }
            )
            decoded = decode_block(
                block_bytes,
                ordinal_to_doc_id,
                term_id=int(term["term_id"]),
                block_index=block_index,
            )
            if decoded != part:
                raise error(
                    "E_BLOCK_CORRUPTION",
                    "压缩分块回读结果与原始 posting 不一致",
                    term_id=int(term["term_id"]),
                    block_index=block_index,
                )
        for ordinal in range(0, len(postings), skip_step):
            skips.append(
                {
                    "ordinal": ordinal,
                    "doc_id": postings[ordinal][0],
                    "block_index": ordinal // block_size,
                }
            )
        manifest_terms.append(
            {
                "term_id": int(term["term_id"]),
                "term": term["term"],
                "document_frequency": int(term["document_frequency"]),
                "quant_scale": int(term["quant_scale"]),
                "skip_step": skip_step,
                "skips": skips,
                "blocks": blocks,
            }
        )
    binary_path = stage_dir / "postings.bin"
    stage_dir.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(binary_path, bytes(binary))
    manifest = {
        "schema_version": 1,
        "stage": "postings",
        "stage_number": 3,
        "index_version": config.index_version,
        "stage_fingerprint": config.stage_fingerprint(3),
        "input_digest": config.stage_fingerprint(2),
        "encoding": {
            "doc_id": "delta-leb128",
            "weight": "zigzag-delta-leb128",
            "block_header": "PBLK+version+length+crc32",
            "file_magic": FILE_MAGIC.decode("ascii", errors="replace"),
            "block_size": block_size,
            "skip_step": skip_step,
        },
        "document_order": ordinal_to_doc_id,
        "binary_file": "postings.bin",
        "binary_digest": sha256_bytes(bytes(binary)),
        "binary_size": len(binary),
        "terms": manifest_terms,
    }
    atomic_write_json(stage_dir / "postings.json", manifest)
    return manifest


def load_stage3(stage_dir: Path) -> tuple[dict[str, Any], Path]:
    from .utils import load_json

    manifest_path = stage_dir / "postings.json"
    binary_path = stage_dir / "postings.bin"
    manifest = load_json(manifest_path)
    if (
        not isinstance(manifest, dict)
        or manifest.get("stage") != "postings"
        or not binary_path.exists()
    ):
        raise error("E_INVALID_STAGE", "缺少或损坏阶段 3 产物", stage="postings")
    return manifest, binary_path


def verify_compressed_postings(config: IndexConfig, stage2_dir: Path, stage3_dir: Path) -> dict[str, Any]:
    """Read every compressed block and compare it with the raw stage-2 source."""

    stage2 = load_stage2(stage2_dir)
    manifest, binary_path = load_stage3(stage3_dir)
    raw_terms = {int(term["term_id"]): term for term in stage2["terms"]}
    ordinal_to_doc_id = list(manifest["document_order"])
    with binary_path.open("rb") as handle:
        if handle.read(len(FILE_MAGIC)) != FILE_MAGIC:
            raise error("E_BLOCK_CORRUPTION", "postings.bin 文件头不匹配")
        for term in manifest["terms"]:
            term_id = int(term["term_id"])
            expected_postings = [
                [str(doc_id), int(weight)] for doc_id, weight in raw_terms[term_id]["postings"]
            ]
            decoded_postings: list[list[Any]] = []
            for block in term["blocks"]:
                handle.seek(int(block["offset"]))
                block_bytes = handle.read(int(block["length"]))
                decoded = decode_block(
                    block_bytes,
                    ordinal_to_doc_id,
                    term_id=term_id,
                    block_index=int(block["block_index"]),
                )
                decoded_postings.extend([[doc_id, weight] for doc_id, weight in decoded])
            if decoded_postings != expected_postings:
                raise error(
                    "E_BLOCK_CORRUPTION",
                    "压缩倒排列表与原始 posting 不一致",
                    term_id=term_id,
                )
            for skip in term["skips"]:
                ordinal = int(skip["ordinal"])
                if expected_postings[ordinal][0] != skip["doc_id"]:
                    raise error(
                        "E_BLOCK_CORRUPTION",
                        "跳表 doc_id 与 posting 不一致",
                        term_id=term_id,
                        ordinal=ordinal,
                    )
    return {
        "ok": True,
        "term_count": len(manifest["terms"]),
        "block_count": sum(len(term["blocks"]) for term in manifest["terms"]),
        "skip_entry_count": sum(len(term["skips"]) for term in manifest["terms"]),
    }
