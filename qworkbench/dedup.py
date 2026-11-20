from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .config import DedupConfig
from .util import normalize_text, sha256_text, text_tokens


BUCKET_BITS = 16
BUCKET_COUNT = 4
SIMHASH_BITS = 64


def token_hash(token: str) -> int:
    return int.from_bytes(hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest(), "big")


def simhash64(tokens: Iterable[str]) -> int:
    counts = Counter(tokens)
    if not counts:
        return 0
    vector = [0] * SIMHASH_BITS
    for token in sorted(counts):
        weight = counts[token]
        value = token_hash(token)
        for bit in range(SIMHASH_BITS):
            vector[bit] += weight if value & (1 << bit) else -weight
    result = 0
    for bit, value in enumerate(vector):
        if value >= 0:
            result |= 1 << bit
    return result


def simhash_buckets(value: int) -> tuple[int, int, int, int]:
    if value < 0 or value >= (1 << SIMHASH_BITS):
        raise ValueError("SimHash value must be an unsigned 64-bit integer")
    return tuple((value >> (BUCKET_BITS * index)) & 0xFFFF for index in range(BUCKET_COUNT))


def hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def normalize_record(sample: Mapping[str, Any], separator: str) -> dict[str, Any]:
    instruction = normalize_text(str(sample["instruction"]))
    input_text = normalize_text(str(sample["input"]))
    output = normalize_text(str(sample["output"]))
    joined = separator.join((instruction, input_text, output))
    value = simhash64(text_tokens(joined))
    buckets = simhash_buckets(value)
    return {
        "id": str(sample["id"]),
        "instruction": instruction,
        "input": input_text,
        "output": output,
        "source": str(sample["source"]),
        "lang": str(sample["lang"]),
        "normalized_concat": joined,
        "normalized_sha256": sha256_text(joined),
        "simhash_hex": f"{value:016x}",
        "simhash_uint": value,
        "buckets": list(buckets),
        "token_count": len(text_tokens(joined)),
        "whitespace_output_tokens": len(output.split()),
    }


class _UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, value: int) -> int:
        while self.parent[value] != value:
            self.parent[value] = self.parent[self.parent[value]]
            value = self.parent[value]
        return value

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            root_left, root_right = root_right, root_left
        self.parent[root_right] = root_left
        if self.rank[root_left] == self.rank[root_right]:
            self.rank[root_left] += 1


def deduplicate(
    samples: list[Mapping[str, Any]],
    config: DedupConfig,
    stop_checker: Callable[[], None] | None = None,
) -> dict[str, Any]:
    check = stop_checker or (lambda: None)
    records: list[dict[str, Any]] = []
    for sample in samples:
        records.append(normalize_record(sample, config.separator))
        check()
    records.sort(key=lambda record: (record["id"], record["normalized_sha256"]))

    bucket_map: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        for bucket_index, bucket_value in enumerate(record["buckets"]):
            bucket_map[(bucket_index, bucket_value)].append(index)
        check()

    candidate_pairs: set[tuple[int, int]] = set()
    for members in bucket_map.values():
        if len(members) < 2:
            continue
        for left_pos in range(len(members)):
            for right_pos in range(left_pos + 1, len(members)):
                candidate_pairs.add((members[left_pos], members[right_pos]))

    matches: list[dict[str, Any]] = []
    neighbors: dict[int, set[int]] = defaultdict(set)
    union_find = _UnionFind(len(records))
    for left, right in sorted(candidate_pairs):
        distance = hamming_distance(records[left]["simhash_uint"], records[right]["simhash_uint"])
        if distance <= config.hamming_threshold:
            union_find.union(left, right)
            neighbors[left].add(right)
            neighbors[right].add(left)
            matches.append(
                {
                    "left_id": records[left]["id"],
                    "right_id": records[right]["id"],
                    "hamming_distance": distance,
                    "exact_normalized": records[left]["normalized_sha256"]
                    == records[right]["normalized_sha256"],
                }
            )
        check()

    components: dict[int, list[int]] = defaultdict(list)
    for index in range(len(records)):
        components[union_find.find(index)].append(index)

    groups: list[dict[str, Any]] = []
    retained_indices: set[int] = set()
    index_by_id = {record["id"]: index for index, record in enumerate(records)}
    duplicate_count = 0
    exact_duplicate_group_count = 0
    near_duplicate_group_count = 0
    for component in components.values():
        remaining = sorted(component, key=lambda index: records[index]["id"])
        while remaining:
            representative_index = remaining[0]
            representative = records[representative_index]
            members = [representative_index]
            members.extend(
                index
                for index in remaining[1:]
                if index in neighbors.get(representative_index, set())
            )
            remaining = [index for index in remaining if index not in set(members)]
            retained_indices.add(representative_index)
            if len(members) == 1:
                check()
                continue
            member_records = sorted(
                (records[index] for index in members), key=lambda record: record["id"]
            )
            duplicate_count += len(member_records) - 1
            hashes = {record["normalized_sha256"] for record in member_records}
            exact_only = len(hashes) == 1
            if exact_only:
                exact_duplicate_group_count += 1
            else:
                near_duplicate_group_count += 1
            member_ids = [record["id"] for record in member_records]
            group_id = "dup-" + hashlib.sha256("\0".join(member_ids).encode("utf-8")).hexdigest()[:12]
            groups.append(
                {
                    "group_id": group_id,
                    "retained_id": representative["id"],
                    "member_ids": member_ids,
                    "removed_ids": [sample_id for sample_id in member_ids if sample_id != representative["id"]],
                    "kind": "exact" if exact_only else "near",
                    "max_hamming_to_representative": max(
                        hamming_distance(representative["simhash_uint"], record["simhash_uint"])
                        for record in member_records
                    ),
                }
            )
            check()
    groups.sort(key=lambda group: group["retained_id"])

    retained_records = sorted(
        (records[index] for index in retained_indices), key=lambda record: record["id"]
    )
    matches.sort(key=lambda item: (item["left_id"], item["right_id"]))
    source_lang_counts: dict[str, int] = defaultdict(int)
    for record in retained_records:
        source_lang_counts[f"{record['source']}::{record['lang']}"] += 1

    return {
        "records": retained_records,
        "groups": groups,
        "matches": matches,
        "summary": {
            "input_samples": len(records),
            "retained_samples": len(retained_records),
            "removed_samples": duplicate_count,
            "duplicate_groups": len(groups),
            "exact_duplicate_groups": exact_duplicate_group_count,
            "near_duplicate_groups": near_duplicate_group_count,
            "candidate_pairs": len(candidate_pairs),
            "confirmed_pairs": len(matches),
            "retained_by_source_lang": dict(sorted(source_lang_counts.items())),
            "simhash": {
                "bits": SIMHASH_BITS,
                "bucket_count": BUCKET_COUNT,
                "bucket_bits": BUCKET_BITS,
                "hamming_threshold": config.hamming_threshold,
                "separator": config.separator,
            },
        },
    }
