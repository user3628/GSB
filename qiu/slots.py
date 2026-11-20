"""Dictionary-backed longest matching and deterministic overlap resolution."""

from __future__ import annotations

import unicodedata
from collections import defaultdict
from typing import Any

from .errors import QueryError, RequestError

SLOT_TYPES: tuple[str, ...] = ("category", "brand", "region")


class SlotExtractor:
    def __init__(self, dictionary: dict[str, Any] | None, config: dict[str, Any]) -> None:
        self.dictionary = dictionary
        self.config = config
        self.index: dict[str, dict[str, str]] = {slot_type: {} for slot_type in SLOT_TYPES}
        self.actual_version: str | None = None
        self.load_error: str | None = None
        if dictionary is None:
            return
        if not isinstance(dictionary, dict):
            self.load_error = "dictionary root is not an object"
            return
        version = dictionary.get("version")
        if isinstance(version, str):
            self.actual_version = version
        entries = dictionary.get("entries")
        if not isinstance(entries, list):
            self.load_error = "dictionary.entries must be a list"
            return
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                self.load_error = f"dictionary entry {index} is not an object"
                continue
            slot_type = entry.get("type")
            term = entry.get("term")
            canonical = entry.get("canonical", term)
            if slot_type not in SLOT_TYPES or not isinstance(term, str) or not isinstance(canonical, str):
                self.load_error = f"dictionary entry {index} is malformed"
                continue
            normalized_term = normalize_dictionary_term(term)
            if normalized_term:
                self.index[slot_type][normalized_term] = canonical

    def summarize(self) -> dict[str, Any]:
        return {
            "actual_version": self.actual_version,
            "entry_counts": {slot_type: len(self.index[slot_type]) for slot_type in SLOT_TYPES},
            "load_error": self.load_error,
            "priority": {slot_type: int(self.config["type_priority"].get(slot_type, 0)) for slot_type in SLOT_TYPES},
        }

    def extract(
        self,
        normalized_query: str,
        *,
        query_id: str | None,
        faults: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        faults = faults or {}
        if faults.get("missing_dictionary") or self.dictionary is None or self.load_error:
            raise QueryError(
                "E_DICT_MISSING",
                "slots",
                "slot dictionary is missing or unreadable",
                query_id=query_id,
                details={"reason": self.load_error or "dictionary_not_found"},
            )

        actual_version = faults.get("dict_version", self.actual_version)
        expected_version = self.config.get("expected_dict_version")
        if not actual_version:
            raise QueryError(
                "E_DICT_MISSING",
                "slots",
                "slot dictionary has no version",
                query_id=query_id,
                details={"expected_dict_version": expected_version},
            )
        if actual_version != expected_version:
            raise QueryError(
                "E_DICT_VERSION",
                "slots",
                "slot dictionary version does not match the configured version",
                query_id=query_id,
                details={"actual_dict_version": actual_version, "expected_dict_version": expected_version},
            )

        candidates: list[dict[str, Any]] = []
        for start in range(len(normalized_query)):
            for slot_type in SLOT_TYPES:
                longest_term = None
                longest_canonical = None
                for term, canonical in self.index[slot_type].items():
                    if normalized_query.startswith(term, start):
                        if longest_term is None or len(term) > len(longest_term):
                            longest_term = term
                            longest_canonical = canonical
                if longest_term is not None:
                    candidates.append(
                        self._candidate(
                            slot_type=slot_type,
                            term=longest_term,
                            canonical=longest_canonical or longest_term,
                            start=start,
                            end=start + len(longest_term),
                            source_version=actual_version,
                            injected=False,
                        )
                    )

        injected = faults.get("overlap_slot")
        if injected is not None:
            candidates.append(self._injected_candidate(injected, normalized_query, actual_version, query_id))

        deduplicated: dict[tuple[Any, ...], dict[str, Any]] = {}
        for candidate in candidates:
            key = (
                candidate["type"],
                candidate["start"],
                candidate["end"],
                candidate["text"],
                candidate["injected"],
            )
            deduplicated.setdefault(key, candidate)
        candidates = list(deduplicated.values())
        candidates.sort(key=self._sort_key)

        accepted: list[dict[str, Any]] = []
        suppressed: list[dict[str, Any]] = []
        for candidate in candidates:
            overlap = next((slot for slot in accepted if _overlaps(candidate, slot)), None)
            if overlap is None:
                accepted.append(candidate)
            else:
                diagnostic = dict(candidate)
                diagnostic["reason"] = "overlap"
                diagnostic["suppressed_by"] = {
                    "end": overlap["end"],
                    "start": overlap["start"],
                    "text": overlap["text"],
                    "type": overlap["type"],
                }
                suppressed.append(diagnostic)

        accepted.sort(key=lambda item: (item["start"], item["end"], item["type"], item["text"]))
        suppressed.sort(key=lambda item: (item["start"], item["end"], item["type"], item["text"]))
        public_slots = [
            {
                "end": item["end"],
                "source_dictionary_version": item["source_dictionary_version"],
                "start": item["start"],
                "text": item["text"],
                "type": item["type"],
            }
            for item in accepted
        ]
        return {
            "diagnostics": {
                "candidate_count": len(candidates),
                "resolved_count": len(accepted),
                "suppressed": suppressed,
                "suppressed_count": len(suppressed),
            },
            "slots": public_slots,
        }

    def _candidate(
        self,
        *,
        slot_type: str,
        term: str,
        canonical: str,
        start: int,
        end: int,
        source_version: str,
        injected: bool,
    ) -> dict[str, Any]:
        return {
            "canonical": canonical,
            "end": end,
            "injected": injected,
            "priority": int(self.config["type_priority"].get(slot_type, 0)),
            "source_dictionary_version": source_version,
            "start": start,
            "text": term,
            "type": slot_type,
        }

    def _injected_candidate(
        self,
        injected: Any,
        normalized_query: str,
        actual_version: str,
        query_id: str | None,
    ) -> dict[str, Any]:
        if not isinstance(injected, dict):
            raise RequestError("faults.overlap_slot must be an object", query_id=query_id)
        slot_type = injected.get("type")
        text = injected.get("text")
        if slot_type not in SLOT_TYPES or not isinstance(text, str) or not text:
            raise RequestError("faults.overlap_slot requires a valid type and text", query_id=query_id)
        normalized_text = normalize_dictionary_term(text)
        start = injected.get("start")
        end = injected.get("end")
        if start is None:
            start = normalized_query.find(normalized_text)
            if start < 0:
                raise RequestError("injected slot text is not present in the normalized query", query_id=query_id)
            end = start + len(normalized_text)
        if not isinstance(start, int) or not isinstance(end, int) or start < 0 or end <= start or end > len(normalized_query):
            raise RequestError("faults.overlap_slot has invalid offsets", query_id=query_id)
        if normalized_query[start:end] != normalized_text:
            raise RequestError("faults.overlap_slot text does not match its offsets", query_id=query_id)
        return self._candidate(
            slot_type=slot_type,
            term=normalized_text,
            canonical=injected.get("canonical", text),
            start=start,
            end=end,
            source_version=actual_version,
            injected=True,
        )

    @staticmethod
    def _sort_key(candidate: dict[str, Any]) -> tuple[Any, ...]:
        return (
            -candidate["priority"],
            -(candidate["end"] - candidate["start"]),
            candidate["start"],
            candidate["end"],
            candidate["type"],
            candidate["text"],
        )


def normalize_dictionary_term(value: str) -> str:
    return unicodedata.normalize("NFKC", value).strip().lower()


def _overlaps(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left["start"] < right["end"] and right["start"] < left["end"]
