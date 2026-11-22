"""Stage 2b: dictionary longest-match entity slot extraction.

Slot types: category / brand / region. Candidates are all occurrences of
dictionary terms in the normalized query. Overlapping candidates are
resolved by (longer match, higher configured priority, earlier start);
suppressed candidates are kept in the diagnostics with their suppression
basis. Missing dictionary or out-of-range offsets raise E_SLOT_INVALID.
"""

import json

from .canon import canonical, digest_bytes
from .errors import E_SLOT_INVALID, WorkbenchError
from .tokenizer import normalize_text

SLOT_TYPES = ("category", "brand", "region")
_STAGE = "slots"


def _invalid(reason, details=None):
    return WorkbenchError(E_SLOT_INVALID, reason, stage=_STAGE, details=details)


def load_dictionary(path):
    """Load and validate the slot dictionary. Returns dict with digest."""
    try:
        raw = open(path, "rb").read()
    except OSError:
        raise _invalid("dictionary_missing", {"path": str(path)})
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise _invalid("dictionary_corrupt", {"path": str(path)})
    if not isinstance(data, dict) or not isinstance(data.get("version"), str):
        raise _invalid("dictionary_missing_version", {"path": str(path)})
    entries = data.get("entries")
    if not isinstance(entries, list) or not entries:
        raise _invalid("dictionary_entries_missing", {"path": str(path)})
    norm_entries = []
    for i, e in enumerate(entries):
        term = normalize_text(e.get("term", "") if isinstance(e, dict) else "")
        if not term:
            raise _invalid("dictionary_term_empty", {"index": i})
        slot_type = e.get("slot_type")
        if slot_type not in SLOT_TYPES:
            raise _invalid("unknown_slot_type", {"index": i, "slot_type": slot_type})
        norm_entries.append(
            {
                "term": term,
                "slot_type": slot_type,
                "priority": e.get("priority"),
            }
        )
    norm_entries.sort(key=lambda x: (x["term"], x["slot_type"]))
    return {
        "version": data["version"],
        "entries": norm_entries,
        "digest": digest_bytes(canonical({"version": data["version"], "entries": norm_entries})),
    }


def _overlaps(a, b):
    return a["start"] < b["end"] and b["start"] < a["end"]


def extract_slots(normalized, dictionary, priority_config):
    """Longest-match extraction + overlap resolution.

    Returns (accepted, suppressed). Every suppressed candidate carries the
    winning candidate and the reason so diagnostics never lose information.
    """
    version = dictionary["version"]
    candidates = []
    for entry in dictionary["entries"]:
        term = entry["term"]
        start = 0
        while True:
            idx = normalized.find(term, start)
            if idx < 0:
                break
            candidates.append(
                {
                    "term": term,
                    "slot_type": entry["slot_type"],
                    "start": idx,
                    "end": idx + len(term),
                    "dict_version": version,
                    "priority": entry["priority"],
                }
            )
            start = idx + 1  # allow overlapping occurrences
    n = len(normalized)
    for c in candidates:
        if not (0 <= c["start"] < c["end"] <= n):
            raise _invalid(
                "slot_offset_out_of_bounds",
                {"term": c["term"], "start": c["start"], "end": c["end"], "text_len": n},
            )

    def eff_priority(c):
        if c["priority"] is not None:
            return c["priority"]
        return priority_config.get(c["slot_type"], 99)

    ordered = sorted(
        candidates,
        key=lambda c: (-(c["end"] - c["start"]), eff_priority(c), c["start"], c["term"]),
    )
    accepted, suppressed = [], []
    for cand in ordered:
        winner = next((a for a in accepted if _overlaps(a, cand)), None)
        if winner is None:
            accepted.append(cand)
        else:
            suppressed.append(
                dict(
                    cand,
                    reason="overlap_lower_priority",
                    suppressed_by={
                        "term": winner["term"],
                        "slot_type": winner["slot_type"],
                        "start": winner["start"],
                        "end": winner["end"],
                    },
                )
            )
    accepted.sort(key=lambda c: (c["start"], c["end"], c["slot_type"], c["term"]))
    suppressed.sort(key=lambda c: (c["start"], c["end"], c["slot_type"], c["term"]))
    for c in accepted + suppressed:
        c.pop("priority", None)
    return accepted, suppressed
