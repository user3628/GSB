"""Generate a structure-v1 legacy evidence manifest for migration demos."""

from pathlib import Path

from .serde import write_json_atomic


def build_legacy_v1():
    return {
        "manifest_version": 1,
        "legacy_decisions": [
            {
                "contract_id": "CTR-LEGACY",
                "free_locator_text": "依据 Section 3.2 之约定",
                "citation_ids": ["LEG-1", "LEG-2"],
                "legacy_adopted_id": "LEG-1",
            }
        ],
        "legacy_evidence": [
            {
                "citation_id": "LEG-1",
                "free_locator_text": "依据 Section 3.2 之约定",
                "penalty_text": "1,234.5600",
                "raw_text": "依据 Section 3.2 之约定，违约金为 1,234.5600%。",
                "citation_text": "Section 3.2",
                "citation_span": [3, 14],
                "legacy_status": "ADOPTED",
                "legacy_reason": "TOP_LOG_SCORE",
            },
            {
                "citation_id": "LEG-2",
                "free_locator_text": "依据 Section 3.2 之另一版本",
                "penalty_text": "12.5000",
                "raw_text": "依据 Section 3.2 之另一版本，违约金为 12.5000%。",
                "citation_text": "Section 3.2",
                "citation_span": [3, 14],
                "legacy_status": "DOWNGRADED",
                "legacy_reason": "LOWER_LOG_SCORE",
            },
        ],
    }


def write_legacy_v1(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json_atomic(path, build_legacy_v1(), indent=2)
    return path
