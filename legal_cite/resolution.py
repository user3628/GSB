"""Stage 3: duplicate folding and log-domain conflict resolution."""

import math
from datetime import date

from .elements import comparable_tuple
from .errors import E_ELEMENT_INVALID, LegalCiteError


def _parse_iso_day(value):
    if not value:
        return None
    parts = [int(p) for p in value.split("-")]
    return date(parts[0], parts[1], parts[2])


def log_odds(p):
    p = min(max(p, 1e-9), 1 - 1e-9)
    return math.log(p / (1 - p))


def freshness_log_score(issued_date, cfg):
    """Newer citations receive a smaller penalty (linear per 30 days)."""
    ref = _parse_iso_day(cfg["freshness"]["reference_date"])
    issued = _parse_iso_day(issued_date)
    if issued is None:
        return 0.0
    age_days = max((ref - issued).days, 0)
    periods = age_days / 30.0
    return cfg["freshness"]["decay_per_30d"] * periods


def score_citation(rec, cfg):
    """Compose trust score in the log domain.

    score = logit(channel prior) + log(language weight) + freshness term
    """
    channel = rec["channel"]
    if channel not in cfg["channel_priors"]:
        raise LegalCiteError(
            E_ELEMENT_INVALID, "unknown source channel for scoring",
            {"citation_id": rec["citation_id"], "element": "channel",
             "value": channel},
        )
    lang = rec["lang"]
    if lang not in cfg["language_weights"]:
        raise LegalCiteError(
            E_ELEMENT_INVALID, "unknown language for scoring",
            {"citation_id": rec["citation_id"], "element": "lang",
             "value": lang},
        )
    prior = log_odds(cfg["channel_priors"][channel])
    lang_term = math.log(cfg["language_weights"][lang])
    fresh = freshness_log_score(rec.get("issued_date"), cfg)
    total = prior + lang_term + fresh
    return {
        "channel_log_prior": round(prior, 10),
        "language_log_weight": round(lang_term, 10),
        "freshness_log_score": round(fresh, 10),
        "total_log_score": round(total, 10),
    }


def fold_duplicates(records, cfg):
    """Merge identical cross-shard duplicates.

    Duplicates share the same citation id and every normalized value.  The
    highest-scored copy survives; the others are recorded with provenance.
    """
    by_id = {}
    for rec in records:
        by_id.setdefault(rec["citation_id"], []).append(rec)
    survivors, folded = [], []
    for citation_id in sorted(by_id):
        copies = by_id[citation_id]
        scored = []
        for copy in copies:
            scored.append((copy, score_citation(copy, cfg)))
        scored.sort(key=lambda item: (
            -item[1]["total_log_score"],
            item[0].get("shard_id", ""),
        ))
        winner, winner_score = scored[0]
        survivors.append(winner)
        for loser, loser_score in scored[1:]:
            folded.append({
                "citation_id": citation_id,
                "kept_shard_id": winner.get("shard_id"),
                "dropped_shard_id": loser.get("shard_id"),
                "reason": "DUPLICATE_LOWER_SCORE",
                "kept_score": winner_score["total_log_score"],
                "dropped_score": loser_score["total_log_score"],
            })
    return survivors, folded


def resolve_group(group, cfg):
    """Resolve one (contract, locator) group to a single adopted citation."""
    members = group["members"]
    scored = [(rec, score_citation(rec, cfg)) for rec in members]
    # Deterministic arbitration: highest score, then citation_id ascending.
    scored.sort(key=lambda item: (-item[1]["total_log_score"],
                                  item[0]["citation_id"]))
    winner, winner_score = scored[0]
    min_score = cfg["review"]["min_trust_log_score"]
    needs_review = winner_score["total_log_score"] < min_score
    resolutions = []
    for rank, (rec, score) in enumerate(scored):
        if rank == 0:
            status = "ADOPTED" if not needs_review else "PENDING_REVIEW"
            reason = "TOP_LOG_SCORE" if not needs_review \
                else "ALL_CITATIONS_BELOW_TRUST_THRESHOLD"
        else:
            status = "DOWNGRADED"
            if score["total_log_score"] == winner_score["total_log_score"]:
                reason = "TIE_BROKEN_BY_CITATION_ID"
            else:
                reason = "LOWER_LOG_SCORE"
        resolutions.append({
            "citation_id": rec["citation_id"],
            "status": status,
            "reason": reason,
            "scores": score,
            "value_signature": list(comparable_tuple(
                rec["elements_normalized"])),
        })
    return {
        "contract_id": group["contract_id"],
        "locator": group["locator"],
        "article": group["article"],
        "section": group["section"],
        "has_conflict": group["has_conflict"],
        "distinct_value_signatures": group["distinct_value_signatures"],
        "needs_manual_review": needs_review,
        "review_basis": None if not needs_review else {
            "threshold": min_score,
            "best_log_score": winner_score["total_log_score"],
            "best_citation_id": winner["citation_id"],
        },
        "adopted_citation_id": None if needs_review else winner["citation_id"],
        "resolutions": resolutions,
    }


def resolve_all(groups, cfg):
    return [resolve_group(group, cfg) for group in groups]
