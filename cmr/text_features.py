"""Deterministic text features: NFKC + lowercase + whitespace folding, then a
hashed bag-of-words and hashed character n-grams, weighted and concatenated."""

import hashlib
import math
import re
import unicodedata

import numpy as np

_TOKEN_RE = re.compile(r"[a-z0-9]+|[一-鿿]")


def normalize_text(text):
    text = unicodedata.normalize("NFKC", text or "")
    text = text.lower()
    return " ".join(text.split())


def word_tokens(norm_text):
    return _TOKEN_RE.findall(norm_text)


def char_ngrams(norm_text, n_min, n_max):
    grams = []
    for n in range(n_min, n_max + 1):
        for i in range(0, max(0, len(norm_text) - n + 1)):
            grams.append(norm_text[i : i + n])
    return grams


def _bin_of(term, bins):
    digest = hashlib.sha256(term.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % bins


def term_weights(norm_text, cfg):
    """Return (bow_terms, ngram_terms): dicts term -> weight (deterministic)."""
    tokens = word_tokens(norm_text)
    grams = char_ngrams(norm_text, cfg.ngram_min, cfg.ngram_max)
    bow, ngram = {}, {}
    for tok in tokens:
        bow[tok] = bow.get(tok, 0) + 1
    for gram in grams:
        ngram[gram] = ngram.get(gram, 0) + 1
    bow = {t: cfg.bow_weight * (1.0 + math.log(c)) for t, c in sorted(bow.items())}
    ngram = {g: cfg.ngram_weight * (1.0 + math.log(c)) for g, c in sorted(ngram.items())}
    return bow, ngram


def extract_text_features(text, cfg):
    """Fixed-dim text feature vector of dimension cfg.text_dim."""
    norm = normalize_text(text)
    bow, ngram = term_weights(norm, cfg)
    vec = np.zeros(cfg.text_dim, dtype=np.float64)
    for term, weight in bow.items():
        vec[_bin_of("b:" + term, cfg.bow_bins)] += weight
    for gram, weight in ngram.items():
        vec[cfg.bow_bins + _bin_of("g:" + gram, cfg.ngram_bins)] += weight
    return vec
