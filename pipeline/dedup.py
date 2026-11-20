"""SimHash banded near-duplicate detection.

Index kept records by 4 bands x 16 bits; a record is a duplicate when a
*kept* candidate falls within the Hamming threshold. First occurrence
always wins, which keeps the output order deterministic.
"""

from collections import defaultdict

from .textnorm import hamming64


def band_keys(simhash, bands=4, band_bits=16):
    mask = (1 << band_bits) - 1
    keys = []
    for band in range(bands):
        value = (simhash >> (band * band_bits)) & mask
        keys.append((band, value))
    return keys


def dedup_records(simhashes, threshold=3, bands=4, band_bits=16):
    """Greedy first-occurrence-wins dedup over an ordered simhash list.

    Returns (kept_indices, duplicates) where duplicates is a list of
    (index, duplicate_of_index, hamming_distance) tuples.
    """
    kept = []
    duplicates = []
    index = defaultdict(list)
    for pos, simhash in enumerate(simhashes):
        keys = band_keys(simhash, bands, band_bits)
        candidates = set()
        for key in keys:
            candidates.update(index.get(key, ()))
        hit = None
        for cand in sorted(candidates):
            distance = hamming64(simhash, simhashes[cand])
            if distance <= threshold:
                hit = (cand, distance)
                break
        if hit is not None:
            duplicates.append((pos, hit[0], hit[1]))
        else:
            kept.append(pos)
            for key in keys:
                index[key].append(pos)
    return kept, duplicates
