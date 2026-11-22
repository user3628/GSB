"""Streaming greedy decoder with fixed-hash signature emission scores."""

import math

import numpy as np

from .errors import StreamMismatchError
from .vocabulary import VOCABULARY, signature_bin, transition_weight


def linear_bands(feature, frame_config):
    return feature[: frame_config.bands].astype(np.float64)


def log_bands(feature, frame_config):
    return feature[frame_config.bands : frame_config.bands * 2].astype(np.float64)


def marker_bands(feature, frame_config):
    start = frame_config.bands * 2
    return feature[start : start + frame_config.marker_bins].astype(np.float64)


def marker_relative(feature, frame_config, decoder_config):
    """Per-bin marker energy above the deterministic silence baseline."""
    marker = marker_bands(feature, frame_config)
    baseline = np.asarray(decoder_config.silence_marker_baseline)
    return marker - baseline


def frame_is_voiced(feature, frame_config, decoder_config):
    return float(np.mean(log_bands(feature, frame_config))) >= decoder_config.voiced_energy_db


def emission_scores(feature, frame_config, decoder_config):
    """Fixed-hash signature-bin emission score for one frame."""
    if not frame_is_voiced(feature, frame_config, decoder_config):
        return {word: -100.0 for word in VOCABULARY}
    relative = marker_relative(feature, frame_config, decoder_config)
    scale = float(np.mean(np.asarray(decoder_config.silence_marker_baseline))) or 1.0
    scores = {}
    for word in VOCABULARY:
        bin_index = signature_bin(word, frame_config.marker_bins) - 2
        strength = float(relative[bin_index]) / scale
        scores[word] = round(decoder_config.energy_scale * strength, 6)
    return scores


def softmax_confidence(mean_score, decoder_config):
    z = max(-50.0, min(50.0, mean_score))
    top = 1.0 / (1.0 + math.exp(-z))
    other = (1.0 - top) / max(1, len(VOCABULARY) - 1)
    return round(top / (top + other), 6)


class StreamingDecoder:
    """Frame-block feed interface; words publish only after silence confirmation."""

    def __init__(self, frame_config, decoder_config):
        self.frame_config = frame_config
        self.config = decoder_config
        self.candidate = None
        self.silence_run = 0
        self.committed = []
        self.trajectory = []

    def _open_candidate(self, index, relative, previous_word):
        self.candidate = {
            "start_frame": index,
            "end_frame": index,
            "marker_sum": np.asarray(relative, dtype=np.float64),
            "frame_count": 1,
            "previous_word": previous_word,
        }
        self.silence_run = 0

    def _candidate_word(self):
        average = self.candidate["marker_sum"] / self.candidate["frame_count"]
        scale = float(np.mean(np.asarray(self.config.silence_marker_baseline))) or 1.0
        best_word = None
        best_strength = None
        for word in VOCABULARY:
            bin_index = signature_bin(word, self.frame_config.marker_bins) - 2
            strength = float(average[bin_index]) / scale
            if best_strength is None or strength > best_strength:
                best_strength = strength
                best_word = word
        return best_word, best_strength

    def _commit_candidate(self):
        candidate = self.candidate
        if candidate is None or candidate["frame_count"] < self.config.min_word_frames:
            self.candidate = None
            return
        word, strength = self._candidate_word()
        mean_score = round(self.config.energy_scale * strength, 6)
        transition = transition_weight(candidate["previous_word"], word)
        start_ms = candidate["start_frame"] * self.frame_config.hop_ms
        end_ms = (candidate["end_frame"] + 1) * self.frame_config.hop_ms
        self.committed.append(
            {
                "word": word,
                "start_ms": int(round(start_ms)),
                "end_ms": int(round(end_ms)),
                "start_frame": candidate["start_frame"],
                "end_frame": candidate["end_frame"],
                "confidence": softmax_confidence(mean_score, self.config),
                "mean_score": mean_score,
                "transition_weight": round(transition, 6),
            }
        )
        self.candidate = None

    def push_frame(self, index, feature):
        voiced = frame_is_voiced(feature, self.frame_config, self.config)
        if voiced:
            relative = marker_relative(feature, self.frame_config, self.config)
            scores = emission_scores(feature, self.frame_config, self.config)
            top_word = max(scores, key=lambda key: scores[key])
            top_score = scores[top_word]
        else:
            relative = None
            top_word = None
            top_score = None
        self.trajectory.append(
            {
                "frame": index,
                "state": "voiced" if voiced else "silence",
                "top_word": top_word,
                "top_score": top_score,
            }
        )
        if not voiced:
            if self.candidate is not None:
                self.silence_run += 1
                if self.silence_run >= self.config.min_silence_frames:
                    self._commit_candidate()
                    self.silence_run = 0
            else:
                self.silence_run = 0
            return
        if self.candidate is None:
            previous_word = self.committed[-1]["word"] if self.committed else None
            self._open_candidate(index, relative, previous_word)
            return
        self.candidate["end_frame"] = index
        self.candidate["marker_sum"] += np.asarray(relative, dtype=np.float64)
        self.candidate["frame_count"] += 1
        self.silence_run = 0

    def push_block(self, features):
        base = len(self.trajectory)
        for offset, feature in enumerate(features):
            self.push_frame(base + offset, feature)

    def finalize(self):
        if self.candidate is not None:
            self._commit_candidate()
        words = [dict(item) for item in self.committed]
        text = " ".join(item["word"] for item in words)
        return {
            "text": text,
            "words": words,
            "trajectory": [dict(item) for item in self.trajectory],
        }


def decode_stream(features, frame_config, decoder_config, block_size=1):
    decoder = StreamingDecoder(frame_config, decoder_config)
    for start in range(0, len(features), block_size):
        decoder.push_block(features[start : start + block_size])
    return decoder.finalize()


def decode_whole(features, frame_config, decoder_config):
    return decode_stream(features, frame_config, decoder_config, block_size=len(features) or 1)


def _compare_values(stream_value, whole_value):
    if isinstance(stream_value, list):
        if len(stream_value) != len(whole_value):
            return False
        return all(_compare_values(left, right) for left, right in zip(stream_value, whole_value))
    if isinstance(stream_value, dict):
        if set(stream_value) != set(whole_value):
            return False
        return all(_compare_values(stream_value[key], whole_value[key]) for key in stream_value)
    return stream_value == whole_value


def _first_diverging_word(stream_words, whole_words):
    for index in range(max(len(stream_words), len(whole_words))):
        if index >= len(stream_words) or index >= len(whole_words):
            return index
        if stream_words[index] != whole_words[index]:
            return index
    return 0


def verify_consistency(features, frame_config, decoder_config, block_sizes=(1, 3, 7)):
    """Run streaming at several block sizes plus a whole-shard pass."""
    reference = decode_whole(features, frame_config, decoder_config)
    for block_size in block_sizes:
        streamed = decode_stream(
            features, frame_config, decoder_config, block_size=max(1, block_size)
        )
        if not _compare_values(streamed, reference):
            index = _first_diverging_word(streamed["words"], reference["words"])
            raise StreamMismatchError(
                "streaming and whole-shard decoding diverged",
                {
                    "block_size": block_size,
                    "first_diverging_word_index": index,
                    "stream_value": streamed["words"][index]
                    if index < len(streamed["words"])
                    else None,
                    "whole_value": reference["words"][index]
                    if index < len(reference["words"])
                    else None,
                },
            )
    return reference
