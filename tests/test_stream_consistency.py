from __future__ import annotations

import numpy as np
import pytest

from vadkit import StreamingVAD, VADConfig, detect_samples, iter_frames


def _stream_with_chunks(samples: np.ndarray, config: VADConfig, sizes: list[int]):
    session = StreamingVAD(config, chunk_id="stream", expected_sample_count=len(samples))
    offset = 0
    index = 0
    while offset < len(samples):
        size = sizes[index % len(sizes)]
        session.feed_samples(samples[offset : offset + size])
        offset += size
        index += 1
    return session.finalize()


def test_stream_and_batch_match_for_many_chunk_sizes(dataset) -> None:
    config = VADConfig()
    for chunk in dataset.chunks:
        batch = detect_samples(chunk.samples, config, chunk_id=chunk.chunk_id)
        for sizes in ([1], [3, 17, 160], [333], [4_096, 1_024, 7], [len(chunk.samples)]):
            stream = _stream_with_chunks(chunk.samples, config, sizes)
            assert [endpoint.as_dict() for endpoint in stream.endpoints] == [
                endpoint.as_dict() for endpoint in batch.endpoints
            ]
            assert stream.frame_count == batch.frame_count
            assert stream.truncated_frame_count == batch.truncated_frame_count


def test_preframed_stream_matches_batch(dataset) -> None:
    config = VADConfig()
    chunk = dataset.chunks[2]
    batch = detect_samples(chunk.samples, config, chunk_id=chunk.chunk_id)
    session = StreamingVAD(config, chunk_id=chunk.chunk_id)
    for frame in iter_frames(chunk.samples, config):
        session.feed_frame(frame.samples, is_short=frame.is_short)
    stream = session.finalize()
    assert [endpoint.as_dict() for endpoint in stream.endpoints] == [
        endpoint.as_dict() for endpoint in batch.endpoints
    ]


def test_candidate_is_tentative_until_min_silence_confirmed(dataset) -> None:
    config = VADConfig()
    chunk = dataset.chunks[0]
    session = StreamingVAD(config, chunk_id=chunk.chunk_id)
    session.feed_samples(chunk.samples[:8_000])
    assert session.published_endpoints == ()
    tentative = session.tentative_endpoints
    assert len(tentative) == 1
    assert tentative[0]["status"] == "tentative"
    assert tentative[0]["start_ms"] == 240.0

    session.feed_samples(chunk.samples[8_000:12_000])
    assert session.published_endpoints == ()
    assert session.tentative_endpoints[0]["candidate_state"] == "pending_silence"

    session.feed_samples(chunk.samples[12_000:13_000])
    assert len(session.published_endpoints) == 1
    assert session.published_endpoints[0].start_ms == 240.0
    assert session.published_endpoints[0].end_ms == 630.0


def test_frame_state_and_reason_are_exposed(dataset) -> None:
    chunk = dataset.chunks[1]
    result = detect_samples(chunk.samples, VADConfig(), chunk_id=chunk.chunk_id)
    assert len(result.frames) > 0
    assert {frame.raw_state for frame in result.frames} <= {"speech", "silence"}
    assert any("enter_high_threshold" in frame.reason for frame in result.frames)
    assert any("exit_low_threshold" in frame.reason for frame in result.frames)
    assert any("confirm_min_silence" in frame.reason for frame in result.frames)

