package speakerdiar

import (
	"bytes"
	"testing"
)

func newTestManifest(t *testing.T) (string, *SessionManifest) {
	t.Helper()
	dir := t.TempDir()
	m, err := GenerateSession(dir, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func TestFramingSpectrumDeterminismAndNumericCode(t *testing.T) {
	dir, m := newTestManifest(t)
	chunk := m.Chunks[0]
	_, pcm, err := ReadAndValidateChunk(dir, m, chunk)
	if err != nil {
		t.Fatal(err)
	}
	cfg := m.Config
	if cfg.FrameSamples() != 400 || cfg.HopSamples() != 160 {
		t.Fatalf("integer conversion = %d/%d", cfg.FrameSamples(), cfg.HopSamples())
	}
	spec1, err := AnalyzeSpectrum(chunk.ChunkID, PCMToFloat(pcm), cfg)
	if err != nil {
		t.Fatal(err)
	}
	spec2, err := AnalyzeSpectrum(chunk.ChunkID, PCMToFloat(pcm), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if spec1.Digest != spec2.Digest {
		t.Fatal("spectral digest is not deterministic")
	}
	if len(spec1.PaddedFrames) == 0 {
		t.Fatal("expected deterministic zero-padded tail frames")
	}
	raw1, _ := CanonicalJSON(spec1)
	raw2, _ := CanonicalJSON(spec2)
	if !bytes.Equal(raw1, raw2) {
		t.Fatal("spectral bytes are not deterministic")
	}

	_, err = RunFrameStage(chunk, pcm, cfg, InjectNumericSpectrum)
	if CodeOf(err) != CodeNumeric {
		t.Fatalf("expected E_NUMERIC, got %v", err)
	}
	de, _ := AsError(err)
	if de.ChunkID != chunk.ChunkID || de.FrameIndex < 0 {
		t.Fatalf("numeric error missing coordinates: %+v", de)
	}
}

func TestSeparationDeterminismAndDiagnosticCodes(t *testing.T) {
	dir, m := newTestManifest(t)
	chunk := m.Chunks[0]
	_, pcm, err := ReadAndValidateChunk(dir, m, chunk)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := AnalyzeSpectrum(chunk.ChunkID, PCMToFloat(pcm), m.Config)
	if err != nil {
		t.Fatal(err)
	}
	sep1, err := SeparateSources(spec, chunk.SpeakerCount, m.Config, InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	sep2, err := SeparateSources(spec, chunk.SpeakerCount, m.Config, InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	if sep1.Digest != sep2.Digest {
		t.Fatal("separation digest is not deterministic")
	}
	rounds1, _ := CanonicalJSON(sep1.Rounds)
	rounds2, _ := CanonicalJSON(sep2.Rounds)
	if !bytes.Equal(rounds1, rounds2) {
		t.Fatal("mask round changes are not byte deterministic")
	}

	_, err = SeparateSources(spec, m.Config.MaxSpeakers+1, m.Config, InjectNone)
	if CodeOf(err) != CodeSeparation {
		t.Fatalf("expected E_SEPARATION for too many speakers, got %v", err)
	}
	_, err = SeparateSources(spec, chunk.SpeakerCount, m.Config, InjectZeroMaskedSource)
	if CodeOf(err) != CodeSeparation {
		t.Fatalf("expected E_SEPARATION for zero source, got %v", err)
	}
	de, _ := AsError(err)
	if de.SourceIndex != 0 || de.Round != 1 || de.ChunkID == "" {
		t.Fatalf("separation diagnostic context incomplete: %+v", de)
	}
}

func TestEmbeddingProjectionTieAndStableSpeakerSet(t *testing.T) {
	if _, err := HashProject([]float64{1, 2}, 3, 8, 1, "c"); CodeOf(err) != CodeEmbedding {
		t.Fatalf("expected projection mismatch E_EMBEDDING, got %v", err)
	}
	_, _, _, err := ArbitrateFrame(ArbitrationRequest{
		ChunkID: "c", FrameIndex: 9, SourceIndex: 1, Threshold: 0.1,
		Scores: []SpeakerScore{{SpeakerID: "S1", Score: 0.9}, {SpeakerID: "S2", Score: 0.9}},
	})
	if CodeOf(err) != CodeEmbedding {
		t.Fatalf("expected tie E_EMBEDDING, got %v", err)
	}
	chosen, reason, _, err := ArbitrateFrame(ArbitrationRequest{
		ChunkID: "c", FrameIndex: 9, SourceIndex: 1, Threshold: 0.1, SourceSpeaker: "S2",
		Scores: []SpeakerScore{{SpeakerID: "S1", Score: 0.9}, {SpeakerID: "S2", Score: 0.9}},
	})
	if err != nil || chosen != "S2" || reason == "" {
		t.Fatalf("source evidence should break tie: chosen=%q reason=%q err=%v", chosen, reason, err)
	}
}

func TestTimelineSplitMergeAndConflict(t *testing.T) {
	tl := NewTimeline("s")
	seg, err := tl.AddSegment(TimelineSegment{ID: "a", ChunkID: "c0", StartMS: 0, EndMS: 1000, SpeakerID: "S1", Source: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tl.AddSegment(TimelineSegment{ChunkID: "c0", StartMS: 10, EndMS: 5, SpeakerID: "S1"}); CodeOf(err) != CodeTimeline {
		t.Fatalf("expected E_TIMELINE for end before start, got %v", err)
	}
	if err := tl.SplitSegment(seg.ID, 500, "test"); err != nil {
		t.Fatal(err)
	}
	if len(tl.Segments) != 2 {
		t.Fatalf("split should produce two segments, got %d", len(tl.Segments))
	}
	merged, err := tl.MergeAdjacent(seg.ID, 0, "test")
	if err != nil || !merged || len(tl.Segments) != 1 {
		t.Fatalf("merge failed merged=%v len=%d err=%v", merged, len(tl.Segments), err)
	}
	if tl.Version == 0 || len(tl.Revisions) == 0 {
		t.Fatal("timeline edits must record monotonic revisions")
	}

	refA := SpeakerRef{ChunkID: "c0", LocalNumber: 1}
	refB := SpeakerRef{ChunkID: "c1", LocalNumber: 1}
	if err := tl.MergeCrossChunkSpeakers(refA, refB, "S1", "test"); err != nil {
		t.Fatal(err)
	}
	err = tl.MergeCrossChunkSpeakers(refA, SpeakerRef{ChunkID: "c2", LocalNumber: 1}, "S2", "test")
	if CodeOf(err) != CodeTimeline {
		t.Fatalf("expected cross-chunk number conflict E_TIMELINE, got %v", err)
	}
}

func TestLocalDeterministicAcceptance(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(dir, "acceptance", InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	report, err := svc.Run()
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyIncrementalEqualsFull(dir, svc.Manifest, report); err != nil {
		t.Fatal(err)
	}
	if len(report.Final.Segments) < 12 {
		t.Fatalf("expected substantive segments, got %d", len(report.Final.Segments))
	}
	if len(report.Final.Overlaps) < 6 {
		t.Fatalf("expected overlap segments, got %d", len(report.Final.Overlaps))
	}

	for i, chunk := range svc.Manifest.Chunks {
		expectedSpeakers := map[string]bool{}
		for _, ref := range chunk.ReferenceSegments {
			expectedSpeakers[ref.SpeakerID] = true
		}
		gotSpeakers := map[string]bool{}
		for _, a := range report.Products[i].Embedding.Speakers {
			if a.LocalNumber < 1 || a.LocalNumber > chunk.SpeakerCount {
				t.Fatalf("local number out of contiguous range: %+v", a)
			}
			gotSpeakers[a.SpeakerID] = true
		}
		for id := range expectedSpeakers {
			if !gotSpeakers[id] {
				t.Fatalf("chunk %s missing speaker %s in %v", chunk.ChunkID, id, gotSpeakers)
			}
		}
		for _, truth := range referenceOverlapIntervals(chunk) {
			if !hasOverlapIntersection(report.Final.Overlaps, chunk.ChunkID, truth[0], truth[1]) {
				t.Fatalf("chunk %s overlap %v not detected: %+v", chunk.ChunkID, truth, report.Final.Overlaps)
			}
		}
	}

	full1, files1, err := ComputeFullArtifacts(dir, svc.Manifest, InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	_, files2, err := ComputeFullArtifacts(dir, svc.Manifest, InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(files1.Segments, files2.Segments) || !bytes.Equal(files1.Overlaps, files2.Overlaps) || !bytes.Equal(files1.Voiceprints, files2.Voiceprints) {
		t.Fatal("full artifacts are not byte deterministic")
	}
	if len(full1) != 3 {
		t.Fatalf("expected 3 chunk products, got %d", len(full1))
	}
}

func referenceOverlapIntervals(chunk ChunkManifest) [][2]int {
	var out [][2]int
	for i := 0; i < len(chunk.ReferenceSegments); i++ {
		for j := i + 1; j < len(chunk.ReferenceSegments); j++ {
			a, b := chunk.ReferenceSegments[i], chunk.ReferenceSegments[j]
			if a.SpeakerID == b.SpeakerID {
				continue
			}
			start := maxInt(a.StartMS, b.StartMS)
			end := minInt(a.EndMS, b.EndMS)
			if end > start {
				out = append(out, [2]int{start, end})
			}
		}
	}
	return out
}

func hasOverlapIntersection(overlaps []OverlapSegment, chunkID string, start, end int) bool {
	for _, ov := range overlaps {
		if ov.ChunkID == chunkID && intervalsOverlap(start, end, ov.StartMS, ov.EndMS) {
			return true
		}
	}
	return false
}

func TestClusterDigestsIndependentOfChunkOrder(t *testing.T) {
	dir, m := newTestManifest(t)
	normal, _, err := ComputeFullArtifacts(dir, m, InjectNone)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(m.Chunks) - 1; i >= 0; i-- {
		chunk := m.Chunks[i]
		raw, pcm, err := ReadAndValidateChunk(dir, m, chunk)
		if err != nil {
			t.Fatal(err)
		}
		p, _, _, err := runChunkWithAudio(m, chunk, raw, pcm, m.Config, InjectNone)
		if err != nil {
			t.Fatal(err)
		}
		if p.Embedding.Digest != normal[i].Embedding.Digest {
			t.Fatalf("chunk %s embedding changed under reverse processing order", chunk.ChunkID)
		}
		if p.Summary.Digest != normal[i].Summary.Digest {
			t.Fatalf("chunk %s summary changed under reverse processing order", chunk.ChunkID)
		}
	}
}
